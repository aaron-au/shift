// Package api exposes the runner's HTTP surface: the task/benchmark API,
// the embedded dashboard, and the webhook / direct-execution endpoints
// (ADR-0016). Hook endpoints (`POST /hooks/{name}`) authenticate by a
// per-webhook token. The control/dashboard endpoints are still loopback and
// unauthenticated in this stage; user auth (HTTP Basic + per-API
// permissions) is the next M5d-2 stage and MUST land before a non-local
// bind of the control surface ships (ADR-0008 deferral, now scheduled).
package api

import (
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/ratelimit"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/webhook"
)

// ExecReporter reports a finished direct (push) execution to the hub as
// metadata (ADR-0016). nil disables reporting (standalone runner).
type ExecReporter func(t task.Task, trigger string)

// maxWebhookBody bounds an inbound webhook payload (buffered before async
// execution).
const maxWebhookBody = 8 << 20

//go:embed ui.html
var uiHTML []byte

// Handler builds the runner's HTTP mux. hubStatus is optional: when the
// hub lease intake is running it supplies the intake snapshot for
// /api/status (nil = local-only runner). guard authenticates the control
// surface; a nil/open guard leaves it unauthenticated (loopback dev).
func Handler(svc *service.Service, runnerName, version string, started time.Time, hubStatus func() any, guard *auth.Guard, report ExecReporter, hooks *webhook.Registry, metricsHandler http.Handler, webhookLimit *ratelimit.Limiter) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler) // Prometheus scrape (M6a, ADR-0020)
	}

	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		body := map[string]any{
			"name":     runnerName,
			"version":  version,
			"uptime_s": time.Since(started).Seconds(),
			"status":   svc.Status(),
		}
		if hubStatus != nil {
			body["hub"] = hubStatus()
		}
		writeJSON(w, http.StatusOK, body)
	})

	mux.HandleFunc("POST /api/flows/execute", func(w http.ResponseWriter, r *http.Request) {
		doc, err := decodeFlow(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		// Test-mode data capture: ?capture=1[&capture_max=N]. The sample
		// stays runner-side, redacted, and ephemeral (GET .../capture).
		opts := service.SubmitOpts{}
		if r.URL.Query().Get("capture") == "1" {
			opts.Capture = true
			if n, err := strconv.Atoi(r.URL.Query().Get("capture_max")); err == nil && n > 0 {
				opts.CaptureMax = n
			}
		}
		id, err := svc.SubmitWith(doc, opts)
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity, err)
			return
		}
		reportWhenDone(svc, id, "api", report)
		writeJSON(w, http.StatusAccepted, map[string]string{"task_id": id})
	})

	mux.HandleFunc("GET /api/tasks", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tasks": svc.Tasks(limit)})
	})

	mux.HandleFunc("GET /api/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		t, ok := svc.Task(r.PathValue("id"))
		if !ok {
			writeErr(w, http.StatusNotFound, errors.New("unknown task"))
			return
		}
		writeJSON(w, http.StatusOK, t)
	})

	// Per-step INPUT/OUTPUT capture for a task (test mode). Payload data —
	// runner-only, redacted, ephemeral. Empty unless the run had capture on.
	mux.HandleFunc("GET /api/tasks/{id}/capture", func(w http.ResponseWriter, r *http.Request) {
		t, ok := svc.Task(r.PathValue("id"))
		if !ok {
			writeErr(w, http.StatusNotFound, errors.New("unknown task"))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"task_id": t.ID, "captured": t.Captured})
	})

	// --- webhooks / direct execution (ADR-0016) ---
	// The registry is owned by the caller: hub-attached runners have it
	// filled by the sync loop; standalone runners use the local PUT below.

	// Register/replace a webhook: PUT /api/webhooks/{name} with
	// {"document": <flow>, "token": "<optional per-hook credential>"}.
	mux.HandleFunc("PUT /api/webhooks/{name}", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Document json.RawMessage `json:"document"`
			Token    string          `json:"token"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if _, err := flow.Parse(req.Document); err != nil { // validate at registration
			writeErr(w, http.StatusUnprocessableEntity, err)
			return
		}
		name := r.PathValue("name")
		hooks.Put(webhook.Hook{Name: name, Doc: req.Document, TokenHash: hashHookToken(req.Token)})
		writeJSON(w, http.StatusOK, map[string]any{"name": name, "protected": req.Token != ""})
	})

	mux.HandleFunc("GET /api/webhooks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"webhooks": hooks.Names()})
	})

	mux.HandleFunc("DELETE /api/webhooks/{name}", func(w http.ResponseWriter, r *http.Request) {
		if !hooks.Delete(r.PathValue("name")) {
			writeErr(w, http.StatusNotFound, errors.New("unknown webhook"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Trigger: POST /hooks/{name}. The request body becomes the flow's
	// @webhook source. Async — returns 202 + task_id; poll /api/tasks/{id}
	// (and .../capture) for status/results. Payload never leaves the runner.
	mux.HandleFunc("POST /hooks/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		// Resolve the hook first: an attacker-chosen name must not mint a
		// permanent limiter bucket (unbounded map growth = memory DoS). Only
		// known hooks reach the limiter, so the bucket keyspace is bounded by
		// the registered-hook set × source IPs.
		h, ok := hooks.Get(name)
		if !ok {
			writeErr(w, http.StatusNotFound, errors.New("unknown webhook"))
			return
		}
		// Rate limit the public ingress by {hook, source IP} before any work
		// (M6c, ADR-0021) — a per-hook ceiling stops one flow flooding
		// admission. Keyed pre-auth so a token isn't needed to be throttled.
		if !webhookLimit.Allow("webhook", name+"|"+ratelimit.ClientIP(r)) {
			ratelimit.Reject(w)
			return
		}
		if h.TokenHash != "" && !validHookToken(r, h.TokenHash) {
			writeErr(w, http.StatusUnauthorized, errors.New("invalid webhook token"))
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		doc, err := flow.Parse(h.Doc)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		id, err := svc.SubmitWith(doc, service.SubmitOpts{WebhookBody: body})
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity, err)
			return
		}
		reportWhenDone(svc, id, "webhook", report)
		writeJSON(w, http.StatusAccepted, map[string]string{"task_id": id})
	})

	mux.HandleFunc("POST /api/benchmark", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Records int64 `json:"records"`
			Streams int   `json:"streams"`
		}
		if r.ContentLength > 0 {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		}
		// Run asynchronously; the dashboard polls status for completion.
		go func() { _, _ = svc.RunBenchmark(req.Records, req.Streams) }()
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "benchmark started"})
	})

	mux.HandleFunc("GET /api/benchmark", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"history": svc.BenchHistory()})
	})

	mux.HandleFunc("POST /api/benchmark/tiers", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Records int64 `json:"records"`
			Streams int   `json:"streams"`
		}
		if r.ContentLength > 0 {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		}
		// Run asynchronously; the dashboard polls status for completion.
		go func() { _, _ = svc.RunTieredBenchmark(req.Records, req.Streams) }()
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "tiered benchmark started"})
	})

	mux.HandleFunc("GET /api/benchmark/tiers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"history": svc.TieredHistory()})
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})

	return guard.Wrap(mux, permFor)
}

// permFor maps a request to the permission its endpoint needs. Health checks
// and the hook trigger are unguarded (the latter authenticates by its own
// per-hook token); reads need PermRead; webhook config changes (PUT/DELETE)
// need PermManage; other writes (execute, benchmarks) need PermExecute.
func permFor(r *http.Request) (auth.Permission, bool) {
	p := r.URL.Path
	if p == "/healthz" || p == "/metrics" || strings.HasPrefix(p, "/hooks/") {
		return "", false
	}
	switch r.Method {
	case http.MethodGet:
		return auth.PermRead, true
	case http.MethodPut, http.MethodDelete:
		return auth.PermManage, true
	default: // POST and anything else that mutates
		return auth.PermExecute, true
	}
}

// reportWhenDone spawns a best-effort watcher that reports a direct
// execution to the hub once it reaches a terminal state. No-op when there
// is no reporter (standalone runner).
func reportWhenDone(svc *service.Service, id, trigger string, report ExecReporter) {
	if report == nil {
		return
	}
	go func() {
		deadline := time.Now().Add(10 * time.Minute)
		for {
			t, ok := svc.Task(id)
			if !ok {
				return // evicted before we saw it finish
			}
			if t.State == task.StateCompleted || t.State == task.StateFailed {
				report(t, trigger)
				return
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
}

func decodeFlow(r *http.Request) (*flow.Document, error) {
	defer func() { _ = r.Body.Close() }()
	var raw json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20)).Decode(&raw); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	return flow.Parse(raw)
}

// hashHookToken returns the hex SHA-256 of a hook token (the stored form),
// matching the hub's hashing so local and synced hooks verify identically.
func hashHookToken(tok string) string {
	if tok == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// validHookToken checks the per-webhook credential (X-Webhook-Token header
// or Authorization: Bearer) against the stored hash, in constant time.
func validHookToken(r *http.Request, wantHash string) bool {
	got := r.Header.Get("X-Webhook-Token")
	if got == "" {
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimSpace(h[len("Bearer "):])
		}
	}
	return subtle.ConstantTimeCompare([]byte(hashHookToken(got)), []byte(wantHash)) == 1
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
