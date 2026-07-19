// Package api is the hub's HTTP control API (ADR-0009): admin endpoints
// for flows/tasks/runner-tokens behind the admin bearer token, and runner
// endpoints (register/lease/heartbeat/complete/fail) behind per-runner
// bearer secrets. JSON in, JSON out, stdlib mux only. The hub never sees
// payload data — documents and results are control-plane metadata.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Options configure the API.
type Options struct {
	// AdminToken guards admin endpoints. Required, min 16 bytes.
	AdminToken string
	// LeaseTTL is how long a claimed task stays leased between heartbeats
	// (default 30s).
	LeaseTTL time.Duration
	// LeasePoll is the claim re-check interval inside a long-poll lease
	// request (default 200ms).
	LeasePoll time.Duration
	// MaxLeaseWait caps a lease request's long-poll (default 30s).
	MaxLeaseWait time.Duration
}

func (o *Options) defaults() error {
	if len(o.AdminToken) < 16 {
		return fmt.Errorf("api: admin token must be at least 16 characters")
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	if o.LeasePoll <= 0 {
		o.LeasePoll = 200 * time.Millisecond
	}
	if o.MaxLeaseWait <= 0 {
		o.MaxLeaseWait = 30 * time.Second
	}
	return nil
}

type api struct {
	st   *store.Store
	opts Options
}

// Handler builds the hub's HTTP handler.
func Handler(st *store.Store, opts Options) (http.Handler, error) {
	if err := opts.defaults(); err != nil {
		return nil, err
	}
	a := &api{st: st, opts: opts}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := a.st.Ping(r.Context()); err != nil {
			http.Error(w, "db unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Admin realm.
	mux.Handle("POST /api/v1/runner-tokens", a.admin(a.createRunnerToken))
	mux.Handle("GET /api/v1/runners", a.admin(a.listRunners))
	mux.Handle("PUT /api/v1/flows/{name}", a.admin(a.deployFlow))
	mux.Handle("GET /api/v1/flows", a.admin(a.listFlows))
	mux.Handle("GET /api/v1/flows/{name}", a.admin(a.getFlow))
	mux.Handle("POST /api/v1/flows/{name}/execute", a.admin(a.executeFlow))
	mux.Handle("GET /api/v1/tasks", a.admin(a.listTasks))
	mux.Handle("GET /api/v1/tasks/{id}", a.admin(a.getTask))

	// Runner realm. Registration authenticates by single-use token in the
	// body; everything else by the runner's bearer secret.
	mux.HandleFunc("POST /api/v1/runners/register", a.register)
	mux.Handle("POST /api/v1/lease", a.runner(a.lease))
	mux.Handle("POST /api/v1/tasks/{id}/heartbeat", a.runner(a.heartbeat))
	mux.Handle("POST /api/v1/tasks/{id}/complete", a.runner(a.complete))
	mux.Handle("POST /api/v1/tasks/{id}/fail", a.runner(a.fail))

	return mux, nil
}

// --- auth -----------------------------------------------------------------

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if !strings.HasPrefix(h, p) {
		return ""
	}
	return strings.TrimSpace(h[len(p):])
}

func (a *api) admin(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := bearer(r)
		if subtle.ConstantTimeCompare([]byte(tok), []byte(a.opts.AdminToken)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

type runnerKey struct{}

func (a *api) runner(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := a.st.AuthRunner(r.Context(), bearer(r))
		if err != nil {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), runnerKey{}, id)))
	})
}

func runnerID(r *http.Request) string {
	id, _ := r.Context().Value(runnerKey{}).(string)
	return id
}

// --- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// readBody decodes a bounded JSON body ({} for an empty body).
func readBody(r *http.Request, into any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, into)
}

// --- admin handlers ---------------------------------------------------------

func (a *api) createRunnerToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TTLSeconds int `json:"ttl_seconds"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	token, expires, err := a.st.CreateRegistrationToken(r.Context(), time.Duration(req.TTLSeconds)*time.Second)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), "admin", "runner-token.create", "", nil)
	writeJSON(w, http.StatusCreated, map[string]any{"token": token, "expires_at": expires})
}

func (a *api) listRunners(w http.ResponseWriter, r *http.Request) {
	runners, err := a.st.Runners(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": runners})
}

func (a *api) deployFlow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	if doc.Name != name {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("document name %q does not match URL flow %q", doc.Name, name))
		return
	}
	version, err := a.st.DeployFlow(r.Context(), name, raw)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), "admin", "flow.deploy", name, map[string]int{"version": version})
	writeJSON(w, http.StatusCreated, map[string]any{"name": name, "version": version})
}

func (a *api) listFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := a.st.Flows(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flows})
}

func (a *api) getFlow(w http.ResponseWriter, r *http.Request) {
	f, doc, err := a.st.GetFlow(r.Context(), r.PathValue("name"), 0)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flow": f, "document": json.RawMessage(doc)})
}

func (a *api) executeFlow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Version        int    `json:"version"`
		IdempotencyKey string `json:"idempotency_key"`
		MaxAttempts    int    `json:"max_attempts"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	id, err := a.st.Enqueue(r.Context(), r.PathValue("name"), req.Version, req.IdempotencyKey, req.MaxAttempts)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), "admin", "task.enqueue", id, nil)
	writeJSON(w, http.StatusAccepted, map[string]string{"task_id": id})
}

func (a *api) listTasks(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	tasks, err := a.st.Tasks(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (a *api) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := a.st.GetTask(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	attempts, err := a.st.TaskAttempts(r.Context(), t.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": t, "attempts": attempts})
}

// --- runner handlers --------------------------------------------------------

func (a *api) register(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
		Name  string `json:"name"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" || req.Name == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("token and name are required"))
		return
	}
	id, secret, err := a.st.RegisterRunner(r.Context(), req.Token, req.Name)
	if errors.Is(err, store.ErrTokenInvalid) {
		writeErr(w, http.StatusUnauthorized, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), "runner:"+id, "runner.register", req.Name, nil)
	writeJSON(w, http.StatusCreated, map[string]string{"runner_id": id, "secret": secret})
}

// lease long-polls the queue: it claims immediately when work is queued,
// otherwise re-checks every LeasePoll until wait_seconds (capped) elapse.
func (a *api) lease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WaitSeconds int `json:"wait_seconds"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	wait := min(time.Duration(req.WaitSeconds)*time.Second, a.opts.MaxLeaseWait)
	deadline := time.Now().Add(wait)
	id := runnerID(r)

	for {
		t, err := a.st.Claim(r.Context(), id, a.opts.LeaseTTL)
		if err != nil {
			if r.Context().Err() != nil {
				return // client went away mid-poll
			}
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if t != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"task":              t,
				"lease_ttl_seconds": int(a.opts.LeaseTTL.Seconds()),
			})
			return
		}
		if time.Now().After(deadline) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(a.opts.LeasePoll):
		}
	}
}

func (a *api) heartbeat(w http.ResponseWriter, r *http.Request) {
	err := a.st.Heartbeat(r.Context(), r.PathValue("id"), runnerID(r), a.opts.LeaseTTL)
	if errors.Is(err, store.ErrLeaseLost) {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) complete(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	err = a.st.Complete(r.Context(), r.PathValue("id"), runnerID(r), raw)
	if errors.Is(err, store.ErrLeaseLost) {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) fail(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Error string `json:"error"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	requeued, err := a.st.Fail(r.Context(), r.PathValue("id"), runnerID(r), req.Error)
	if errors.Is(err, store.ErrLeaseLost) {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	state := "failed"
	if requeued {
		state = "queued"
	}
	writeJSON(w, http.StatusOK, map[string]string{"state": state})
}
