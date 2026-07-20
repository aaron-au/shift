// Package api is the hub's HTTP control API (ADR-0009): admin endpoints
// for flows/tasks/runner-tokens behind the admin bearer token, and runner
// endpoints (register/lease/heartbeat/complete/fail) behind per-runner
// bearer secrets. JSON in, JSON out, stdlib mux only. The hub never sees
// payload data — documents and results are control-plane metadata.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/hub/internal/connpolicy"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/ratelimit"
	"github.com/aaron-au/shift/hub/internal/scheduler"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Options configure the API.
type Options struct {
	// AdminToken is the break-glass admin credential (min 16 bytes when
	// set). Optional once OIDC is configured; at least one of the two is
	// required.
	AdminToken string
	// OIDC verifies human bearer tokens / session cookies. Optional.
	OIDC *oidcauth.Verifier
	// OIDCFlow enables the dashboard's browser login (/auth/*). Optional;
	// requires OIDC.
	OIDCFlow *oidcauth.Flow
	// Secrets enables the secrets endpoints. Optional (absent without a
	// configured KEK).
	Secrets *secrets.Service
	// SchedStatus reports the scheduler loop's last pass for /api/v1/stats.
	// Optional (tests and API-only deployments).
	SchedStatus func() scheduler.Status
	// ConnectorPolicy is the per-deployment connector capability policy:
	// disallowed connectors are hidden from listing/resolution and rejected
	// at deploy. Optional; nil allows everything (self-hosted default).
	ConnectorPolicy *connpolicy.Policy
	// LeaseTTL is how long a claimed task stays leased between heartbeats
	// (default 30s).
	LeaseTTL time.Duration
	// LeasePoll is the claim re-check interval inside a long-poll lease
	// request (default 200ms).
	LeasePoll time.Duration
	// MaxLeaseWait caps a lease request's long-poll (default 30s).
	MaxLeaseWait time.Duration
	// MetricsHandler serves GET /metrics (Prometheus scrape, M6a). Optional;
	// unauthenticated like the dashboard root — gate by network posture.
	MetricsHandler http.Handler
	// RateLimit throttles the control API per identity/IP (M6c, ADR-0021).
	// Optional; nil (or a class with RPS<=0) disables limiting.
	RateLimit *ratelimit.Limiter
}

func (o *Options) defaults() error {
	if o.AdminToken == "" && o.OIDC == nil {
		return fmt.Errorf("api: an admin realm is required — configure OIDC or a break-glass admin token")
	}
	if o.AdminToken != "" && len(o.AdminToken) < 16 {
		return fmt.Errorf("api: admin token must be at least 16 characters")
	}
	if o.OIDCFlow != nil && o.OIDC == nil {
		return fmt.Errorf("api: OIDCFlow requires OIDC")
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

	// Dashboard page (static; its data calls are authenticated). The
	// authinfo probe is unauthenticated so the login page knows whether
	// to offer OIDC — it reveals only which login methods exist.
	mux.HandleFunc("GET /", a.publicLimit(a.dashboard))
	mux.HandleFunc("GET /api/v1/authinfo", a.publicLimit(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{
			"oidc_login":  opts.OIDCFlow != nil,
			"break_glass": opts.AdminToken != "",
		})
	}))
	mux.Handle("GET /api/v1/stats", a.admin(a.stats))
	mux.Handle("GET /api/v1/audit", a.admin(a.listAudit)) // M6b

	if opts.MetricsHandler != nil {
		mux.Handle("GET /metrics", opts.MetricsHandler) // Prometheus scrape (M6a, ADR-0020)
	}
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
	mux.Handle("GET /api/v1/flows/{name}/graph", a.admin(a.getFlowGraph))
	mux.Handle("POST /api/v1/flows/{name}/versions/{version}/publish", a.admin(a.publishFlow))
	mux.Handle("POST /api/v1/flows/{name}/execute", a.admin(a.executeFlow))
	mux.Handle("PUT /api/v1/flows/{name}/schedule", a.admin(a.putSchedule))
	mux.Handle("GET /api/v1/flows/{name}/schedule", a.admin(a.getSchedule))
	mux.Handle("DELETE /api/v1/flows/{name}/schedule", a.admin(a.deleteSchedule))
	mux.Handle("GET /api/v1/schedules", a.admin(a.listSchedules))
	mux.Handle("PUT /api/v1/webhooks/{name}", a.admin(a.putWebhook))
	mux.Handle("GET /api/v1/webhooks", a.admin(a.listWebhooks))
	mux.Handle("DELETE /api/v1/webhooks/{name}", a.admin(a.deleteWebhook))
	mux.Handle("GET /api/v1/webhooks/sync", a.runner(a.syncWebhooks))
	mux.Handle("GET /api/v1/tasks", a.admin(a.listTasks))
	mux.Handle("GET /api/v1/executions", a.admin(a.listDirectExecutions))
	mux.Handle("GET /api/v1/tasks/{id}", a.admin(a.getTask))
	mux.Handle("GET /api/v1/me", a.admin(a.me))

	// Secrets (admin manages; runners resolve). Absent without a KEK.
	if opts.Secrets != nil {
		mux.Handle("PUT /api/v1/secrets/{name}", a.admin(a.putSecret))
		mux.Handle("GET /api/v1/secrets", a.admin(a.listSecrets))
		mux.Handle("DELETE /api/v1/secrets/{name}", a.admin(a.deleteSecret))
		mux.Handle("POST /api/v1/keys/rotate", a.admin(a.rotateKEK))
		mux.Handle("POST /api/v1/secrets/resolve", a.runner(a.resolveSecrets))
	}

	// Connector registry: publishing is admin-only; resolve/fetch and
	// the trusted-key list serve runners too (their verification path).
	mux.Handle("POST /api/v1/publisher-keys", a.admin(a.addPublisherKey))
	mux.Handle("GET /api/v1/publisher-keys", a.adminOrRunner(a.listPublisherKeys))
	mux.Handle("DELETE /api/v1/publisher-keys/{id}", a.admin(a.revokePublisherKey))
	mux.Handle("PUT /api/v1/connectors/{name}/versions/{version}", a.admin(a.uploadConnector))
	mux.Handle("GET /api/v1/connectors", a.admin(a.listConnectors))
	mux.Handle("GET /api/v1/connectors/{name}/resolve", a.adminOrRunner(a.resolveConnector))
	mux.Handle("GET /api/v1/connectors/{name}/versions/{version}/artifact", a.adminOrRunner(a.downloadConnector))

	// Dashboard browser login.
	if opts.OIDCFlow != nil {
		mux.HandleFunc("GET /auth/login", a.publicLimit(a.login))
		mux.HandleFunc("GET /auth/callback", a.publicLimit(a.callback))
		mux.HandleFunc("GET /auth/logout", a.publicLimit(a.logout))
	}

	// Runner realm. Registration authenticates by single-use token in the
	// body; everything else by the runner's bearer secret.
	mux.HandleFunc("POST /api/v1/runners/register", a.register)
	mux.Handle("POST /api/v1/lease", a.runner(a.lease))
	mux.Handle("POST /api/v1/tasks/{id}/heartbeat", a.runner(a.heartbeat))
	mux.Handle("POST /api/v1/tasks/{id}/complete", a.runner(a.complete))
	mux.Handle("POST /api/v1/tasks/{id}/fail", a.runner(a.fail))
	mux.Handle("POST /api/v1/executions", a.runner(a.reportExecution))

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
	_ = a.st.Audit(r.Context(), actor(r), "runner-token.create", "", nil)
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
	if err := a.checkSecretRefs(r, doc); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	if err := a.checkConnectorPolicy(doc); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	version, err := a.st.DeployFlow(r.Context(), name, raw)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "flow.deploy", name, map[string]int{"version": version})
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

// getFlowGraph returns the published flow's render graph (nodes + typed
// outcome edges) for the studio — data-free, no payload.
func (a *api) getFlowGraph(w http.ResponseWriter, r *http.Request) {
	_, doc, err := a.st.GetFlow(r.Context(), r.PathValue("name"), 0)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	parsed, err := flowdoc.Parse(doc)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	g, err := parsed.GraphView()
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

// publishFlow marks a version published (POST .../versions/{version}/publish).
func (a *api) publishFlow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || version < 1 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("version must be a positive integer"))
		return
	}
	err = a.st.PublishFlow(r.Context(), name, version)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "flow.publish", name, map[string]int{"version": version})
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "published_version": version})
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
	// The scheduler derives its dedup keys as "sched:<id>:<tick>"; a
	// user key in that namespace could silently absorb a tick.
	if strings.HasPrefix(req.IdempotencyKey, "sched:") {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf(`idempotency keys may not use the reserved "sched:" prefix`))
		return
	}
	id, err := a.st.Enqueue(r.Context(), r.PathValue("name"), req.Version, req.IdempotencyKey, req.MaxAttempts)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if errors.Is(err, store.ErrNotPublished) {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "task.enqueue", id, nil)
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

// listDirectExecutions returns the account's recent direct executions.
func (a *api) listDirectExecutions(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	execs, err := a.st.DirectExecutions(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": execs})
}

// reportExecution records a runner-reported direct (push) execution — a
// webhook / direct-API run that never entered the queue (ADR-0016). It is
// metadata only; no payload crosses to the hub.
func (a *api) reportExecution(w http.ResponseWriter, r *http.Request) {
	var e store.DirectExecution
	if err := readBody(r, &e); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if e.FlowName == "" || (e.State != "completed" && e.State != "failed") {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("flow_name and a terminal state are required"))
		return
	}
	if e.Trigger == "" {
		e.Trigger = "api"
	}
	id, err := a.st.RecordDirectExecution(r.Context(), runnerID(r), e)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
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
