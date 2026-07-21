package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/ratelimit"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/webhook"
	"golang.org/x/crypto/bcrypt"
)

// These tests exercise the runner HTTP surface's request-handling paths that
// are reachable WITHOUT a live connector subprocess: validation, auth,
// permission gating, error responses, metadata endpoints, and rate-limit
// rejection. They are fully deterministic (no timing, no subprocess) so they
// run in the SHIFT_COVERAGE gate pass (ADR-0022) as well as make test. The
// connector-spawning happy-path tests live in api_test.go and skip under
// -short.

// newSvc returns a service with no connector directory. Nothing here submits a
// runnable flow, so no connector is ever spawned; Close is immediate.
func newSvc(t *testing.T) *service.Service {
	t.Helper()
	svc := service.New(service.Options{})
	t.Cleanup(func() { _ = svc.Close(5 * time.Second) })
	return svc
}

// serve runs one request through the handler with a recorder (no network).
func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

// req builds an in-memory request; body "" means no body.
func req(method, target, body string) *http.Request {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	r := httptest.NewRequestWithContext(context.Background(), method, target, rd)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func TestHealthMetricsAndRoot(t *testing.T) {
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("# metrics\n"))
	})
	h := Handler(newSvc(t), "r", "1.2.3", time.Now(), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), metrics, nil)

	// Healthz open and returns ok.
	rec := serve(h, req(http.MethodGet, "/healthz", ""))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
	// Wrong method on a known path → 405 (ServeMux method mismatch).
	if rec := serve(h, req(http.MethodPost, "/healthz", "")); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz = %d, want 405", rec.Code)
	}
	// Metrics handler mounted and open.
	rec = serve(h, req(http.MethodGet, "/metrics", ""))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "metrics") {
		t.Fatalf("metrics = %d %q", rec.Code, rec.Body.String())
	}
	// Root serves the dashboard HTML.
	rec = serve(h, req(http.MethodGet, "/", ""))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("root = %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	// Any other path under the root catch-all → 404.
	if rec := serve(h, req(http.MethodGet, "/does-not-exist", "")); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path = %d, want 404", rec.Code)
	}
}

func TestStatusShape(t *testing.T) {
	// Without a hub, no "hub" key.
	h := Handler(newSvc(t), "runner-x", "9.9.9", time.Now().Add(-time.Second), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)
	rec := serve(h, req(http.MethodGet, "/api/status", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["name"] != "runner-x" || body["version"] != "9.9.9" {
		t.Fatalf("status body = %+v", body)
	}
	if _, ok := body["uptime_s"]; !ok {
		t.Fatal("status missing uptime_s")
	}
	if _, ok := body["status"]; !ok {
		t.Fatal("status missing status object")
	}
	if _, ok := body["hub"]; ok {
		t.Fatal("status should not carry hub without hubStatus")
	}

	// With a hubStatus, the "hub" key is populated.
	hub := func() any { return map[string]any{"attached": true} }
	h = Handler(newSvc(t), "r", "0", time.Now(), hub, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)
	rec = serve(h, req(http.MethodGet, "/api/status", ""))
	body = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["hub"]; !ok {
		t.Fatalf("status missing hub: %+v", body)
	}
}

func TestFlowsExecuteRejections(t *testing.T) {
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)

	// Malformed JSON → 400 with error body.
	rec := serve(h, req(http.MethodPost, "/api/flows/execute", `{not json`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json = %d, want 400", rec.Code)
	}
	var errBody map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("error body = %q (%v)", rec.Body.String(), err)
	}

	// Valid JSON but an invalid flow document (missing source/sink) → 400
	// (decode-time validation via flow.Parse).
	if rec := serve(h, req(http.MethodPost, "/api/flows/execute", `{"name":"x"}`)); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid flow = %d, want 400", rec.Code)
	}

	// Oversized body → rejected by the 4 MiB MaxBytesReader (400), never
	// reaching the engine.
	big := `"` + strings.Repeat("a", 5<<20) + `"`
	if rec := serve(h, req(http.MethodPost, "/api/flows/execute", big)); rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized body = %d, want 400", rec.Code)
	}

	// Wrong method on the execute path → 405 (a non-GET method, since GET
	// would fall through to the "GET /" dashboard route).
	if rec := serve(h, req(http.MethodDelete, "/api/flows/execute", "")); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE execute = %d, want 405", rec.Code)
	}
}

func TestTaskMetadataEndpoints(t *testing.T) {
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)

	// Empty task list.
	rec := serve(h, req(http.MethodGet, "/api/tasks", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("tasks = %d", rec.Code)
	}
	var list struct {
		Tasks []any `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Tasks) != 0 {
		t.Fatalf("expected no tasks, got %d", len(list.Tasks))
	}

	// limit query param is accepted.
	if rec := serve(h, req(http.MethodGet, "/api/tasks?limit=5", "")); rec.Code != http.StatusOK {
		t.Fatalf("tasks?limit=5 = %d", rec.Code)
	}

	// Unknown task and its capture → 404.
	if rec := serve(h, req(http.MethodGet, "/api/tasks/nope", "")); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown task = %d, want 404", rec.Code)
	}
	if rec := serve(h, req(http.MethodGet, "/api/tasks/nope/capture", "")); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown capture = %d, want 404", rec.Code)
	}
}

func TestWebhookConfigEndpoints(t *testing.T) {
	reg := webhook.NewRegistry()
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(nil), nil, reg, nil, nil)

	validDoc := `{"document":{"name":"hook","source":{"connector":"@webhook","action":"ndjson"},` +
		`"sink":{"connector":"gen","action":"discard"}}}`

	// Malformed registration JSON → 400.
	if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", `{bad`)); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad webhook json = %d, want 400", rec.Code)
	}

	// Valid JSON but an invalid flow document → 422 (validated at registration).
	badDoc := `{"document":{"name":"x"}}`
	if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", badDoc)); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid webhook doc = %d, want 422", rec.Code)
	}

	// Valid registration, no token → protected:false.
	rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", validDoc))
	if rec.Code != http.StatusOK {
		t.Fatalf("register = %d, want 200", rec.Code)
	}
	var putResp struct {
		Name      string `json:"name"`
		Protected bool   `json:"protected"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &putResp); err != nil {
		t.Fatal(err)
	}
	if putResp.Name != "ingest" || putResp.Protected {
		t.Fatalf("put resp = %+v", putResp)
	}

	// Valid registration WITH a token → protected:true.
	withTok := `{"document":{"name":"hook","source":{"connector":"@webhook","action":"ndjson"},` +
		`"sink":{"connector":"gen","action":"discard"}},"token":"s3cret"}`
	rec = serve(h, req(http.MethodPut, "/api/webhooks/secure", withTok))
	if err := json.Unmarshal(rec.Body.Bytes(), &putResp); err != nil {
		t.Fatal(err)
	}
	if !putResp.Protected {
		t.Fatalf("protected webhook resp = %+v", putResp)
	}

	// Listing includes both names.
	rec = serve(h, req(http.MethodGet, "/api/webhooks", ""))
	var listResp struct {
		Webhooks []string `json:"webhooks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatal(err)
	}
	if len(listResp.Webhooks) != 2 {
		t.Fatalf("webhooks = %+v", listResp)
	}

	// Delete unknown → 404.
	if rec := serve(h, req(http.MethodDelete, "/api/webhooks/absent", "")); rec.Code != http.StatusNotFound {
		t.Fatalf("delete unknown = %d, want 404", rec.Code)
	}
	// Delete known → 204.
	if rec := serve(h, req(http.MethodDelete, "/api/webhooks/ingest", "")); rec.Code != http.StatusNoContent {
		t.Fatalf("delete known = %d, want 204", rec.Code)
	}
}

// TestHookTriggerRejections covers the /hooks/{name} pre-connector rejections:
// unknown hook (404), rate-limit (429), and bad token (401). None of these
// reach flow submission, so no connector is spawned.
func TestHookTriggerRejections(t *testing.T) {
	reg := webhook.NewRegistry()
	// A tiny per-hook budget: burst 1 so the second request is throttled.
	lim := ratelimit.New(map[string]ratelimit.Cfg{"webhook": {RPS: 0.001, Burst: 1}})
	t.Cleanup(lim.Stop)
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(nil), nil, reg, nil, lim)

	// Unknown hook → 404 (resolved before the limiter, so no bucket is minted).
	if rec := serve(h, req(http.MethodPost, "/hooks/nope", `{}`)); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown hook = %d, want 404", rec.Code)
	}

	// Register a token-protected hook. Requests without the token fail the
	// token check AFTER passing the limiter — so the first is 401 and the
	// second (same client IP, burst exhausted) is 429.
	reg.Put(webhook.Hook{Name: "ingest", Doc: []byte(`{"name":"hook",` +
		`"source":{"connector":"@webhook","action":"ndjson"},` +
		`"sink":{"connector":"gen","action":"discard"}}`), TokenHash: hashHookToken("s3cret")})

	rec := serve(h, req(http.MethodPost, "/hooks/ingest", `{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", rec.Code)
	}
	rec = serve(h, req(http.MethodPost, "/hooks/ingest", `{}`))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limited = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 missing Retry-After")
	}

	// A wrong bearer token also yields 401 (fresh limiter so it isn't
	// throttled first): use a distinct hook to avoid the exhausted bucket.
	reg.Put(webhook.Hook{Name: "other", Doc: []byte(`{"name":"hook",` +
		`"source":{"connector":"@webhook","action":"ndjson"},` +
		`"sink":{"connector":"gen","action":"discard"}}`), TokenHash: hashHookToken("s3cret")})
	r := req(http.MethodPost, "/hooks/other", `{}`)
	r.Header.Set("Authorization", "Bearer wrong")
	if rec := serve(h, r); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad bearer = %d, want 401", rec.Code)
	}
}

func TestBenchmarkEndpointRejections(t *testing.T) {
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)

	// Malformed JSON with a positive Content-Length → 400 (never starts a run).
	for _, path := range []string{"/api/benchmark", "/api/benchmark/tiers"} {
		if rec := serve(h, req(http.MethodPost, path, `{bad`)); rec.Code != http.StatusBadRequest {
			t.Fatalf("POST %s bad json = %d, want 400", path, rec.Code)
		}
	}

	// History reads are always available and return a JSON object.
	for _, path := range []string{"/api/benchmark", "/api/benchmark/tiers"} {
		rec := serve(h, req(http.MethodGet, path, ""))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", path, rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET %s body: %v", path, err)
		}
		if _, ok := body["history"]; !ok {
			t.Fatalf("GET %s missing history: %v", path, body)
		}
	}
}

// TestPermissionGating exercises permFor + the auth guard: the required
// permission per method/path, open endpoints, and the 401/403 responses.
func TestPermissionGating(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	// One user per role so read/execute/manage gating is all observable.
	spec := "viewer:" + string(hash) + ":viewer;" +
		"operator:" + string(hash) + ":operator;" +
		"admin:" + string(hash) + ":admin"
	basic, err := auth.NewBasic(spec)
	if err != nil {
		t.Fatal(err)
	}
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("m")) })
	h := Handler(newSvc(t), "r", "0", time.Now(), nil, auth.NewGuard(basic), nil, webhook.NewRegistry(), metrics, nil)

	call := func(method, path, user string) *httptest.ResponseRecorder {
		r := req(method, path, "")
		if user != "" {
			r.SetBasicAuth(user, "pw")
		}
		return serve(h, r)
	}

	// Open endpoints bypass the guard entirely (permFor ok=false).
	if rec := call(http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("healthz open = %d, want 200", rec.Code)
	}
	if rec := call(http.MethodGet, "/metrics", ""); rec.Code != http.StatusOK {
		t.Errorf("metrics open = %d, want 200", rec.Code)
	}
	// Hook endpoints are unguarded by user auth: unknown hook → 404, not 401.
	if rec := call(http.MethodPost, "/hooks/none", ""); rec.Code != http.StatusNotFound {
		t.Errorf("hook = %d, want 404 (not user-guarded)", rec.Code)
	}

	// No credentials on a guarded read → 401 with a challenge header.
	rec := call(http.MethodGet, "/api/status", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no creds = %d, want 401", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic ") {
		t.Fatalf("missing WWW-Authenticate: %q", rec.Header().Get("WWW-Authenticate"))
	}

	// Bad password → 401.
	badPw := req(http.MethodGet, "/api/status", "")
	badPw.SetBasicAuth("viewer", "nope")
	if rec := serve(h, badPw); rec.Code != http.StatusUnauthorized {
		t.Errorf("bad pw = %d, want 401", rec.Code)
	}

	// PermRead: every role may read.
	for _, u := range []string{"viewer", "operator", "admin"} {
		if rec := call(http.MethodGet, "/api/status", u); rec.Code != http.StatusOK {
			t.Errorf("%s read = %d, want 200", u, rec.Code)
		}
	}

	// PermExecute (POST): viewer forbidden, operator allowed past the guard
	// (empty body then fails decode → 400, i.e. it cleared authz).
	if rec := call(http.MethodPost, "/api/flows/execute", "viewer"); rec.Code != http.StatusForbidden {
		t.Errorf("viewer execute = %d, want 403", rec.Code)
	}
	if rec := call(http.MethodPost, "/api/flows/execute", "operator"); rec.Code != http.StatusBadRequest {
		t.Errorf("operator execute = %d, want 400 (past authz, empty body)", rec.Code)
	}

	// PermManage (PUT/DELETE): viewer and operator forbidden, admin allowed
	// past the guard (bad JSON → 400).
	if rec := call(http.MethodPut, "/api/webhooks/x", "viewer"); rec.Code != http.StatusForbidden {
		t.Errorf("viewer manage = %d, want 403", rec.Code)
	}
	if rec := call(http.MethodDelete, "/api/webhooks/x", "operator"); rec.Code != http.StatusForbidden {
		t.Errorf("operator manage = %d, want 403", rec.Code)
	}
	if rec := call(http.MethodPut, "/api/webhooks/x", "admin"); rec.Code != http.StatusBadRequest {
		t.Errorf("admin manage = %d, want 400 (past authz, empty body)", rec.Code)
	}
}
