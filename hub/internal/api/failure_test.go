package api_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/connpolicy"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/ratelimit"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/consign"
)

// openStore is a migrated store for tests that need to reach past the API.
func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return st
}

// serve mounts a handler built from opts on a test server.
func serve(t *testing.T, st *store.Store, opts api.Options) *httptest.Server {
	t.Helper()
	h, err := api.Handler(st, opts)
	if err != nil {
		t.Fatalf("api.Handler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// TestHandlerRejectsUnsafeOptions: the API refuses to start without a usable
// admin realm — a hub that boots with no way to authenticate an operator (or
// with a guessable break-glass token) is a security hole, not a convenience.
func TestHandlerRejectsUnsafeOptions(t *testing.T) {
	st := openStore(t)

	if _, err := api.Handler(st, api.Options{}); err == nil {
		t.Fatal("Handler started with no admin realm at all")
	} else if !strings.Contains(err.Error(), "admin realm is required") {
		t.Fatalf("no-realm error = %v", err)
	}

	if _, err := api.Handler(st, api.Options{AdminToken: "short"}); err == nil {
		t.Fatal("Handler accepted a 5-character break-glass token")
	} else if !strings.Contains(err.Error(), "at least 16") {
		t.Fatalf("short-token error = %v", err)
	}

	// The browser login flow cannot exist without the verifier that validates
	// the tokens it issues.
	idp := oidctest.New(t, "shift-hub")
	flow, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config:      oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: "shift-hub"},
		RedirectURL: "http://127.0.0.1:1/auth/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.Handler(st, api.Options{AdminToken: adminToken, OIDCFlow: flow}); err == nil {
		t.Fatal("Handler accepted OIDCFlow without OIDC")
	} else if !strings.Contains(err.Error(), "OIDCFlow requires OIDC") {
		t.Fatalf("flow-without-verifier error = %v", err)
	}
}

// TestLeaseDefaultsApplied: with no timing options set, a claim still reports
// the documented 30s lease TTL — the runner's heartbeat cadence depends on it.
func TestLeaseDefaultsApplied(t *testing.T) {
	st := openStore(t)
	srv := serve(t, st, api.Options{AdminToken: adminToken}) // no LeaseTTL/LeasePoll

	deployPublish(t, srv.URL)
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken, `{}`, nil); c != 202 {
		t.Fatalf("execute = %d", c)
	}
	secret := registerRunner(t, srv.URL, "default-runner")

	var lease struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
		TTL int `json:"lease_ttl_seconds"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/lease", secret, `{"wait_seconds":0}`, &lease); c != 200 {
		t.Fatalf("lease = %d", c)
	}
	if lease.TTL != 30 {
		t.Fatalf("lease_ttl_seconds = %d, want the 30s default", lease.TTL)
	}
	// An empty body is a valid request on the JSON handlers (readBody treats
	// it as {}), and complete with no body records an empty result.
	if c := call(t, "POST", srv.URL+"/api/v1/runner-tokens", adminToken, "", nil); c != 201 {
		t.Fatalf("runner-tokens with an empty body = %d, want 201", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/"+lease.Task.ID+"/complete", secret, "", nil); c != 204 {
		t.Fatalf("complete with an empty body = %d, want 204", c)
	}
}

// TestControlAPIFailsLoudlyWhenDBIsDown: during a database outage every data
// endpoint must return 500 with the error envelope. Reporting 404/200-empty
// would tell an operator (or the studio) that their flows were deleted.
func TestControlAPIFailsLoudlyWhenDBIsDown(t *testing.T) {
	srv, _, st := newOIDCServer(t)
	deployPublish(t, srv.URL)

	st.Close() // the DB goes away under a live hub

	// Liveness stays green (the process is fine); readiness goes red.
	if _, c := callHdr(t, "GET", srv.URL+"/healthz", nil, nil); c != 200 {
		t.Fatalf("healthz during outage = %d, want 200 (liveness)", c)
	}
	if _, c := callHdr(t, "GET", srv.URL+"/readyz", nil, nil); c != 503 {
		t.Fatalf("readyz during outage = %d, want 503", c)
	}

	for _, path := range []string{
		"/api/v1/stats",
		"/api/v1/runners",
		"/api/v1/flows",
		"/api/v1/flows/orders",
		"/api/v1/flows/orders/graph",
		"/api/v1/flows/orders/schedule",
		"/api/v1/schedules",
		"/api/v1/webhooks",
		"/api/v1/tasks",
		"/api/v1/tasks/00000000-0000-0000-0000-000000000000",
		"/api/v1/executions",
		"/api/v1/usage",
		"/api/v1/usage/events",
		"/api/v1/audit",
		"/api/v1/secrets",
		"/api/v1/publisher-keys",
		"/api/v1/connectors",
		"/api/v1/connectors/myconn/versions",
		"/api/v1/connectors/myconn/resolve?os=linux&arch=amd64",
	} {
		body, c := call2t(t, "GET", srv.URL+path, adminToken, "")
		if c != 500 {
			t.Errorf("GET %s during outage = %d, want 500", path, c)
			continue
		}
		var env struct {
			Error struct {
				Status  int    `json:"status"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &env); err != nil {
			t.Errorf("GET %s: body %q is not the error envelope: %v", path, body, err)
			continue
		}
		if env.Error.Status != 500 || env.Error.Message == "" {
			t.Errorf("GET %s envelope = %+v, want status 500 + message", path, env.Error)
		}
	}

	// Writes fail the same way rather than half-succeeding.
	for _, w := range []struct{ method, path, body string }{
		{"PUT", "/api/v1/flows/orders", goodFlow},
		{"POST", "/api/v1/flows/orders/versions/1/publish", ""},
		{"POST", "/api/v1/flows/orders/execute", `{}`},
		{"POST", "/api/v1/runner-tokens", `{}`},
		{"PUT", "/api/v1/webhooks/hook1", `{"flow_name":"orders"}`},
		{"PUT", "/api/v1/secrets/api_key", `{"value":"v"}`},
		{"POST", "/api/v1/keys/rotate", ""},
	} {
		if c := call(t, w.method, srv.URL+w.path, adminToken, w.body, nil); c != 500 {
			t.Errorf("%s %s during outage = %d, want 500", w.method, w.path, c)
		}
	}

	// Deleting during an outage must not report a spurious 404 either.
	for _, path := range []string{"/api/v1/webhooks/hook1", "/api/v1/flows/orders/schedule", "/api/v1/secrets/api_key"} {
		if c := call(t, "DELETE", srv.URL+path, adminToken, "", nil); c != 500 {
			t.Errorf("DELETE %s during outage = %d, want 500", path, c)
		}
	}

	// The dashboard shell is static and keeps serving; the login probe too.
	if _, c := callHdr(t, "GET", srv.URL+"/", nil, nil); c != 200 {
		t.Errorf("dashboard during outage = %d, want 200 (static page)", c)
	}
	if c := call(t, "GET", srv.URL+"/api/v1/authinfo", "", "", nil); c != 200 {
		t.Errorf("authinfo during outage = %d, want 200 (no DB needed)", c)
	}
}

// TestRealmSeparation: the human and runner realms are disjoint. A runner
// secret must never reach admin surfaces, and the break-glass admin token must
// never drive the queue protocol — a stolen credential's blast radius is
// bounded by its realm (ADR-0009).
func TestRealmSeparation(t *testing.T) {
	srv := newServer(t)
	deployPublish(t, srv.URL)
	secret := registerRunner(t, srv.URL, "realm-runner")

	adminOnly := []struct{ method, path, body string }{
		{"GET", "/api/v1/flows", ""},
		{"GET", "/api/v1/tasks", ""},
		{"GET", "/api/v1/runners", ""},
		{"GET", "/api/v1/stats", ""},
		{"GET", "/api/v1/audit", ""},
		{"GET", "/api/v1/schedules", ""},
		{"GET", "/api/v1/webhooks", ""},
		{"GET", "/api/v1/usage", ""},
		{"GET", "/api/v1/me", ""},
		{"POST", "/api/v1/runner-tokens", `{}`},
		{"PUT", "/api/v1/flows/orders", goodFlow},
		{"POST", "/api/v1/flows/orders/execute", `{}`},
	}
	for _, e := range adminOnly {
		if c := call(t, e.method, srv.URL+e.path, secret, e.body, nil); c != 401 {
			t.Errorf("%s %s with a runner secret = %d, want 401", e.method, e.path, c)
		}
	}

	runnerOnly := []struct{ method, path, body string }{
		{"POST", "/api/v1/lease", `{"wait_seconds":0}`},
		{"POST", "/api/v1/tasks/00000000-0000-0000-0000-000000000000/heartbeat", ""},
		{"POST", "/api/v1/tasks/00000000-0000-0000-0000-000000000000/complete", `{}`},
		{"POST", "/api/v1/tasks/00000000-0000-0000-0000-000000000000/fail", `{"error":"x"}`},
		{"POST", "/api/v1/executions", `{"flow_name":"orders","state":"completed"}`},
		{"GET", "/api/v1/webhooks/sync", ""},
	}
	for _, e := range runnerOnly {
		if c := call(t, e.method, srv.URL+e.path, adminToken, e.body, nil); c != 401 {
			t.Errorf("%s %s with the admin token = %d, want 401", e.method, e.path, c)
		}
		if c := call(t, e.method, srv.URL+e.path, "", e.body, nil); c != 401 {
			t.Errorf("%s %s unauthenticated = %d, want 401", e.method, e.path, c)
		}
	}

	// A revoked-looking / garbage bearer is one opaque 401 in both realms —
	// no oracle distinguishing "unknown" from "wrong".
	for _, cred := range []string{"sr_deadbeef", "Bearer", adminToken + "x", ""} {
		if c := call(t, "GET", srv.URL+"/api/v1/flows", cred, "", nil); c != 401 {
			t.Errorf("admin realm with %q = %d, want 401", cred, c)
		}
	}
}

// TestMethodNotAllowed: a known path with the wrong verb is 405 (with an Allow
// header), not a 404 that would look like the route does not exist.
func TestMethodNotAllowed(t *testing.T) {
	srv := newServer(t)

	for _, e := range []struct{ method, path, allow string }{
		{"POST", "/api/v1/flows", "GET"},
		{"DELETE", "/api/v1/tasks", "GET"},
		{"PUT", "/api/v1/webhooks", "GET"},
		{"POST", "/api/v1/audit", "GET"},
	} {
		req, err := http.NewRequestWithContext(t.Context(), e.method, srv.URL+e.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+adminToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 405", e.method, e.path, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); !strings.Contains(got, e.allow) {
			t.Errorf("%s %s Allow = %q, want it to include %s", e.method, e.path, got, e.allow)
		}
	}
}

// TestUsageEventsExport covers the billing-pull contract: an id cursor that
// only advances on a full page, an honoured limit, a CSV rendering, and
// rejection of unparseable time bounds.
func TestUsageEventsExport(t *testing.T) {
	srv := newServer(t)
	secret := registerRunner(t, srv.URL, "usage-runner")

	for i := range 3 {
		body := `{"flow_name":"orders","state":"completed","trigger":"webhook","records_in":` +
			string(rune('1'+i)) + `,"records_out":0}`
		if c := call(t, "POST", srv.URL+"/api/v1/executions", secret, body, nil); c != 201 {
			t.Fatalf("report %d = %d", i, c)
		}
	}

	type page struct {
		Events []struct {
			ID       int64  `json:"id"`
			Source   string `json:"source"`
			FlowName string `json:"flow_name"`
			Outcome  string `json:"outcome"`
		} `json:"events"`
		Next int64 `json:"next"`
	}

	// A full page hands back a cursor; the caller resumes from it.
	var first page
	if c := call(t, "GET", srv.URL+"/api/v1/usage/events?limit=2", adminToken, "", &first); c != 200 {
		t.Fatalf("events limit=2 = %d", c)
	}
	if len(first.Events) != 2 || first.Next != first.Events[1].ID {
		t.Fatalf("first page = %+v (next %d), want 2 events with next = last id", first.Events, first.Next)
	}
	if first.Events[0].Source != "webhook" || first.Events[0].Outcome != "completed" {
		t.Fatalf("event = %+v, want the reported webhook execution", first.Events[0])
	}

	var second page
	url := srv.URL + "/api/v1/usage/events?limit=2&since_id=" + itoa(first.Next)
	if c := call(t, "GET", url, adminToken, "", &second); c != 200 {
		t.Fatalf("resumed page = %d", c)
	}
	// A partial page means "caught up": the cursor stops advancing.
	if len(second.Events) != 1 || second.Next != 0 {
		t.Fatalf("resumed page = %+v (next %d), want 1 event and next 0", second.Events, second.Next)
	}
	if second.Events[0].ID <= first.Next {
		t.Fatalf("cursor is not exclusive: got id %d after since_id %d", second.Events[0].ID, first.Next)
	}

	// An unparseable cursor is ignored (treated as "from the start") rather
	// than erroring the billing pull.
	var all page
	if c := call(t, "GET", srv.URL+"/api/v1/usage/events?since_id=abc", adminToken, "", &all); c != 200 || len(all.Events) != 3 {
		t.Fatalf("garbage since_id = %d with %d events, want 200 with all 3", c, len(all.Events))
	}
	// A non-numeric limit falls back to the default page rather than erroring.
	if c := call(t, "GET", srv.URL+"/api/v1/usage/events?limit=nope", adminToken, "", &all); c != 200 || len(all.Events) != 3 {
		t.Fatalf("garbage limit = %d with %d events", c, len(all.Events))
	}

	// CSV rendering: fixed header, one row per event. The header is a CONTRACT
	// with whatever ingests the export, so it is pinned here — a column added
	// or renamed without a deliberate change breaks a consumer we cannot see.
	body, c := call2t(t, "GET", srv.URL+"/api/v1/usage/events?format=csv", adminToken, "")
	if c != 200 {
		t.Fatalf("csv export = %d", c)
	}
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if lines[0] != "id,at,source,flow_name,outcome,records_in,records_out,exec_seconds,test" {
		t.Fatalf("csv header = %q", lines[0])
	}
	if len(lines) != 4 {
		t.Fatalf("csv = %d lines, want header + 3 rows:\n%s", len(lines), body)
	}
	for _, ln := range lines[1:] {
		if !strings.Contains(ln, "webhook") || !strings.Contains(ln, "orders") {
			t.Fatalf("csv row = %q, want the reported execution", ln)
		}
	}

	// A headless page carries the SAME columns in the same order, minus the
	// names — so a paging consumer can concatenate pages without a header row
	// landing mid-file. Whether row 1 is data is declared in the media type
	// (RFC 4180), never guessed from the body.
	headless, hdrs, c := call3t(t, "GET", srv.URL+"/api/v1/usage/events?format=csv&header=absent", adminToken, "")
	if c != 200 {
		t.Fatalf("headless csv = %d", c)
	}
	if ct := hdrs.Get("Content-Type"); !strings.Contains(ct, "header=absent") {
		t.Fatalf("headless Content-Type = %q, want the RFC 4180 header=absent parameter", ct)
	}
	bare := strings.Split(strings.TrimRight(headless, "\n"), "\n")
	if len(bare) != 3 {
		t.Fatalf("headless csv = %d lines, want 3 rows and no header:\n%s", len(bare), headless)
	}
	if strings.HasPrefix(bare[0], "id,at,") {
		t.Fatalf("header=absent still emitted the header: %q", bare[0])
	}
	// Same column count in both modes: dropping the names must not drop or
	// reorder a field, or the positional contract silently differs by mode.
	if got, want := strings.Count(bare[0], ","), strings.Count(lines[0], ","); got != want {
		t.Fatalf("headless row has %d separators, headed header has %d", got, want)
	}
	// The cursor a JSON caller reads from the body travels in a header here,
	// so a headless consumer never has to parse the payload to page.
	if hdrs.Get("X-Shift-Next-Cursor") != "0" {
		t.Fatalf("next cursor = %q, want 0 (a partial page means caught up)",
			hdrs.Get("X-Shift-Next-Cursor"))
	}

	// Time-bounded rollup: both bounds parse as RFC3339, and neither is optional
	// to get right — a typo is a 400, never a silently empty report.
	var rep struct {
		Totals struct {
			Executions int64 `json:"executions"`
		} `json:"totals"`
	}
	until := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	since := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	if c := call(t, "GET", srv.URL+"/api/v1/usage?since="+since+"&until="+until, adminToken, "", &rep); c != 200 {
		t.Fatalf("bounded usage = %d", c)
	}
	if rep.Totals.Executions != 3 {
		t.Fatalf("bounded usage executions = %d, want 3", rep.Totals.Executions)
	}
	// A window that ends before the events start is empty, not an error.
	past := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	if c := call(t, "GET", srv.URL+"/api/v1/usage?until="+past, adminToken, "", &rep); c != 200 || rep.Totals.Executions != 0 {
		t.Fatalf("past window = %d with %d executions, want 200/0", c, rep.Totals.Executions)
	}
	if c := call(t, "GET", srv.URL+"/api/v1/usage?until=not-a-time", adminToken, "", nil); c != 400 {
		t.Fatalf("bad until = %d, want 400", c)
	}
}

// itoa avoids pulling strconv in for one call site.
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// TestMetricsRouteServedWhenConfigured: /metrics exists only when a scrape
// handler is supplied, and it is deliberately unauthenticated (gated by
// network posture, ADR-0020).
func TestMetricsRouteServedWhenConfigured(t *testing.T) {
	st := openStore(t)

	plain := serve(t, st, api.Options{AdminToken: adminToken})
	if _, c := callHdr(t, "GET", plain.URL+"/metrics", nil, nil); c != 404 {
		t.Fatalf("/metrics without a handler = %d, want 404", c)
	}

	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("shift_up 1\n"))
	})
	withMetrics := serve(t, st, api.Options{AdminToken: adminToken, MetricsHandler: metrics})
	body, c := callHdr(t, "GET", withMetrics.URL+"/metrics", nil, nil)
	if c != 200 || !strings.Contains(body, "shift_up 1") {
		t.Fatalf("/metrics = %d %q", c, body)
	}
}

// TestRateLimitRejectsFloods pins ADR-0021: each class is throttled on its own
// key, over its budget the answer is 429 with a Retry-After hint, and the
// limiter counts what it rejected.
func TestRateLimitRejectsFloods(t *testing.T) {
	st := openStore(t)
	lim := ratelimit.New(map[string]ratelimit.Cfg{
		"public": {RPS: 0.001, Burst: 1},
		"admin":  {RPS: 0.001, Burst: 1},
		"runner": {RPS: 0.001, Burst: 1},
	})
	t.Cleanup(lim.Stop)
	srv := serve(t, st, api.Options{AdminToken: adminToken, RateLimit: lim})

	// Public class: the unauthenticated probe burns its single token.
	if c := call(t, "GET", srv.URL+"/api/v1/authinfo", "", "", nil); c != 200 {
		t.Fatalf("first public request = %d, want 200", c)
	}
	body, c := callHdr(t, "GET", srv.URL+"/api/v1/authinfo", nil, nil)
	if c != 429 {
		t.Fatalf("second public request = %d, want 429", c)
	}
	if !strings.Contains(body, "rate limited") {
		t.Fatalf("429 body = %q, want a rate-limited message", body)
	}

	// Admin class is keyed separately, so it still has its own token.
	if c := call(t, "GET", srv.URL+"/api/v1/me", adminToken, "", nil); c != 200 {
		t.Fatalf("first admin request = %d, want 200", c)
	}
	if c := call(t, "GET", srv.URL+"/api/v1/me", adminToken, "", nil); c != 429 {
		t.Fatalf("second admin request = %d, want 429", c)
	}

	if got := lim.Rejected("public"); got != 1 {
		t.Errorf("public rejections = %d, want 1", got)
	}
	if got := lim.Rejected("admin"); got != 1 {
		t.Errorf("admin rejections = %d, want 1", got)
	}
	// The registration route is public-classed too, so the flood already
	// closed it — pre-auth token brute force is throttled (ADR-0021).
	if c := call(t, "POST", srv.URL+"/api/v1/runners/register", "", `{"token":"srt_x","name":"r"}`, nil); c != 429 {
		t.Fatalf("registration during a public flood = %d, want 429", c)
	}
}

// TestRunnerRealmRateLimited: an authenticated runner is throttled on its own
// id, so one misbehaving runner cannot starve the others.
func TestRunnerRealmRateLimited(t *testing.T) {
	st := openStore(t)
	lim := ratelimit.New(map[string]ratelimit.Cfg{"runner": {RPS: 0.001, Burst: 1}})
	t.Cleanup(lim.Stop)
	srv := serve(t, st, api.Options{
		AdminToken: adminToken, RateLimit: lim,
		LeaseTTL: 2 * time.Second, LeasePoll: 20 * time.Millisecond,
	})
	secret := registerRunner(t, srv.URL, "flooding-runner")

	if c := call(t, "POST", srv.URL+"/api/v1/lease", secret, `{"wait_seconds":0}`, nil); c != 204 {
		t.Fatalf("first lease = %d, want 204 (empty queue)", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/lease", secret, `{"wait_seconds":0}`, nil); c != 429 {
		t.Fatalf("second lease = %d, want 429", c)
	}
	if got := lim.Rejected("runner"); got != 1 {
		t.Errorf("runner rejections = %d, want 1", got)
	}
	// The admin realm is unthrottled here (no admin class configured).
	if c := call(t, "GET", srv.URL+"/api/v1/me", adminToken, "", nil); c != 200 {
		t.Fatalf("admin request under a runner-only limiter = %d, want 200", c)
	}
}

// TestConnectorPolicyHidesEveryRegistrySurface (ADR-0015): a denied connector
// must be invisible, not merely unusable — list, resolve, download and version
// history all answer as if it were never published.
func TestConnectorPolicyHidesEveryRegistrySurface(t *testing.T) {
	st := openStore(t)
	openHub := serve(t, st, api.Options{AdminToken: adminToken})

	// Publish a signed artifact on an unrestricted hub.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if c := call(t, "POST", openHub.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"pub1","public_key":"`+base64.StdEncoding.EncodeToString(pub)+`"}`, nil); c != 201 {
		t.Fatalf("addPublisherKey = %d", c)
	}
	art := []byte("dangerous-connector-artifact")
	sum := sha256.Sum256(art)
	m := consign.Manifest{Name: "exec", Version: "1.0.0", OS: "linux", Arch: "amd64"}
	copy(m.Digest[:], sum[:])
	upHdr := map[string]string{
		"Authorization":         "Bearer " + adminToken,
		"X-Shift-Publisher-Key": "pub1",
		"X-Shift-Signature":     base64.StdEncoding.EncodeToString(consign.Sign(priv, m)),
	}
	if _, c := callHdr(t, "PUT", openHub.URL+"/api/v1/connectors/exec/versions/1.0.0?os=linux&arch=amd64", upHdr, art); c != 201 {
		t.Fatalf("upload = %d", c)
	}
	// The open hub serves it, so the artifact really is there.
	if c := call(t, "GET", openHub.URL+"/api/v1/connectors/exec/resolve?os=linux&arch=amd64", adminToken, "", nil); c != 200 {
		t.Fatalf("resolve on the open hub = %d", c)
	}

	// The same store behind a restricted hub: every registry surface hides it.
	locked := serve(t, st, api.Options{
		AdminToken:      adminToken,
		ConnectorPolicy: connpolicy.Parse("", "exec"), // deny exec
	})
	for _, path := range []string{
		"/api/v1/connectors/exec/resolve?os=linux&arch=amd64",
		"/api/v1/connectors/exec/versions",
		"/api/v1/connectors/exec/versions/1.0.0/artifact?os=linux&arch=amd64",
	} {
		if c := call(t, "GET", locked.URL+path, adminToken, "", nil); c != 404 {
			t.Errorf("GET %s on a restricted hub = %d, want 404 (hidden by policy)", path, c)
		}
	}
	body, c := call2t(t, "GET", locked.URL+"/api/v1/connectors", adminToken, "")
	if c != 200 || strings.Contains(body, "exec") {
		t.Fatalf("listConnectors on a restricted hub = %d body=%s, want the denied connector omitted", c, body)
	}
}

// TestYankRequestParsing: the yank endpoint rejects a malformed body outright
// and, given an empty one, defaults to yanking on the hub's own platform.
func TestYankRequestParsing(t *testing.T) {
	srv := newServer(t)

	// Malformed JSON → 400 before anything is mutated.
	if c := call(t, "POST", srv.URL+"/api/v1/connectors/myconn/versions/1.0.0/yank", adminToken, `{bad`, nil); c != 400 {
		t.Fatalf("yank with a malformed body = %d, want 400", c)
	}
	// Empty body → defaults applied; nothing is published, so the store
	// reports the miss as 404 rather than inventing a row.
	if c := call(t, "POST", srv.URL+"/api/v1/connectors/myconn/versions/1.0.0/yank", adminToken, "", nil); c != 404 {
		t.Fatalf("yank of an unpublished version = %d, want 404", c)
	}
}

// TestFlowGraphRejectsUndecodableStoredDocument: a document that no longer
// parses (older schema, hand-edited row) yields 422 from the studio's graph
// endpoint, not a 500 or a half-built graph.
func TestFlowGraphRejectsUndecodableStoredDocument(t *testing.T) {
	st := openStore(t)
	srv := serve(t, st, api.Options{AdminToken: adminToken})

	// Write past the API's validation, the way a stale row would look.
	ctx := t.Context()
	if _, err := st.DeployFlow(ctx, "legacy", json.RawMessage(`{"name":"legacy","steps":[]}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.PublishFlow(ctx, "legacy", 1); err != nil {
		t.Fatal(err)
	}

	body, c := call2t(t, "GET", srv.URL+"/api/v1/flows/legacy/graph", adminToken, "")
	if c != 422 {
		t.Fatalf("graph of an unparseable document = %d, want 422 (body %s)", c, body)
	}
	// The raw document is still readable so an operator can repair it.
	if c := call(t, "GET", srv.URL+"/api/v1/flows/legacy", adminToken, "", nil); c != 200 {
		t.Fatalf("getFlow of an unparseable document = %d, want 200", c)
	}
}
