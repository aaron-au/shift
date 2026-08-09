package api_test

// TC-012: EVERY registered route answers errors in the ADR-0023 envelope.
//
// The existing envelope tests are per handler, which is exactly the shape that
// stops covering new code: a handler added tomorrow that answers
// `http.Error(w, "boom", 500)` passes a suite that only knows about the
// handlers written yesterday. So this sweep does not hand-list routes. It
// reads them out of the router registration in api.go and asserts that the set
// of cases below MATCHES it — adding a route without adding a case fails the
// test, which is the only thing that keeps a sweep a sweep.
//
// Each route is then driven to an error and the response is checked for: the
// documented envelope shape, `application/json`, the path-carried API version
// (ADR-0023 versions in the path, so /api/v1 IS the version signal), a machine
// code that is a stable token when present, and no internal detail — no SQL,
// no Go error chains naming internal packages, no stack traces, and none of
// the credentials the request carried.

import (
	"crypto/rand"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
)

// How a route's error is provoked.
type provokeMode int

const (
	// modeLive drives the route on a healthy hub with input it must reject.
	modeLive provokeMode = iota
	// modeDeadDB drives it on a hub whose database has gone away — the only way
	// to reach the error arm of a read that takes no input.
	modeDeadDB
	// modeHiddenTable renames one table out from under a live hub, for the
	// runner-realm reads whose own credential check needs the database and so
	// cannot be driven by taking the whole database away.
	modeHiddenTable
	// modeNoError documents a route with NO reachable error response. The sweep
	// asserts these really do succeed, so the exemption cannot quietly go
	// stale once somebody adds validation.
	modeNoError
	// modeNotJSON documents a route that is deliberately not a JSON API surface.
	// Asserted to leak nothing; the envelope does not apply.
	modeNotJSON
)

type sweepCase struct {
	pattern string // EXACTLY as registered in api.go — the key the sweep matches on
	path    string // concrete request path
	realm   string // "admin" | "runner" | "none"
	body    string
	mode    provokeMode
	table   string // hiddenTable mode: the table to rename away
	want    int    // expected status (live/deadDB/hiddenTable modes)
	why     string // noError / notJSON: why there is nothing to provoke
}

// A syntactically valid UUID that names nothing.
const ghostID = "00000000-0000-0000-0000-0000000000ff"

// badJSON is a body no handler can decode. It reaches the FIRST thing every
// write handler does, which is the arm most likely to be hand-rolled.
const badJSON = `{`

func sweepCases() []sweepCase {
	return []sweepCase{
		{pattern: "GET /", path: "/", mode: modeNotJSON, why: "the dashboard shell is a static HTML page"},
		{pattern: "GET /metrics", path: "/metrics", mode: modeNotJSON, why: "Prometheus text exposition, not a JSON API"},
		{pattern: "GET /healthz", path: "/healthz", mode: modeNotJSON, why: "liveness probe: a status code and nothing else"},
		{pattern: "GET /readyz", path: "/readyz", mode: modeNotJSON, why: "readiness probe: a status code and nothing else"},
		{pattern: "GET /auth/login", path: "/auth/login", mode: modeNotJSON, why: "browser redirect to the IdP"},
		{pattern: "GET /auth/logout", path: "/auth/logout", mode: modeNotJSON, why: "browser redirect after clearing the session"},

		{pattern: "GET /api/v1/authinfo", path: "/api/v1/authinfo", mode: modeNoError,
			why: "reports which login methods are configured, from options alone; nothing it reads can fail"},
		{pattern: "GET /api/v1/me", path: "/api/v1/me", realm: "admin", mode: modeNoError,
			why: "echoes the caller's own already-authenticated identity"},
		{pattern: "GET /api/v1/review-checks", path: "/api/v1/review-checks", realm: "admin", mode: modeNoError,
			why: "an in-process list of the review checks; it touches no state"},

		{pattern: "GET /auth/callback", path: "/auth/callback?code=x&state=y", want: 400},

		{pattern: "GET /api/v1/stats", path: "/api/v1/stats", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/audit", path: "/api/v1/audit", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/runners", path: "/api/v1/runners", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/gateways", path: "/api/v1/gateways", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/routes", path: "/api/v1/routes", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/flows", path: "/api/v1/flows", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/schedules", path: "/api/v1/schedules", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/webhooks", path: "/api/v1/webhooks", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/tasks", path: "/api/v1/tasks", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/executions", path: "/api/v1/executions", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/usage", path: "/api/v1/usage", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/usage/events", path: "/api/v1/usage/events", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/secrets", path: "/api/v1/secrets", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/connections", path: "/api/v1/connections", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/publisher-keys", path: "/api/v1/publisher-keys", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/connectors", path: "/api/v1/connectors", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/connectors/eol", path: "/api/v1/connectors/eol", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "GET /api/v1/connector-upgrades", path: "/api/v1/connector-upgrades", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "POST /api/v1/keys/rotate", path: "/api/v1/keys/rotate", realm: "admin", mode: modeDeadDB, want: 500},

		{pattern: "GET /api/v1/gateways/sync", path: "/api/v1/gateways/sync", realm: "runner",
			mode: modeHiddenTable, table: "gateways", want: 500},
		{pattern: "GET /api/v1/webhooks/sync", path: "/api/v1/webhooks/sync", realm: "runner",
			mode: modeHiddenTable, table: "webhooks", want: 500},

		{pattern: "POST /api/v1/runner-tokens", path: "/api/v1/runner-tokens", realm: "admin", body: badJSON, want: 400},
		{pattern: "DELETE /api/v1/runners/{id}", path: "/api/v1/runners/" + ghostID, realm: "admin", want: 404},
		{pattern: "PUT /api/v1/runners/{id}/labels", path: "/api/v1/runners/" + ghostID + "/labels", realm: "admin", body: badJSON, want: 400},
		{pattern: "PUT /api/v1/runners/{id}/tier", path: "/api/v1/runners/" + ghostID + "/tier", realm: "admin", body: `{"tier":"nope"}`, want: 422},

		{pattern: "POST /api/v1/gateways", path: "/api/v1/gateways", realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/gateways/{id}", path: "/api/v1/gateways/" + ghostID, realm: "admin", want: 404},
		{pattern: "POST /api/v1/gateways/{id}/adopt", path: "/api/v1/gateways/" + ghostID + "/adopt", realm: "admin", want: 404},
		{pattern: "POST /api/v1/gateways/{id}/rotate", path: "/api/v1/gateways/" + ghostID + "/rotate", realm: "admin", want: 404},
		{pattern: "DELETE /api/v1/gateways/{id}", path: "/api/v1/gateways/" + ghostID, realm: "admin", want: 404},
		{pattern: "PUT /api/v1/gateways/{id}/trusted-proxies", path: "/api/v1/gateways/" + ghostID + "/trusted-proxies",
			realm: "admin", body: `{"trusted_proxies":["not-a-cidr"]}`, want: 422},

		{pattern: "POST /api/v1/routes", path: "/api/v1/routes", realm: "admin", body: badJSON, want: 400},
		{pattern: "DELETE /api/v1/routes/{id}", path: "/api/v1/routes/" + ghostID, realm: "admin", want: 404},
		{pattern: "POST /api/v1/routes/{id}/rotate-token", path: "/api/v1/routes/" + ghostID + "/rotate-token", realm: "admin", want: 404},

		{pattern: "GET /api/v1/connectors/{name}/versions/{version}/references",
			path: "/api/v1/connectors/ghost/versions/9.9.9/references", realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "POST /api/v1/connectors/collect", path: "/api/v1/connectors/collect", realm: "admin",
			mode: modeDeadDB, want: 500},
		{pattern: "POST /api/v1/connectors/{name}/versions/{version}/eol",
			path: "/api/v1/connectors/ghost/versions/9.9.9/eol", realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/connectors/{name}/upgrade", path: "/api/v1/connectors/ghost/upgrade", realm: "admin", want: 404},
		{pattern: "POST /api/v1/connectors/{name}/upgrade/test", path: "/api/v1/connectors/ghost/upgrade/test",
			realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/connector-upgrades/{id}", path: "/api/v1/connector-upgrades/" + ghostID, realm: "admin", want: 404},
		{pattern: "POST /api/v1/connector-upgrades/{id}/publish", path: "/api/v1/connector-upgrades/" + ghostID + "/publish",
			realm: "admin", want: 404},

		{pattern: "PUT /api/v1/flows/{name}", path: "/api/v1/flows/ghost", realm: "admin", body: `{"name":"ghost"}`, want: 422},
		{pattern: "GET /api/v1/flows/{name}", path: "/api/v1/flows/ghost", realm: "admin", want: 404},
		{pattern: "GET /api/v1/flows/{name}/graph", path: "/api/v1/flows/ghost/graph", realm: "admin", want: 404},
		{pattern: "POST /api/v1/flows/review", path: "/api/v1/flows/review", realm: "admin", body: badJSON, want: 422},
		{pattern: "GET /api/v1/flows/{name}/review", path: "/api/v1/flows/ghost/review", realm: "admin", want: 404},
		{pattern: "POST /api/v1/flows/{name}/versions/{version}/publish",
			path: "/api/v1/flows/ghost/versions/1/publish", realm: "admin", want: 404},
		{pattern: "POST /api/v1/flows/{name}/execute", path: "/api/v1/flows/ghost/execute", realm: "admin", body: `{}`, want: 404},
		{pattern: "PUT /api/v1/flows/{name}/schedule", path: "/api/v1/flows/ghost/schedule", realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/flows/{name}/schedule", path: "/api/v1/flows/ghost/schedule", realm: "admin", want: 404},
		{pattern: "DELETE /api/v1/flows/{name}/schedule", path: "/api/v1/flows/ghost/schedule", realm: "admin", want: 404},

		{pattern: "PUT /api/v1/webhooks/{name}", path: "/api/v1/webhooks/ghost", realm: "admin", body: badJSON, want: 400},
		{pattern: "DELETE /api/v1/webhooks/{name}", path: "/api/v1/webhooks/ghost", realm: "admin", want: 404},
		{pattern: "GET /api/v1/tasks/{id}", path: "/api/v1/tasks/" + ghostID, realm: "admin", want: 404},

		{pattern: "PUT /api/v1/secrets/{name}", path: "/api/v1/secrets/ghost", realm: "admin", body: badJSON, want: 400},
		{pattern: "DELETE /api/v1/secrets/{name}", path: "/api/v1/secrets/ghost", realm: "admin", want: 404},
		{pattern: "POST /api/v1/secrets/resolve", path: "/api/v1/secrets/resolve", realm: "runner", body: badJSON, want: 400},

		{pattern: "PUT /api/v1/connections/{name}", path: "/api/v1/connections/ghost", realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/connections/{name}", path: "/api/v1/connections/ghost", realm: "admin", want: 404},
		{pattern: "DELETE /api/v1/connections/{name}", path: "/api/v1/connections/ghost", realm: "admin", want: 404},
		{pattern: "POST /api/v1/connections/resolve", path: "/api/v1/connections/resolve", realm: "runner", body: badJSON, want: 400},
		{pattern: "POST /api/v1/task-config/resolve", path: "/api/v1/task-config/resolve", realm: "runner", body: badJSON, want: 400},

		{pattern: "POST /api/v1/publisher-keys", path: "/api/v1/publisher-keys", realm: "admin", body: badJSON, want: 400},
		{pattern: "DELETE /api/v1/publisher-keys/{id}", path: "/api/v1/publisher-keys/" + ghostID, realm: "admin", want: 404},
		{pattern: "PUT /api/v1/connectors/{name}/versions/{version}", path: "/api/v1/connectors/ghost/versions/9.9.9",
			realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/connectors/{name}/versions", path: "/api/v1/connectors/ghost/versions",
			realm: "admin", mode: modeDeadDB, want: 500},
		{pattern: "POST /api/v1/connectors/{name}/versions/{version}/yank", path: "/api/v1/connectors/ghost/versions/9.9.9/yank",
			realm: "admin", body: badJSON, want: 400},
		{pattern: "GET /api/v1/connectors/{name}/resolve", path: "/api/v1/connectors/ghost/resolve?os=linux&arch=amd64",
			realm: "admin", want: 404},
		{pattern: "GET /api/v1/connectors/{name}/versions/{version}/artifact",
			path: "/api/v1/connectors/ghost/versions/9.9.9/artifact", realm: "admin", want: 404},

		{pattern: "POST /api/v1/runners/register", path: "/api/v1/runners/register", body: badJSON, want: 400},
		{pattern: "POST /api/v1/runners/certificate", path: "/api/v1/runners/certificate", realm: "runner", body: badJSON, want: 400},
		{pattern: "POST /api/v1/lease", path: "/api/v1/lease", realm: "runner", body: badJSON, want: 400},
		{pattern: "POST /api/v1/tasks/{id}/heartbeat", path: "/api/v1/tasks/" + ghostID + "/heartbeat",
			realm: "runner", body: `{}`, want: 409},
		{pattern: "POST /api/v1/tasks/{id}/complete", path: "/api/v1/tasks/" + ghostID + "/complete",
			realm: "runner", body: `{}`, want: 409},
		{pattern: "POST /api/v1/tasks/{id}/fail", path: "/api/v1/tasks/" + ghostID + "/fail",
			realm: "runner", body: `{"error":"x"}`, want: 409},
		{pattern: "POST /api/v1/executions", path: "/api/v1/executions", realm: "runner", body: badJSON, want: 400},
		{pattern: "POST /api/v1/execution-status", path: "/api/v1/execution-status", realm: "runner", body: badJSON, want: 400},
		{pattern: "POST /api/v1/execution-status/{id}/finish", path: "/api/v1/execution-status/" + ghostID + "/finish",
			realm: "runner", body: `{"state":"completed"}`, want: 404},
		{pattern: "GET /api/v1/execution-status/{id}", path: "/api/v1/execution-status/" + ghostID, realm: "runner", want: 404},
	}
}

// TestEveryRegisteredRouteAnswersErrorsInTheDocumentedEnvelope is the sweep.
func TestEveryRegisteredRouteAnswersErrorsInTheDocumentedEnvelope(t *testing.T) {
	registered := registeredRoutes(t)
	cases := sweepCases()
	assertSweepCoversEveryRoute(t, registered, cases)

	healthy, faults, runnerSecret := newSweepServer(t)
	dead, deadRunner := newDeadSweepServer(t)

	for _, tc := range cases {
		t.Run(tc.pattern, func(t *testing.T) {
			srv, secret := healthy, runnerSecret
			if tc.mode == modeDeadDB {
				srv, secret = dead, deadRunner
			}
			if tc.mode == modeHiddenTable {
				faults.hideTable(tc.table)
				defer faults.unhideTable(tc.table)
			}

			token := ""
			switch tc.realm {
			case "admin":
				token = adminToken
			case "runner":
				token = secret
			}
			body, hdr, status := call3t(t, methodOf(tc.pattern), srv.URL+tc.path, token, tc.body)

			switch tc.mode {
			case modeNoError:
				if status >= 400 {
					t.Fatalf("documented as having no reachable error (%s) but answered %d: %s", tc.why, status, body)
				}
				return
			case modeNotJSON:
				// Still swept, for the one property that applies everywhere.
				wantNoInternalLeak(t, tc.pattern, body, adminToken, secret)
				return
			case modeLive, modeDeadDB, modeHiddenTable:
				// Provoked above; the envelope contract applies below.
			}

			if status != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", status, tc.want, body)
			}
			wantSweptEnvelope(t, tc.pattern, body, hdr, status, adminToken, secret)
		})
	}
}

// wantSweptEnvelope is the whole ADR-0023 contract in one place.
func wantSweptEnvelope(t *testing.T, where, body string, hdr http.Header, status int, secrets ...string) {
	t.Helper()
	if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("%s: Content-Type = %q, want application/json", where, ct)
	}
	var env struct {
		Error struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("%s: body is not the ADR-0023 envelope (%v): %s", where, err, body)
	}
	if env.Error.Status != status {
		t.Errorf("%s: envelope status = %d, want the HTTP status %d: %s", where, env.Error.Status, status, body)
	}
	if env.Error.Message == "" {
		t.Errorf("%s: envelope carries no message, so a client has nothing to show: %s", where, body)
	}
	// `code` is optional by ADR-0023 (present only where several conditions
	// share a status) but when present it is a machine token clients branch
	// on — so it must look like one, not like prose.
	if c := env.Error.Code; c != "" && !machineCode.MatchString(c) {
		t.Errorf("%s: error.code = %q is not a stable machine token", where, c)
	}
	wantNoInternalLeak(t, where, body, secrets...)
}

var machineCode = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Every route registered by the mux, read from the registration itself.
//
// The stdlib ServeMux does not enumerate its patterns, so the source IS the
// registry. Reading it with go/ast rather than a regexp means a registration
// that stops being a plain string literal fails loudly here instead of
// silently dropping out of the sweep.
func registeredRoutes(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(sourceDir(t), "api.go"), nil, 0)
	if err != nil {
		t.Fatalf("parsing the router registration: %v", err)
	}
	out := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv, ok := sel.X.(*ast.Ident)
		if !ok || recv.Name != "mux" {
			return true
		}
		if sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			t.Errorf("a route is registered with a non-literal pattern (%s); the sweep cannot see it",
				fset.Position(call.Pos()))
			return true
		}
		pattern, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("route pattern %s: %v", lit.Value, err)
		}
		out[pattern] = true
		return true
	})
	// A parser that silently found nothing would turn this whole file into a
	// no-op, which is the exact failure it exists to prevent.
	if len(out) < 50 {
		t.Fatalf("found only %d registered routes; the enumeration is broken", len(out))
	}
	return out
}

func assertSweepCoversEveryRoute(t *testing.T, registered map[string]bool, cases []sweepCase) {
	t.Helper()
	swept := map[string]bool{}
	for _, tc := range cases {
		if swept[tc.pattern] {
			t.Errorf("route %q has two cases; one of them is testing the wrong thing", tc.pattern)
		}
		swept[tc.pattern] = true
	}
	var missing, extra []string
	for p := range registered {
		if !swept[p] {
			missing = append(missing, p)
		}
	}
	for p := range swept {
		if !registered[p] {
			extra = append(extra, p)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("these routes are registered but not swept — add a case, or a new handler can answer\n"+
			"a bare string and nothing will notice:\n  %s", strings.Join(missing, "\n  "))
	}
	if len(extra) > 0 {
		t.Errorf("these cases name routes that are no longer registered:\n  %s", strings.Join(extra, "\n  "))
	}
}

func methodOf(pattern string) string {
	method, _, ok := strings.Cut(pattern, " ")
	if !ok {
		return http.MethodGet
	}
	return method
}

// sourceDir is the package directory (the test binary's working directory).
func sourceDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return wd
}

// newSweepServer builds a hub with EVERY optional feature configured, so every
// conditionally registered route actually exists. A route that only appears
// with secrets or OIDC enabled must be swept too.
func newSweepServer(t *testing.T) (*httptest.Server, *faultDB, string) {
	t.Helper()
	dsn := pgtest.DSN(t)
	st, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	idp := oidctest.New(t, "shift-hub")
	// Unstarted first: the login flow's redirect URL has to name this very
	// server, which means the listener must exist before the handler does.
	srv := httptest.NewUnstartedServer(nil)
	callback := "http://" + srv.Listener.Addr().String() + "/auth/callback"
	verifier, err := oidcauth.New(t.Context(), oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: "shift-hub"})
	if err != nil {
		t.Fatal(err)
	}
	flow, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config:       oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: "shift-hub"},
		ClientSecret: "hub-secret", RedirectURL: callback,
	})
	if err != nil {
		t.Fatal(err)
	}

	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		OIDC:       verifier,
		OIDCFlow:   flow,
		Secrets:    secrets.New(st, localKEK(t)),
		Gateways:   gwpush.New(gatewayCA(t), nil, 5*time.Second),
		MetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("# no metrics in this test\n"))
		}),
		LeaseTTL:  30 * time.Second,
		LeasePoll: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = h
	srv.Start()
	t.Cleanup(srv.Close)

	return srv, newFaultDB(t, dsn), registerRunner(t, srv.URL, "sweep-runner")
}

// newDeadSweepServer is the same hub with its database taken away after the
// runner credential has been minted, for the reads whose only error arm is a
// store failure.
func newDeadSweepServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		Secrets:    secrets.New(st, localKEK(t)),
		Gateways:   gwpush.New(gatewayCA(t), nil, 5*time.Second),
		LeaseTTL:   30 * time.Second,
		LeasePoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	secret := registerRunner(t, srv.URL, "dead-runner")
	st.Close()
	return srv, secret
}

func localKEK(t *testing.T) kek.Provider {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := kek.NewLocalFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

// TestEveryRegisteredRouteRefusesAnUnauthenticatedCallerWithoutLeaking sweeps
// the same set from the other side.
//
// The 401 is held to the SAME envelope as every other error. It was not, when
// this sweep first ran: the auth middleware answered `{"error":"unauthorized"}`
// — a string where ADR-0023 specifies an object — as `text/plain`. That is the
// response a client meets more often than any other, so it is the last one that
// should need its own parser. Fixed in auth.go; asserted here so it stays fixed.
//
// The MESSAGE remains deliberately uninformative — one opaque failure for every
// path, no oracle. Shape and detail are separate decisions.
func TestEveryRegisteredRouteRefusesAnUnauthenticatedCallerWithoutLeaking(t *testing.T) {
	registered := registeredRoutes(t)
	cases := sweepCases()
	assertSweepCoversEveryRoute(t, registered, cases)

	srv, _, _ := newSweepServer(t)
	for _, tc := range cases {
		if tc.realm == "" {
			continue // deliberately open: probes, the dashboard shell, registration
		}
		t.Run(tc.pattern, func(t *testing.T) {
			body, hdr, status := call3t(t, methodOf(tc.pattern), srv.URL+tc.path, "", tc.body)
			if status != http.StatusUnauthorized {
				t.Fatalf("unauthenticated = %d, want 401 (body %q)", status, body)
			}
			wantSweptEnvelope(t, tc.pattern, body, hdr, status)
			wantNoInternalLeak(t, tc.pattern, body)
		})
	}
}
