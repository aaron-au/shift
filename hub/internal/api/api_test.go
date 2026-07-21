package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/connpolicy"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

const adminToken = "test-admin-token-0123456789" // test-only value

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		LeaseTTL:   2 * time.Second,
		LeasePoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// call issues a request and decodes the JSON response body (nil out = discard).
func call(t *testing.T, method, url, token, body string, out any) int {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && resp.StatusCode < 300 {
			t.Fatalf("%s %s: decode: %v", method, url, err)
		}
	}
	return resp.StatusCode
}

const goodFlow = `{"name":"orders",
  "source":{"connector":"gen","action":"gen","config":{"records":10}},
  "sink":{"connector":"gen","action":"discard"}}`

// TestFlowGraph: the studio graph endpoint returns nodes + edges for a
// published flow.
func TestFlowGraph(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil); c != 201 {
		t.Fatalf("deploy = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); c != 200 {
		t.Fatalf("publish = %d", c)
	}
	var g struct {
		Main  []string `json:"main"`
		Nodes []struct {
			ID, Role string
		} `json:"nodes"`
		Edges []struct{ Kind string } `json:"edges"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/flows/orders/graph", adminToken, "", &g); c != 200 {
		t.Fatalf("graph = %d", c)
	}
	if len(g.Main) != 2 || g.Main[0] != "source" || g.Main[1] != "sink" {
		t.Fatalf("main = %v, want [source sink]", g.Main)
	}
	if len(g.Edges) != 1 || g.Edges[0].Kind != "complete" {
		t.Fatalf("edges = %+v", g.Edges)
	}
}

// TestConnectorPolicy: a restricted hub rejects a deploy that uses a
// disallowed connector (422) and hides it from resolution (404).
func TestConnectorPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken:      adminToken,
		ConnectorPolicy: connpolicy.Parse("", "gen"), // deny gen
		LeaseTTL:        2 * time.Second,
		LeasePoll:       20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// Deploy of a flow that uses the denied connector is rejected.
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil); code != 422 {
		t.Fatalf("deploy with denied connector = %d, want 422", code)
	}
	// The denied connector is hidden from resolution (as if absent).
	if code := call(t, "GET", srv.URL+"/api/v1/connectors/gen/resolve", adminToken, "", nil); code != 404 {
		t.Fatalf("resolve denied connector = %d, want 404", code)
	}
}

func TestAuthRealms(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// No/wrong token → 401 on admin routes.
	if code := call(t, "GET", srv.URL+"/api/v1/flows", "", "", nil); code != 401 {
		t.Fatalf("no token = %d", code)
	}
	if code := call(t, "GET", srv.URL+"/api/v1/flows", "wrong-token-aaaaaaaa", "", nil); code != 401 {
		t.Fatalf("wrong token = %d", code)
	}
	// Runner routes reject garbage secrets.
	if code := call(t, "POST", srv.URL+"/api/v1/lease", "rs_bogus", `{}`, nil); code != 401 {
		t.Fatalf("bogus runner secret = %d", code)
	}
	// Registration with a bad token is refused.
	if code := call(t, "POST", srv.URL+"/api/v1/runners/register", "", `{"token":"srt_bogus","name":"r"}`, nil); code != 401 {
		t.Fatalf("bogus reg token = %d", code)
	}
	// The dashboard's overview endpoint (real SQL against real Postgres).
	var stats struct {
		Stats struct {
			Tasks map[string]int `json:"tasks"`
		} `json:"stats"`
		Scheduler map[string]any `json:"scheduler"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/stats", adminToken, "", &stats); code != 200 {
		t.Fatalf("stats = %d", code)
	}
	if stats.Stats.Tasks == nil || stats.Scheduler == nil {
		t.Fatalf("stats payload = %+v", stats)
	}
	// Health endpoints are open.
	if code := call(t, "GET", srv.URL+"/healthz", "", "", nil); code != 200 {
		t.Fatalf("healthz = %d", code)
	}
	if code := call(t, "GET", srv.URL+"/readyz", "", "", nil); code != 200 {
		t.Fatalf("readyz = %d", code)
	}

	// Weak admin token is refused at construction.
	if _, err := api.Handler(nil, api.Options{AdminToken: "short"}); err == nil {
		t.Fatal("weak admin token accepted")
	}
}

func TestFlowDeployValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// Invalid document → 422, nothing stored.
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, `{"name":"orders"}`, nil); code != 422 {
		t.Fatalf("invalid doc = %d", code)
	}
	// Name mismatch → 422.
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/other", adminToken, goodFlow, nil); code != 422 {
		t.Fatalf("name mismatch = %d", code)
	}
	// Valid deploy → 201 v1, redeploy → v2.
	var dep struct{ Version int }
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, &dep); code != 201 || dep.Version != 1 {
		t.Fatalf("deploy = %d v%d", code, dep.Version)
	}
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, &dep); code != 201 || dep.Version != 2 {
		t.Fatalf("redeploy = %d v%d", code, dep.Version)
	}
	// Execute against a missing flow → 404.
	if code := call(t, "POST", srv.URL+"/api/v1/flows/ghost/execute", adminToken, `{}`, nil); code != 404 {
		t.Fatalf("execute missing flow = %d", code)
	}
}

func TestLeaseProtocol(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// Register a runner through the real endpoint chain.
	var tok struct{ Token string }
	if code := call(t, "POST", srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); code != 201 || tok.Token == "" {
		t.Fatalf("token = %d %+v", code, tok)
	}
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != 201 || reg.Secret == "" {
		t.Fatalf("register = %d %+v", code, reg)
	}

	// Empty queue → 204 after the long-poll window.
	if code := call(t, "POST", srv.URL+"/api/v1/lease", reg.Secret, `{"wait_seconds":0}`, nil); code != 204 {
		t.Fatalf("empty lease = %d", code)
	}

	// Deploy + publish + execute, then lease it. Executing a draft-only
	// flow by default is a 409.
	call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil)
	if code := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken, `{}`, nil); code != 409 {
		t.Fatalf("execute unpublished = %d, want 409", code)
	}
	// Reserved scheduler namespace is rejected.
	if code := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken,
		`{"idempotency_key":"sched:x:y"}`, nil); code != 422 {
		t.Fatalf("execute with sched: key = %d, want 422", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); code != 200 {
		t.Fatalf("publish = %d", code)
	}
	var acc struct {
		TaskID string `json:"task_id"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken,
		`{"idempotency_key":"k1"}`, &acc); code != 202 {
		t.Fatalf("execute = %d", code)
	}

	var lease struct {
		Task struct {
			ID       string          `json:"id"`
			Attempt  int             `json:"attempt"`
			Document json.RawMessage `json:"document"`
		} `json:"task"`
		LeaseTTLSeconds int `json:"lease_ttl_seconds"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/lease", reg.Secret, `{"wait_seconds":5}`, &lease); code != 200 {
		t.Fatalf("lease = %d", code)
	}
	if lease.Task.ID != acc.TaskID || lease.Task.Attempt != 1 || len(lease.Task.Document) == 0 || lease.LeaseTTLSeconds != 2 {
		t.Fatalf("lease = %+v", lease)
	}

	// Heartbeat holds it; complete finishes it; second complete conflicts.
	if code := call(t, "POST", srv.URL+"/api/v1/tasks/"+acc.TaskID+"/heartbeat", reg.Secret, "", nil); code != 204 {
		t.Fatalf("heartbeat = %d", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/tasks/"+acc.TaskID+"/complete", reg.Secret,
		`{"records_in":10,"records_out":10}`, nil); code != 204 {
		t.Fatalf("complete = %d", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/tasks/"+acc.TaskID+"/complete", reg.Secret, `{}`, nil); code != 409 {
		t.Fatalf("double complete = %d", code)
	}

	// Admin view: task completed with attempt history and result.
	var tk struct {
		Task struct {
			State  string          `json:"state"`
			Result json.RawMessage `json:"result"`
		} `json:"task"`
		Attempts []struct {
			Outcome string `json:"outcome"`
		} `json:"attempts"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/tasks/"+acc.TaskID, adminToken, "", &tk); code != 200 {
		t.Fatalf("get task = %d", code)
	}
	if tk.Task.State != "completed" || len(tk.Attempts) != 1 || tk.Attempts[0].Outcome != "completed" {
		t.Fatalf("task view = %+v", tk)
	}

	// Idempotent re-execute returns the same finished task.
	var again struct {
		TaskID string `json:"task_id"`
	}
	call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken, `{"idempotency_key":"k1"}`, &again)
	if again.TaskID != acc.TaskID {
		t.Fatalf("idempotent execute: %q != %q", again.TaskID, acc.TaskID)
	}
}

// TestUsageEndpoints (M6d) drives a full lease→complete flow through the API,
// then asserts the usage rollup + cursor export reflect it, plus auth and
// bad-parameter handling.
func TestUsageEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// Empty account: well-formed zero report, no auth → 401, bad since → 400.
	var empty struct {
		Totals struct {
			Executions int64 `json:"executions"`
		} `json:"totals"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/usage", adminToken, "", &empty); code != 200 {
		t.Fatalf("usage (empty) = %d", code)
	}
	if empty.Totals.Executions != 0 {
		t.Fatalf("empty usage executions = %d", empty.Totals.Executions)
	}
	if code := call(t, "GET", srv.URL+"/api/v1/usage", "", "", nil); code != 401 {
		t.Fatalf("usage without token = %d, want 401", code)
	}
	if code := call(t, "GET", srv.URL+"/api/v1/usage?since=notatime", adminToken, "", nil); code != 400 {
		t.Fatalf("usage bad since = %d, want 400", code)
	}

	// Register + publish + execute + lease + complete one task with counts.
	var tok struct{ Token string }
	call(t, "POST", srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		Secret string `json:"secret"`
	}
	call(t, "POST", srv.URL+"/api/v1/runners/register", "", `{"token":"`+tok.Token+`","name":"r1"}`, &reg)
	call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil)
	call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil)
	var acc struct {
		TaskID string `json:"task_id"`
	}
	call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken, `{"idempotency_key":"k1"}`, &acc)
	var lease struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/lease", reg.Secret, `{"wait_seconds":5}`, &lease); code != 200 {
		t.Fatalf("lease = %d", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/tasks/"+lease.Task.ID+"/complete", reg.Secret,
		`{"records_in":7,"records_out":7}`, nil); code != 204 {
		t.Fatalf("complete = %d", code)
	}

	// Rollup now reflects the completed task.
	var rep struct {
		Totals struct {
			Executions int64 `json:"executions"`
			Completed  int64 `json:"completed"`
			RecordsIn  int64 `json:"records_in"`
		} `json:"totals"`
		ByFlow []struct {
			FlowName string `json:"flow_name"`
		} `json:"by_flow"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/usage", adminToken, "", &rep); code != 200 {
		t.Fatalf("usage = %d", code)
	}
	if rep.Totals.Executions != 1 || rep.Totals.Completed != 1 || rep.Totals.RecordsIn != 7 {
		t.Fatalf("usage totals = %+v", rep.Totals)
	}
	if len(rep.ByFlow) != 1 || rep.ByFlow[0].FlowName != "orders" {
		t.Fatalf("usage by-flow = %+v", rep.ByFlow)
	}

	// Cursor export returns the metering row; a caught-up page has next=0.
	var exp struct {
		Events []struct {
			ID     int64  `json:"id"`
			Source string `json:"source"`
		} `json:"events"`
		Next int64 `json:"next"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/usage/events", adminToken, "", &exp); code != 200 {
		t.Fatalf("usage events = %d", code)
	}
	if len(exp.Events) != 1 || exp.Events[0].Source != "task" || exp.Next != 0 {
		t.Fatalf("usage events = %+v (next %d)", exp.Events, exp.Next)
	}
}

// TestErrorEnvelope pins the ADR-0023 error envelope: status + message always,
// and a finer machine `code` on the sub-status cases that need it.
func TestErrorEnvelope(t *testing.T) {
	srv := newServer(t)
	type env struct {
		Error struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	// Generic error: status + message, no code (404 is unambiguous here).
	var e1 env
	if c := call(t, "GET", srv.URL+"/api/v1/flows/nope", adminToken, "", &e1); c != 404 {
		t.Fatalf("unknown flow = %d, want 404", c)
	}
	if e1.Error.Status != 404 || e1.Error.Message == "" {
		t.Fatalf("envelope = %+v, want status 404 + message", e1.Error)
	}
	if e1.Error.Code != "" {
		t.Errorf("unambiguous 404 should carry no code, got %q", e1.Error.Code)
	}

	// Finer code: invalid flow document → 422 flow_invalid.
	var e2 env
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/bad", adminToken, `{"name":"bad"}`, &e2); c != 422 {
		t.Fatalf("invalid flow = %d, want 422", c)
	}
	if e2.Error.Code != "flow_invalid" {
		t.Fatalf("invalid flow code = %q, want flow_invalid (msg %q)", e2.Error.Code, e2.Error.Message)
	}
}
