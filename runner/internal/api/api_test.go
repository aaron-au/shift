package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/webhook"
	"golang.org/x/crypto/bcrypt"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: builds our own package for the test
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	svc := service.New(service.Options{ConnectorDir: dir})
	t.Cleanup(func() { _ = svc.Close(30 * time.Second) })
	return Handler(svc, "test-runner", "0.0.0", time.Now(), nil, auth.NewGuard(nil), nil, webhook.NewRegistry(), nil, nil)
}

func TestAPISurface(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	h := testHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Dashboard serves.
	resp := do(t, http.MethodGet, srv.URL+"/", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("dashboard: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	// Status shape.
	var status struct {
		Name   string `json:"name"`
		Status struct {
			MaxByMem int64 `json:"max_concurrent_by_mem"`
		} `json:"status"`
	}
	getJSON(t, srv.URL+"/api/status", &status)
	if status.Name != "test-runner" || status.Status.MaxByMem <= 0 {
		t.Fatalf("status = %+v", status)
	}

	// Bad flow → 4xx with error body.
	r := do(t, http.MethodPost, srv.URL+"/api/flows/execute", `{"name":""}`)
	_ = r.Body.Close()
	if r.StatusCode != http.StatusBadRequest && r.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("bad flow status = %d", r.StatusCode)
	}

	// Good flow → accepted, then completes.
	flowDoc := `{"name":"api-test",
	  "source":{"connector":"gen","action":"gen","config":{"records":5000}},
	  "sink":{"connector":"gen","action":"discard"}}`
	var accepted struct {
		TaskID string `json:"task_id"`
	}
	r = do(t, http.MethodPost, srv.URL+"/api/flows/execute", flowDoc)
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("execute status = %d", r.StatusCode)
	}
	if err := json.NewDecoder(r.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()

	deadline := time.Now().Add(time.Minute)
	for {
		var tk struct {
			State     string `json:"state"`
			RecordsIn int64  `json:"records_in"`
		}
		getJSON(t, srv.URL+"/api/tasks/"+accepted.TaskID, &tk)
		if tk.State == "completed" {
			if tk.RecordsIn != 5000 {
				t.Fatalf("records = %d", tk.RecordsIn)
			}
			break
		}
		if tk.State == "failed" || time.Now().After(deadline) {
			t.Fatalf("task state %q", tk.State)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Unknown task → 404.
	r = do(t, http.MethodGet, srv.URL+"/api/tasks/nope", "")
	_ = r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown task = %d", r.StatusCode)
	}

	// Task list contains ours.
	var list struct {
		Tasks []struct{ ID string } `json:"tasks"`
	}
	getJSON(t, srv.URL+"/api/tasks", &list)
	if len(list.Tasks) != 1 || list.Tasks[0].ID != accepted.TaskID {
		t.Fatalf("list = %+v", list)
	}
}

// TestWebhookEndpoints: register a hook, enforce its token, trigger it, and
// see the injected body execute (ADR-0016 direct execution).
func TestWebhookEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	srv := httptest.NewServer(testHandler(t))
	defer srv.Close()

	reg := `{"document":{"name":"hook","source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"gen","action":"discard"}},"token":"s3cret"}`
	if r := do(t, http.MethodPut, srv.URL+"/api/webhooks/ingest", reg); r.StatusCode != 200 {
		_ = r.Body.Close()
		t.Fatalf("register = %d", r.StatusCode)
	}

	var list struct {
		Webhooks []string `json:"webhooks"`
	}
	getJSON(t, srv.URL+"/api/webhooks", &list)
	if len(list.Webhooks) != 1 || list.Webhooks[0] != "ingest" {
		t.Fatalf("webhooks = %+v", list)
	}

	body := `{"n":1}` + "\n" + `{"n":2}` + "\n"

	// Missing/wrong token → 401.
	if r := do(t, http.MethodPost, srv.URL+"/hooks/ingest", body); r.StatusCode != http.StatusUnauthorized {
		_ = r.Body.Close()
		t.Fatalf("no token = %d, want 401", r.StatusCode)
	}

	// Correct token → 202 + task id.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/hooks/ingest", strings.NewReader(body))
	req.Header.Set("X-Webhook-Token", "s3cret")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusAccepted {
		_ = r.Body.Close()
		t.Fatalf("trigger = %d, want 202", r.StatusCode)
	}
	var acc struct {
		TaskID string `json:"task_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&acc)
	_ = r.Body.Close()

	deadline := time.Now().Add(time.Minute)
	for {
		var tk struct {
			State     string `json:"state"`
			RecordsIn int64  `json:"records_in"`
		}
		getJSON(t, srv.URL+"/api/tasks/"+acc.TaskID, &tk)
		if tk.State == "completed" {
			if tk.RecordsIn != 2 {
				t.Fatalf("records in = %d, want 2 (body)", tk.RecordsIn)
			}
			break
		}
		if tk.State == "failed" || time.Now().After(deadline) {
			t.Fatalf("task state %q", tk.State)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestAuthEnforced: with a guard configured, the control surface requires
// credentials and enforces per-endpoint permissions, while health checks and
// hook endpoints stay open.
func TestAuthEnforced(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: our own package
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	svc := service.New(service.Options{ConnectorDir: dir})
	t.Cleanup(func() { _ = svc.Close(30 * time.Second) })

	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	basic, err := auth.NewBasic("viewer:" + string(hash) + ":viewer")
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc, "r", "0", time.Now(), nil, auth.NewGuard(basic), nil, webhook.NewRegistry(), nil, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	req := func(method, path, user, pass string) int {
		r, _ := http.NewRequestWithContext(t.Context(), method, srv.URL+path, nil)
		if user != "" {
			r.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	if c := req("GET", "/healthz", "", ""); c != 200 {
		t.Errorf("healthz open = %d, want 200", c)
	}
	if c := req("GET", "/api/status", "", ""); c != 401 {
		t.Errorf("status no-creds = %d, want 401", c)
	}
	if c := req("GET", "/api/status", "viewer", "pw"); c != 200 {
		t.Errorf("status viewer = %d, want 200", c)
	}
	if c := req("GET", "/api/status", "viewer", "wrong"); c != 401 {
		t.Errorf("status bad-pw = %d, want 401", c)
	}
	// Viewer lacks execute → 403 on a write.
	if c := req("POST", "/api/flows/execute", "viewer", "pw"); c != 403 {
		t.Errorf("viewer execute = %d, want 403", c)
	}
	// Hook endpoints bypass user auth (own token); unknown hook → 404, not 401.
	if c := req("POST", "/hooks/none", "", ""); c != 404 {
		t.Errorf("hook no-creds = %d, want 404 (unguarded by user auth)", c)
	}
}

// TestDirectExecutionReported: a direct (local execute) task fires the
// ExecReporter once it finishes, tagged with the trigger.
func TestDirectExecutionReported(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: our own package
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	svc := service.New(service.Options{ConnectorDir: dir})
	t.Cleanup(func() { _ = svc.Close(30 * time.Second) })

	reported := make(chan struct {
		flow, trigger, state string
	}, 1)
	report := func(tk task.Task, trigger string) {
		reported <- struct{ flow, trigger, state string }{tk.Flow, trigger, string(tk.State)}
	}
	srv := httptest.NewServer(Handler(svc, "r", "0", time.Now(), nil, auth.NewGuard(nil), report, webhook.NewRegistry(), nil, nil))
	defer srv.Close()

	flowDoc := `{"name":"direct-test",
	  "source":{"connector":"gen","action":"gen","config":{"records":100}},
	  "sink":{"connector":"gen","action":"discard"}}`
	r := do(t, http.MethodPost, srv.URL+"/api/flows/execute", flowDoc)
	_ = r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("execute = %d", r.StatusCode)
	}

	select {
	case got := <-reported:
		if got.flow != "direct-test" || got.trigger != "api" || got.state != "completed" {
			t.Fatalf("report = %+v", got)
		}
	case <-time.After(time.Minute):
		t.Fatal("execution not reported")
	}
}

// do issues a context-bound request and fails the test on transport error.
func do(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp := do(t, http.MethodGet, url, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s = %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
}
