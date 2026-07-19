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

	"github.com/aaron-au/shift/runner/internal/service"
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
	return Handler(svc, "test-runner", "0.0.0", time.Now())
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
