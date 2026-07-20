// Package e2e proves the M3b/M4a exit criterion end to end: kill -9 a
// runner mid-flow and the task completes on another runner, with no
// duplicate completion. Real Postgres, real hub HTTP API, real runnerd
// processes leasing over the wire, real connector subprocesses.
package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

const adminToken = "e2e-admin-token-0123456789" //nolint:gosec // G101: test-only value, not a credential

// slowFlow runs ~10s: 100 batches × 100ms delay. Long enough to kill a
// runner mid-flight, short enough for CI.
const slowFlow = `{"name":"slow",
  "source":{"connector":"gen","action":"gen","config":{"records":100000,"batch_records":1000,"delay_ms":100}},
  "ops":[{"type":"filter","path":"$.active","op":"eq","value":true}],
  "sink":{"connector":"gen","action":"discard"}}`

func TestCrashRecovery(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("needs postgres + real processes")
	}

	// Hub: real store + API over a fresh database, 2s leases so a dead
	// runner is detected quickly.
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
		LeasePoll:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := httptest.NewServer(h)
	t.Cleanup(hub.Close)

	// Build runnerd + the gen connector once into a shared bin dir.
	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-gen", "github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")

	// Deploy the slow flow and enqueue one execution.
	doJSON(t, hub.URL, "PUT", "/api/v1/flows/slow", slowFlow, nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/slow/versions/1/publish", "", nil)
	var acc struct {
		TaskID string `json:"task_id"`
	}
	doJSON(t, hub.URL, "POST", "/api/v1/flows/slow/execute", `{"idempotency_key":"crash-1"}`, &acc)

	// Runner A claims it...
	runnerA := startRunner(t, hub.URL, bin, "victim", "127.0.0.1:18341")
	waitFor(t, 30*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "leased", "task state " + tk.Task.State
	})

	// ...runs it for a moment, then dies without warning.
	time.Sleep(1 * time.Second)
	if err := runnerA.Process.Kill(); err != nil { // SIGKILL: no drain, no goodbye
		t.Fatal(err)
	}
	_ = runnerA.Wait()
	t.Log("runner A killed (SIGKILL) mid-flow")

	// Runner B picks the task up after the lease expires and finishes it.
	startRunner(t, hub.URL, bin, "rescuer", "127.0.0.1:18342")
	waitFor(t, 90*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "completed", "task state " + tk.Task.State
	})

	// The record must show: attempt 1 lease-expired on the victim,
	// attempt 2 completed on the rescuer, exactly one completion, and the
	// full record count.
	tk := getTask(t, hub.URL, acc.TaskID)
	if tk.Task.Attempt != 2 {
		t.Errorf("attempts = %d, want 2", tk.Task.Attempt)
	}
	if len(tk.Attempts) != 2 {
		t.Fatalf("attempt history = %+v", tk.Attempts)
	}
	if tk.Attempts[0].Outcome != "lease-expired" || tk.Attempts[1].Outcome != "completed" {
		t.Errorf("outcomes = %q, %q", tk.Attempts[0].Outcome, tk.Attempts[1].Outcome)
	}
	if tk.Attempts[0].RunnerID == tk.Attempts[1].RunnerID {
		t.Error("both attempts ran on the same runner id")
	}
	var res struct {
		RecordsIn int64 `json:"records_in"`
	}
	if err := json.Unmarshal(tk.Task.Result, &res); err != nil || res.RecordsIn != 100000 {
		t.Errorf("result = %s (err %v), want records_in 100000", tk.Task.Result, err)
	}

	// Idempotent re-execute after completion: same task, still completed.
	var again struct {
		TaskID string `json:"task_id"`
	}
	doJSON(t, hub.URL, "POST", "/api/v1/flows/slow/execute", `{"idempotency_key":"crash-1"}`, &again)
	if again.TaskID != acc.TaskID {
		t.Errorf("idempotency: new task %s created", again.TaskID)
	}
}

// --- harness -----------------------------------------------------------------

func build(t *testing.T, dir, name, pkg string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: builds our own packages
		"-o", filepath.Join(dir, name), pkg)
	cmd.Dir = ".." // any module dir inside the workspace resolves all packages
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
}

// startRunner registers a fresh single-use token and boots a real runnerd
// wired to the hub. The returned process is cleaned up unless already dead.
func startRunner(t *testing.T, hubURL, bin, name, listen string) *exec.Cmd {
	t.Helper()
	var tok struct {
		Token string `json:"token"`
	}
	doJSON(t, hubURL, "POST", "/api/v1/runner-tokens", `{}`, &tok)

	cmd := exec.CommandContext(t.Context(), filepath.Join(bin, "runnerd"), //nolint:gosec // G204: binary we just built
		"-hub", hubURL, "-listen", listen, "-connector-dir", bin, "-name", name)
	cmd.Env = append(os.Environ(), "SHIFT_HUB_REG_TOKEN="+tok.Token)
	cmd.Stdout = testWriter{t, name}
	cmd.Stderr = testWriter{t, name}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil { // still running
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd
}

type testWriter struct {
	t    *testing.T
	name string
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[%s] %s", w.name, strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

type taskView struct {
	Task struct {
		ID      string          `json:"id"`
		State   string          `json:"state"`
		Attempt int             `json:"attempt"`
		Error   string          `json:"error"`
		Result  json.RawMessage `json:"result"`
	} `json:"task"`
	Attempts []struct {
		Attempt  int    `json:"attempt"`
		RunnerID string `json:"runner_id"`
		Outcome  string `json:"outcome"`
	} `json:"attempts"`
}

func getTask(t *testing.T, hubURL, id string) taskView {
	t.Helper()
	var tk taskView
	doJSON(t, hubURL, "GET", "/api/v1/tasks/"+id, "", &tk)
	return tk
}

func doJSON(t *testing.T, base, method, path, body string, out any) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, base+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		t.Fatalf("%s %s = %d: %s", method, path, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for {
		ok, state := cond()
		if ok {
			return
		}
		if state != last {
			t.Logf("waiting: %s", state)
			last = state
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting; last state: %s", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
