package leaseloop

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/service"
)

// leaseHub is a scriptable stand-in for the hub control API: it hands out at
// most one task, then long-polls (blocking until the runner's context ends),
// and records every heartbeat / complete / fail it receives.
type leaseHub struct {
	mu sync.Mutex

	task   *hubclient.LeasedTask // handed out once, then nil
	ttlSec int
	empty  int // 204s to return before blocking long-polls

	leaseStatus     int // non-zero: always answer lease with this status
	heartbeatStatus int // default 204
	completeStatus  int // default 204
	failStatus      int // default 200
	secretsStatus   int // default 200
	secrets         map[string]string

	leaseCalls int
	heartbeats int
	completes  []hubclient.Result
	fails      []string
}

func TestMain(m *testing.M) {
	code := m.Run()
	if genBuild.dir != "" {
		_ = os.RemoveAll(genBuild.dir)
	}
	os.Exit(code)
}

func newLeaseHub(t *testing.T) (*leaseHub, *hubclient.Client) {
	t.Helper()
	h := &leaseHub{ttlSec: 30, secrets: map[string]string{}}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", h.handleLease)
	mux.HandleFunc("POST /api/v1/tasks/{id}/heartbeat", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		h.heartbeats++
		st := h.heartbeatStatus
		h.mu.Unlock()
		w.WriteHeader(orDefault(st, http.StatusNoContent))
	})
	mux.HandleFunc("POST /api/v1/tasks/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		var res hubclient.Result
		_ = json.NewDecoder(r.Body).Decode(&res)
		h.mu.Lock()
		h.completes = append(h.completes, res)
		st := h.completeStatus
		h.mu.Unlock()
		w.WriteHeader(orDefault(st, http.StatusNoContent))
	})
	mux.HandleFunc("POST /api/v1/tasks/{id}/fail", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		h.mu.Lock()
		h.fails = append(h.fails, body.Error)
		st := h.failStatus
		h.mu.Unlock()
		w.WriteHeader(orDefault(st, http.StatusOK))
	})
	mux.HandleFunc("POST /api/v1/secrets/resolve", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		st, secrets := h.secretsStatus, h.secrets
		h.mu.Unlock()
		if st != 0 && st != http.StatusOK {
			w.WriteHeader(st)
			_, _ = w.Write([]byte(`{"error":"secret backend down"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": secrets})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return h, hubclient.New(srv.URL, "rs_test")
}

func (h *leaseHub) handleLease(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.leaseCalls++
	switch {
	case h.leaseStatus != 0:
		st := h.leaseStatus
		h.mu.Unlock()
		w.WriteHeader(st)
		_, _ = w.Write([]byte(`{"error":"lease unavailable"}`))
		return
	case h.task != nil:
		tk, ttl := h.task, h.ttlSec
		h.task = nil
		h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"task": tk, "lease_ttl_seconds": ttl})
		return
	case h.empty > 0:
		h.empty--
		h.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.mu.Unlock()
	// Long-poll: hold the request until the runner gives up (bounded so a
	// missed cancellation can't wedge server shutdown).
	select {
	case <-r.Context().Done():
	case <-time.After(15 * time.Second):
	}
	w.WriteHeader(http.StatusNoContent)
}

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func (h *leaseHub) failMsgs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.fails...)
}

func (h *leaseHub) completeCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.completes)
}

func (h *leaseHub) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.leaseCalls
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// newService builds a service with a governor budget the caller controls.
// budget<=0 uses the 1 GiB default (ample headroom); a tiny budget forces
// the capacity gate closed.
func newService(t *testing.T, connectorDir string, budget int64) *service.Service {
	t.Helper()
	svc := service.New(service.Options{ConnectorDir: connectorDir, MemBudget: budget})
	t.Cleanup(func() { _ = svc.Close(5 * time.Second) })
	return svc
}

// runLoop runs a loop with fast timers and returns a cancel + a done channel.
func runLoop(t *testing.T, l *Loop) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("loop did not stop after cancel")
		}
	})
	return cancel, done
}

func newLoop(client *hubclient.Client, svc *service.Service) *Loop {
	return New(Options{
		Client:       client,
		Service:      svc,
		LeaseWait:    time.Second,
		HeadroomPoll: 10 * time.Millisecond,
		TaskPoll:     10 * time.Millisecond,
	})
}

// genBuild caches the compiled gen connector across the connector-spawning
// tests so the (slow) build happens at most once per package run.
var genBuild struct {
	once sync.Once
	dir  string
	err  error
}

// buildGen compiles the gen connector once and returns the dir the service
// can spawn it from.
func buildGen(t *testing.T) string {
	t.Helper()
	genBuild.once.Do(func() {
		dir, err := buildGenConnector()
		genBuild.dir, genBuild.err = dir, err
	})
	if genBuild.err != nil {
		t.Fatalf("build gen connector: %v", genBuild.err)
	}
	return genBuild.dir
}

func buildGenConnector() (string, error) {
	dir, err := os.MkdirTemp("", "leaseloop-gen")
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(context.Background(), "go", "build", //nolint:gosec // G204: builds our own package for the test
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%w\n%s", err, out)
	}
	return dir, nil
}

// TestNewDefaults verifies the zero-value options get sane defaults.
func TestNewDefaults(t *testing.T) {
	l := New(Options{})
	if l.opts.LeaseWait != 20*time.Second || l.opts.HeadroomPoll != 250*time.Millisecond || l.opts.TaskPoll != 100*time.Millisecond {
		t.Fatalf("defaults = %+v", l.opts)
	}
}

// TestCapacityGating: with no memory headroom the loop must never lease, so
// hub work stays claimable by other runners (ADR-0005/0008).
func TestCapacityGating(t *testing.T) {
	hub, client := newLeaseHub(t)
	// A task is waiting, but the governor budget is far below one task's cost.
	hub.task = &hubclient.LeasedTask{ID: "t1", Document: json.RawMessage(`{"name":"x"}`)}
	svc := newService(t, t.TempDir(), 1)

	l := newLoop(client, svc)
	runLoop(t, l)

	// Give the loop time to poll headroom repeatedly.
	time.Sleep(150 * time.Millisecond)
	if hub.calls() != 0 {
		t.Fatalf("leased despite no headroom: %d lease calls", hub.calls())
	}
	if l.Status().TotalLeased != 0 {
		t.Fatalf("TotalLeased = %d, want 0", l.Status().TotalLeased)
	}
}

// TestEmptyLongPoll: a 204 window is not an error and leases nothing.
func TestEmptyLongPoll(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.empty = 2 // two immediate empty windows, then block
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 2*time.Second, func() bool { return hub.calls() >= 1 })
	time.Sleep(50 * time.Millisecond)
	st := l.Status()
	if st.TotalLeased != 0 || st.Errors != 0 {
		t.Fatalf("empty poll status = %+v, want zero leased/errors", st)
	}
}

// TestLeaseErrorBackoff: a 5xx lease response is counted as an error and the
// loop backs off rather than hot-looping.
func TestLeaseErrorBackoff(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.leaseStatus = http.StatusInternalServerError
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 2*time.Second, func() bool { return l.Status().Errors >= 1 })
	// Backoff starts at 1s, so within a short window we expect very few calls,
	// not a hot loop.
	time.Sleep(100 * time.Millisecond)
	if c := hub.calls(); c > 3 {
		t.Fatalf("lease calls = %d, expected backoff to throttle retries", c)
	}
}

// TestInvalidDocumentReportsFail: an unparseable flow document is reported as
// a failure, never silently dropped.
func TestInvalidDocumentReportsFail(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.task = &hubclient.LeasedTask{ID: "bad", Document: json.RawMessage(`{"name":""}`)}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	if msg := hub.failMsgs()[0]; !strings.Contains(msg, "invalid flow document") {
		t.Fatalf("fail msg = %q, want invalid-document", msg)
	}
	if l.Status().TotalLeased != 1 {
		t.Fatalf("TotalLeased = %d, want 1", l.Status().TotalLeased)
	}
}

// TestServiceFailureReportsFail: a valid document whose connector is missing
// fails inside the service and is reported (exercises the poll→"failed"→Fail
// path without a connector subprocess).
func TestServiceFailureReportsFail(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.task = &hubclient.LeasedTask{
		ID:       "missing-conn",
		Document: json.RawMessage(`{"name":"x","source":{"connector":"nope","action":"gen"},"sink":{"connector":"nope2","action":"discard"}}`),
	}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 5*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	if msg := hub.failMsgs()[0]; !strings.Contains(msg, "not installed") {
		t.Fatalf("fail msg = %q, want connector-not-installed", msg)
	}
}

// TestSecretResolutionFailure: when the hub cannot resolve a referenced
// secret, the task fails with a name-only message (no plaintext) before it
// ever reaches the service.
func TestSecretResolutionFailure(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.secretsStatus = http.StatusInternalServerError
	hub.task = &hubclient.LeasedTask{
		ID:       "secret-task",
		Document: json.RawMessage(`{"name":"s","source":{"connector":"http","action":"get","config":{"token":{"$secret":"api_key"}}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	if msg := hub.failMsgs()[0]; !strings.Contains(msg, "secret resolution") {
		t.Fatalf("fail msg = %q, want secret-resolution", msg)
	}
}

// TestSecretResolutionSuccess: referenced secrets are fetched and
// substituted; the task then proceeds (and here fails only because the
// connector is absent). Exercises the resolveSecrets success path.
func TestSecretResolutionSuccess(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.secrets = map[string]string{"api_key": "s3cr3t-value"}
	hub.task = &hubclient.LeasedTask{
		ID:       "secret-ok",
		Document: json.RawMessage(`{"name":"s","source":{"connector":"http","action":"get","config":{"token":{"$secret":"api_key"}}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	// Failure is the missing connector, not a secret error — resolution
	// succeeded. The plaintext secret must never appear in the report.
	msg := hub.failMsgs()[0]
	if !strings.Contains(msg, "not installed") {
		t.Fatalf("fail msg = %q, want connector-not-installed after secret resolve", msg)
	}
	if strings.Contains(msg, "s3cr3t-value") {
		t.Fatal("plaintext secret leaked into failure report")
	}
}

// TestSecretMissingFromHub: the hub answers the resolve call but omits a
// requested secret; the task fails with a name-only resolution error.
func TestSecretMissingFromHub(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.secrets = map[string]string{} // resolve succeeds but returns nothing
	hub.task = &hubclient.LeasedTask{
		ID:       "secret-gap",
		Document: json.RawMessage(`{"name":"s","source":{"connector":"http","action":"get","config":{"token":{"$secret":"api_key"}}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	msg := hub.failMsgs()[0]
	if !strings.Contains(msg, "secret resolution") || !strings.Contains(msg, "not returned by hub") {
		t.Fatalf("fail msg = %q, want secret-not-returned", msg)
	}
}

// TestHeartbeatDuringRun: a task that outlives the heartbeat interval makes
// the loop extend its lease at least once while it runs.
func TestHeartbeatDuringRun(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	hub, client := newLeaseHub(t)
	hub.ttlSec = 1 // heartbeat every ~500ms
	// A deliberately slow source (one record per 100ms batch, ~1.5s total)
	// outlives the heartbeat interval without moving much data.
	hub.task = &hubclient.LeasedTask{
		ID:       "long-run",
		Document: json.RawMessage(`{"name":"leased","source":{"connector":"gen","action":"gen","config":{"records":15,"batch_records":1,"delay_ms":100}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, buildGen(t), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 30*time.Second, func() bool { return hub.completeCount() == 1 })
	hub.mu.Lock()
	hbs := hub.heartbeats
	hub.mu.Unlock()
	if hbs < 1 {
		t.Fatalf("heartbeats = %d, want at least 1 during a long run", hbs)
	}
}

// TestReportLeaseLostNoRetry: a 409 on the terminal report means the hub
// re-dispatched the task; the loop must abandon reporting after one attempt,
// not retry five times.
func TestReportLeaseLostNoRetry(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.failStatus = http.StatusConflict
	hub.task = &hubclient.LeasedTask{ID: "lost", Document: json.RawMessage(`{"name":""}`)}
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) >= 1 })
	// Give any (incorrect) retries a chance to land.
	time.Sleep(200 * time.Millisecond)
	if n := len(hub.failMsgs()); n != 1 {
		t.Fatalf("fail attempts = %d, want exactly 1 (lease lost is final)", n)
	}
}

// TestHappyPathCompletes: a real gen→discard task runs to completion and the
// terminal Complete report carries the record counts.
func TestHappyPathCompletes(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	hub, client := newLeaseHub(t)
	hub.task = &hubclient.LeasedTask{
		ID:             "run-1",
		IdempotencyKey: "idem-1",
		Document:       json.RawMessage(`{"name":"leased","source":{"connector":"gen","action":"gen","config":{"records":1000}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, buildGen(t), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 30*time.Second, func() bool { return hub.completeCount() == 1 })
	hub.mu.Lock()
	res := hub.completes[0]
	hub.mu.Unlock()
	if res.RecordsIn != 1000 {
		t.Fatalf("records in = %d, want 1000", res.RecordsIn)
	}
	if res.RunnerTaskID == "" {
		t.Fatal("runner task id missing from report")
	}
	if len(hub.failMsgs()) != 0 {
		t.Fatalf("unexpected failures: %v", hub.failMsgs())
	}
	if l.Status().TotalLeased != 1 {
		t.Fatalf("TotalLeased = %d, want 1", l.Status().TotalLeased)
	}
}

// TestCompleteLeaseLost: a 409 on Complete (the hub re-dispatched mid-run) is
// final — the loop reports once and stops, relying on idempotency keys to
// keep the duplicate harmless.
func TestCompleteLeaseLost(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	hub, client := newLeaseHub(t)
	hub.completeStatus = http.StatusConflict
	hub.task = &hubclient.LeasedTask{
		ID:       "run-2",
		Document: json.RawMessage(`{"name":"leased","source":{"connector":"gen","action":"gen","config":{"records":100}},"sink":{"connector":"gen","action":"discard"}}`),
	}
	svc := newService(t, buildGen(t), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 30*time.Second, func() bool { return hub.completeCount() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := hub.completeCount(); n != 1 {
		t.Fatalf("complete attempts = %d, want exactly 1 (lease lost is final)", n)
	}
}
