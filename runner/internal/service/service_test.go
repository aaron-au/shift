package service

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// buildConnectors compiles the gen connector into a temp dir (the go build
// cache makes repeat builds cheap). Resolved via the repo workspace.
func buildConnectors(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: builds our own package for the test
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build gen connector: %v\n%s", err, out)
	}
	return dir
}

func genFlow(records int64) *flow.Document {
	cfg, _ := json.Marshal(map[string]any{"records": records})
	return &flow.Document{
		Name:   "test-flow",
		Source: flow.Endpoint{Connector: "gen", Action: "gen", Config: cfg},
		Ops: []flow.Op{
			{Type: "filter", Path: "$.active", Cmp: "eq", Value: json.RawMessage("true")},
		},
		Sink: flow.Endpoint{Connector: "gen", Action: "discard"},
	}
}

func newTestService(t *testing.T, opts Options) *Service {
	t.Helper()
	if opts.ConnectorDir == "" {
		opts.ConnectorDir = buildConnectors(t)
	}
	svc := New(opts)
	t.Cleanup(func() {
		if err := svc.Close(30 * time.Second); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return svc
}

func TestExecuteFlowEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	id, err := svc.Submit(genFlow(20_000), false)
	if err != nil {
		t.Fatal(err)
	}
	tk, err := svc.awaitTask(id, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if tk.RecordsIn != 20_000 {
		t.Errorf("records in = %d", tk.RecordsIn)
	}
	if tk.RecordsOut <= 0 || tk.RecordsOut >= tk.RecordsIn {
		t.Errorf("filter did not filter: out = %d", tk.RecordsOut)
	}
	if tk.SinkConfirmed != tk.RecordsOut {
		t.Errorf("sink confirmed %d, pipeline out %d", tk.SinkConfirmed, tk.RecordsOut)
	}
	if len(tk.Ops) != 3 { // source, filter, sink
		t.Errorf("ops = %+v", tk.Ops)
	}
	if got := svc.Status(); got.Totals.Completed != 1 || got.Governor.Used != 0 {
		t.Errorf("status after completion: %+v", got)
	}
}

func TestFailedFlowRecordsError(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	doc := genFlow(100)
	doc.Source.Config = json.RawMessage(`{"records":0}`) // gen rejects
	id, err := svc.Submit(doc, false)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		tk, _ := svc.Task(id)
		if tk.State == task.StateFailed {
			if tk.Error == "" {
				t.Error("failed task has empty error")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never failed: %+v", tk)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestConcurrentExecution: with ample budget, tasks run simultaneously
// (goroutine-per-task, ADR-0005).
func TestConcurrentExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{MemBudget: 8 << 30})
	const n = 4
	ids := make([]string, n)
	for i := range n {
		id, err := svc.Submit(genFlow(400_000), false)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	tasks := make([]task.Task, n)
	for i, id := range ids {
		tk, err := svc.awaitTask(id, 2*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = tk
	}
	overlaps := 0
	for i := range n {
		for j := i + 1; j < n; j++ {
			if tasks[i].Started.Before(*tasks[j].Finished) && tasks[j].Started.Before(*tasks[i].Finished) {
				overlaps++
			}
		}
	}
	if overlaps == 0 {
		t.Error("no tasks overlapped; expected concurrent execution")
	}
}

// TestAdmissionSerializesWhenBudgetIsOneTask: capacity-based waiting —
// with budget for exactly one task, two submissions must not overlap.
func TestAdmissionSerializesWhenBudgetIsOneTask(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	opts := Options{TaskWatermark: 32 << 20, TaskOverhead: 16 << 20}
	opts.MemBudget = 48 << 20 // exactly one task's cost
	svc := newTestService(t, opts)

	a, err := svc.Submit(genFlow(400_000), false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Submit(genFlow(400_000), false)
	if err != nil {
		t.Fatal(err)
	}
	ta, err := svc.awaitTask(a, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := svc.awaitTask(b, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ta.Started.Before(*tb.Finished) && tb.Started.Before(*ta.Finished) {
		t.Errorf("tasks overlapped despite single-task budget:\n a: %v..%v\n b: %v..%v",
			ta.Started, ta.Finished, tb.Started, tb.Finished)
	}
}

func TestBenchmarkEstablishesCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{MemBudget: 8 << 30})
	rep, err := svc.RunBenchmark(150_000, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rep.SingleStreamRecS <= 0 || rep.AggregateRecS <= 0 {
		t.Fatalf("empty capacity numbers: %+v", rep)
	}
	if rep.Streams != 2 || len(rep.TaskIDs) != 3 {
		t.Fatalf("streams/tasks wrong: %+v", rep)
	}
	if rep.ScalingEfficiency <= 0 {
		t.Fatalf("scaling efficiency = %v", rep.ScalingEfficiency)
	}
	// Benchmark tasks are visible, flagged tasks.
	benchTasks := 0
	for _, tk := range svc.Tasks(50) {
		if tk.Benchmark && tk.State == task.StateCompleted {
			benchTasks++
		}
	}
	if benchTasks != 3 {
		t.Errorf("visible completed benchmark tasks = %d, want 3", benchTasks)
	}
	if len(svc.BenchHistory()) != 1 {
		t.Error("history not recorded")
	}
	if st := svc.Status(); st.Benchmark == nil || st.BenchBusy {
		t.Errorf("status benchmark: %+v busy=%v", st.Benchmark, st.BenchBusy)
	}
}

func TestSubmitValidatesEagerly(t *testing.T) {
	svc := newTestService(t, Options{ConnectorDir: t.TempDir()}) // no binaries needed
	bad := &flow.Document{Name: "x",
		Source: flow.Endpoint{Connector: "gen", Action: "gen"},
		Ops:    []flow.Op{{Type: "warp-speed"}},
		Sink:   flow.Endpoint{Connector: "gen", Action: "discard"},
	}
	if _, err := svc.Submit(bad, false); err == nil {
		t.Fatal("expected validation error")
	}
	if got := svc.Status().Totals.Submitted; got != 0 {
		t.Fatalf("invalid flow was recorded: %d", got)
	}
}

func TestDrainRejectsNewWork(t *testing.T) {
	svc := New(Options{ConnectorDir: t.TempDir()})
	if err := svc.Close(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Submit(genFlow(10), false); err == nil {
		t.Fatal("expected draining error")
	}
}
