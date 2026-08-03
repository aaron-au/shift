package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// The benchmark flows are the calibration workload: they must be ordinary,
// valid flow documents that lower through the same Plan as user work
// (ADR-0008), or the numbers describe a path production never takes. These
// checks are pure document construction — no connector is launched.

// srcConfig decodes a benchmark source's config for assertions.
func srcConfig(t *testing.T, doc *flow.Document) map[string]any {
	t.Helper()
	var cfg map[string]any
	if err := json.Unmarshal(doc.Source.Config, &cfg); err != nil {
		t.Fatalf("source config is not JSON: %v", err)
	}
	return cfg
}

// planStepIDs returns the happy-path step ids the document lowers to.
func planStepIDs(t *testing.T, doc *flow.Document) []string {
	t.Helper()
	if err := doc.Validate(); err != nil {
		t.Fatalf("benchmark flow does not validate: %v", err)
	}
	plan, err := doc.Plan()
	if err != nil {
		t.Fatalf("benchmark flow does not plan: %v", err)
	}
	ids := make([]string, 0, len(plan.Main))
	for _, s := range plan.Main {
		ids = append(ids, s.ID)
	}
	return ids
}

func TestBenchmarkFlowShape(t *testing.T) {
	doc := benchmarkFlow(12345)

	if got := planStepIDs(t, doc); len(got) != 5 {
		t.Fatalf("plan steps = %v, want source + 3 ops + sink", got)
	}
	if doc.Source.Connector != "gen" || doc.Source.Action != "gen" {
		t.Errorf("source = %+v, want the gen connector", doc.Source)
	}
	if got := srcConfig(t, doc)["records"]; got != float64(12345) {
		t.Errorf("source records = %v, want 12345", got)
	}
	// Real work, not a passthrough: filter → flatten → project.
	var types []string
	for _, op := range doc.Ops {
		types = append(types, op.Type)
	}
	if len(types) != 3 || types[0] != "filter" || types[1] != "flatten" || types[2] != "project" {
		t.Fatalf("ops = %v, want [filter flatten project]", types)
	}
	if doc.Ops[1].Sep != "_" {
		t.Errorf("flatten separator = %q, want _", doc.Ops[1].Sep)
	}
	// The project reads a field the flatten produced (address_city), renamed —
	// this is what makes the benchmark exercise the flatten, not just append it.
	fields := doc.Ops[2].Fields
	if len(fields) != 4 {
		t.Fatalf("project fields = %+v, want 4", fields)
	}
	if fields[2].Out != "city" || fields[2].Path != "$.address_city" {
		t.Errorf("project field 2 = %+v, want city ← $.address_city", fields[2])
	}
	if doc.Sink.Connector != "gen" || doc.Sink.Action != "discard" {
		t.Errorf("sink = %+v, want gen/discard (no external target)", doc.Sink)
	}
}

func TestBenchmarkFlowRecordsAreIndependentPerCall(t *testing.T) {
	if a, b := srcConfig(t, benchmarkFlow(10)), srcConfig(t, benchmarkFlow(20)); a["records"] == b["records"] {
		t.Fatalf("record count not honoured: %v vs %v", a["records"], b["records"])
	}
}

func TestTierFlowEncodesRecordsAndGroups(t *testing.T) {
	doc := tierFlow("bench-x", 7, 99, nil)
	if doc.Name != "bench-x" {
		t.Errorf("name = %q", doc.Name)
	}
	cfg := srcConfig(t, doc)
	if cfg["records"] != float64(7) || cfg["groups"] != float64(99) {
		t.Errorf("config = %v, want records=7 groups=99", cfg)
	}
	if len(doc.Ops) != 0 {
		t.Errorf("ops = %+v, want none", doc.Ops)
	}
	if got := planStepIDs(t, doc); len(got) != 2 {
		t.Errorf("plan steps = %v, want source + sink", got)
	}
}

// Every graded tier must be a valid, plannable flow, ordered simplest to
// hardest, each ending at the in-process discard sink so the sweep is
// reproducible on any runner with no external target.
func TestBenchTiersAreValidAndGraded(t *testing.T) {
	wantNames := []string{"simple", "standard", "complex", "extreme"}
	if len(benchTiers) != len(wantNames) {
		t.Fatalf("tiers = %d, want %d", len(benchTiers), len(wantNames))
	}
	// The gradient is transform depth and aggregate cardinality (spill
	// pressure), so group counts must never step backwards down the list.
	prevGroups := 0.0
	for i, tr := range benchTiers {
		t.Run(tr.name, func(t *testing.T) {
			if tr.name != wantNames[i] {
				t.Fatalf("tier[%d] = %q, want %q", i, tr.name, wantNames[i])
			}
			if tr.shape == "" {
				t.Error("tier has no shape label (the numbers would hide their shape)")
			}
			doc := tr.flow(1000)
			ids := planStepIDs(t, doc)
			if len(ids) != len(doc.Ops)+2 {
				t.Fatalf("plan steps = %v for %d ops", ids, len(doc.Ops))
			}
			if doc.Source.Connector != "gen" || doc.Sink.Connector != "gen" || doc.Sink.Action != "discard" {
				t.Errorf("tier endpoints = %+v / %+v, want gen → gen discard", doc.Source, doc.Sink)
			}
			cfg := srcConfig(t, doc)
			if cfg["records"] != float64(1000) {
				t.Errorf("records = %v, want 1000", cfg["records"])
			}
			groups, ok := cfg["groups"].(float64)
			if !ok {
				t.Fatalf("groups = %v, want a number", cfg["groups"])
			}
			if groups < prevGroups {
				t.Errorf("tier %q groups = %v, below the previous tier's %v", tr.name, groups, prevGroups)
			}
			prevGroups = groups
			// The first tier is the passthrough baseline: source → sink.
			if tr.name == "simple" && len(doc.Ops) != 0 {
				t.Errorf("simple tier is not a passthrough: %+v", doc.Ops)
			}
		})
	}
}

// The two hardest tiers must aggregate at a high cardinality — that is what
// puts the spill path (ADR-0003) under test rather than a pure streaming loop.
func TestHardTiersAggregateAtHighCardinality(t *testing.T) {
	for _, name := range []string{"complex", "extreme"} {
		var tr tier
		for _, candidate := range benchTiers {
			if candidate.name == name {
				tr = candidate
			}
		}
		doc := tr.flow(1000)
		groups := srcConfig(t, doc)["groups"]
		if g, ok := groups.(float64); !ok || g < 50_000 {
			t.Errorf("tier %q groups = %v, want high cardinality", name, groups)
		}
		found := false
		for _, op := range doc.Ops {
			if op.Type == "aggregate" {
				found = true
			}
		}
		if !found {
			t.Errorf("tier %q has no aggregate op: %+v", name, doc.Ops)
		}
	}
}

// --- report state -----------------------------------------------------------

func TestBenchSnapshotsReportLatestAndBusy(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	if rep, busy := svc.bench.snapshot(); rep != nil || busy {
		t.Fatalf("fresh snapshot = %+v busy=%v", rep, busy)
	}
	if rep, busy := svc.bench.snapshotTiered(); rep != nil || busy {
		t.Fatalf("fresh tiered snapshot = %+v busy=%v", rep, busy)
	}

	cap0 := &CapacityReport{Records: 7}
	tier0 := &TieredReport{Tiers: []TierResult{{Tier: "simple"}}}
	svc.bench.mu.Lock()
	svc.bench.latest, svc.bench.running = cap0, true
	svc.bench.tieredLatest, svc.bench.tieredRunning = tier0, true
	svc.bench.mu.Unlock()

	if rep, busy := svc.bench.snapshot(); rep != cap0 || !busy {
		t.Errorf("snapshot = %+v busy=%v, want the stored report + busy", rep, busy)
	}
	if rep, busy := svc.bench.snapshotTiered(); rep != tier0 || !busy {
		t.Errorf("tiered snapshot = %+v busy=%v, want the stored report + busy", rep, busy)
	}
	// Status is the dashboard's only view of both.
	st := svc.Status()
	if st.Benchmark != cap0 || !st.BenchBusy || st.Tiered != tier0 || !st.TieredBusy {
		t.Errorf("status = %+v, want both reports surfaced as busy", st)
	}
}

func TestBenchHistoriesAreDefensiveCopies(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	if got := svc.BenchHistory(); len(got) != 0 {
		t.Errorf("fresh BenchHistory = %+v, want empty", got)
	}
	if got := svc.TieredHistory(); len(got) != 0 {
		t.Errorf("fresh TieredHistory = %+v, want empty", got)
	}

	svc.bench.mu.Lock()
	svc.bench.history = []CapacityReport{{Records: 3}, {Records: 2}, {Records: 1}}
	svc.bench.tieredHistory = []TieredReport{{DurationS: 3}, {DurationS: 2}}
	svc.bench.mu.Unlock()

	hist := svc.BenchHistory()
	if len(hist) != 3 || hist[0].Records != 3 || hist[2].Records != 1 {
		t.Fatalf("BenchHistory = %+v, want newest first", hist)
	}
	tiered := svc.TieredHistory()
	if len(tiered) != 2 || tiered[0].DurationS != 3 {
		t.Fatalf("TieredHistory = %+v, want newest first", tiered)
	}

	// A caller mutating its copy must not corrupt the runner's record.
	hist[0].Records = 999
	tiered[0].DurationS = 999
	if svc.BenchHistory()[0].Records != 3 {
		t.Error("BenchHistory handed out its internal slice")
	}
	if svc.TieredHistory()[0].DurationS != 3 {
		t.Error("TieredHistory handed out its internal slice")
	}
}

// --- one benchmark at a time ------------------------------------------------

// A second benchmark must be refused, not queued behind the first: two
// concurrent sweeps would each measure the other's load.
func TestRunBenchmarkRejectsConcurrentRun(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	svc.bench.mu.Lock()
	svc.bench.running = true
	svc.bench.mu.Unlock()

	rep, err := svc.RunBenchmark(1000, 1)
	if err == nil {
		t.Fatal("a second benchmark was accepted while one was running")
	}
	if rep != nil {
		t.Errorf("report returned on rejection: %+v", rep)
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("err = %v, want the already-running reason", err)
	}
	// The rejected call must not clear the flag it never set, nor submit work.
	svc.bench.mu.Lock()
	stillRunning := svc.bench.running
	svc.bench.mu.Unlock()
	if !stillRunning {
		t.Error("rejected benchmark cleared the running flag")
	}
	if got := svc.Status().Totals.Submitted; got != 0 {
		t.Errorf("rejected benchmark submitted %d tasks", got)
	}

	svc.bench.mu.Lock()
	svc.bench.running = false
	svc.bench.mu.Unlock()
}

func TestRunTieredBenchmarkRejectsConcurrentRun(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	svc.bench.mu.Lock()
	svc.bench.tieredRunning = true
	svc.bench.mu.Unlock()

	rep, err := svc.RunTieredBenchmark(1000, 1)
	if err == nil {
		t.Fatal("a second tiered benchmark was accepted while one was running")
	}
	if rep != nil {
		t.Errorf("report returned on rejection: %+v", rep)
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("err = %v, want the already-running reason", err)
	}
	svc.bench.mu.Lock()
	stillRunning := svc.bench.tieredRunning
	svc.bench.mu.Unlock()
	if !stillRunning {
		t.Error("rejected tiered benchmark cleared the running flag")
	}
	if got := svc.Status().Totals.Submitted; got != 0 {
		t.Errorf("rejected tiered benchmark submitted %d tasks", got)
	}

	svc.bench.mu.Lock()
	svc.bench.tieredRunning = false
	svc.bench.mu.Unlock()
}

// --- awaitTask --------------------------------------------------------------

func TestAwaitTaskReturnsCompleted(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	id, err := svc.SubmitWith(hookDoc("await-ok", nil, flowdoc.DiscardSink),
		SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`, `{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	tk, err := svc.awaitTask(id, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted || tk.RecordsIn != 2 {
		t.Fatalf("task = %+v, want completed with 2 records in", tk)
	}
}

func TestAwaitTaskSurfacesFailure(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	// No WebhookBody: the task fails as soon as it is admitted.
	id, err := svc.SubmitWith(hookDoc("await-fail", nil, flowdoc.DiscardSink), SubmitOpts{})
	if err != nil {
		t.Fatal(err)
	}
	tk, err := svc.awaitTask(id, time.Minute)
	if err == nil {
		t.Fatalf("failed task reported as success: %+v", tk)
	}
	if !strings.Contains(err.Error(), "benchmark task failed") {
		t.Errorf("err = %v, want the task-failed reason", err)
	}
}

func TestAwaitTaskUnknownID(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	if _, err := svc.awaitTask("no-such-task", time.Minute); err == nil {
		t.Fatal("unknown task id reported as success")
	} else if !strings.Contains(err.Error(), "evicted") {
		t.Errorf("err = %v, want the evicted reason", err)
	}
}

func TestAwaitTaskTimesOut(t *testing.T) {
	// A budget below one task's cost keeps the task waiting for admission.
	svc := New(Options{ConnectorDir: t.TempDir(), MemBudget: 1})
	t.Cleanup(func() { _ = svc.Close(50 * time.Millisecond) })

	id, err := svc.Submit(hookDoc("await-timeout", nil, flowdoc.DiscardSink), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.awaitTask(id, 10*time.Millisecond); err == nil {
		t.Fatal("a task that never finished reported as success")
	} else if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("err = %v, want the timeout reason", err)
	}
}
