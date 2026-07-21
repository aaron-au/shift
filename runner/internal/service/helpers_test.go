package service

import (
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/runner/internal/task"
)

// genBatch builds a batch of n {id:i} records for feeding a sampler directly.
func genBatch(n int) *record.Batch {
	b := record.NewBatch()
	bld := b.Builder()
	for i := range n {
		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(int64(i))
		bld.EndMap()
		b.Append(bld.Finish())
	}
	return b
}

func TestNewCaptureSamplerDefaults(t *testing.T) {
	// max <= 0 defaults to 20; nil redact defaults to identity. Feeding 25
	// records exercises both: bound to 20, mark More, and (identity) leave the
	// plaintext intact.
	cs := newCaptureSampler(0, nil)
	cs.Sample("s1", genBatch(25))

	res := cs.result()
	if len(res) != 1 || res[0].StepID != "s1" {
		t.Fatalf("result = %+v", res)
	}
	if len(res[0].Records) != 20 {
		t.Fatalf("records = %d, want default max 20", len(res[0].Records))
	}
	if !res[0].More {
		t.Error("expected More (25 > 20)")
	}
	// Identity redact: values pass through unmasked.
	var b strings.Builder
	for _, r := range res[0].Records {
		b.Write(r)
	}
	joined := b.String()
	if strings.Contains(joined, "***") {
		t.Errorf("nil redact should be identity, found mask: %s", joined)
	}
	if !strings.Contains(joined, `"id":0`) {
		t.Errorf("expected plaintext records, got: %s", joined)
	}
}

func TestCaptureSamplerUnderMaxNoMore(t *testing.T) {
	// Fewer records than max: keep them all, do not flag More.
	cs := newCaptureSampler(10, nil)
	cs.Sample("only", genBatch(3))

	res := cs.result()
	if len(res) != 1 {
		t.Fatalf("result = %+v", res)
	}
	if len(res[0].Records) != 3 || res[0].More {
		t.Errorf("records=%d more=%v, want 3/false", len(res[0].Records), res[0].More)
	}
}

func TestCaptureSamplerPreservesStepOrder(t *testing.T) {
	// result() returns steps in first-seen order, not map order.
	cs := newCaptureSampler(5, nil)
	cs.Sample("source", genBatch(1))
	cs.Sample("op0", genBatch(1))
	cs.Sample("sink", genBatch(1))
	cs.Sample("op0", genBatch(1)) // second visit must not reorder

	res := cs.result()
	got := []string{res[0].StepID, res[1].StepID, res[2].StepID}
	want := []string{"source", "op0", "sink"}
	if len(res) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("order = %v, want %v", got, want)
	}
	// op0 accumulated across two visits (1 + 1 = 2 records).
	if len(res[1].Records) != 2 {
		t.Errorf("op0 records = %d, want 2 (accumulated)", len(res[1].Records))
	}
}

func TestCaptureResultEmpty(t *testing.T) {
	if res := newCaptureSampler(5, nil).result(); len(res) != 0 {
		t.Errorf("empty sampler result = %+v, want none", res)
	}
}

// These tests exercise the package's PURE, in-process helpers only. None of
// them spawn a connector subprocess, so they run in every pass — including the
// deterministic coverage pass (SHIFT_COVERAGE=1), where the subprocess tests
// are skipped (see coverage_skip_test.go).

// newIdleService builds a Service with no connector binaries. It is only ever
// used to test in-process helpers (Status/Tasks/Task/Close and direct store
// population); nothing here submits a real flow.
func newIdleService(t *testing.T, opts Options) *Service {
	t.Helper()
	if opts.ConnectorDir == "" {
		opts.ConnectorDir = t.TempDir() // present but empty: never launched
	}
	svc := New(opts)
	t.Cleanup(func() {
		if err := svc.Close(time.Second); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return svc
}

func TestNewRedactorIdentityWhenNoSecrets(t *testing.T) {
	// Zero secrets, and secrets that are all empty strings, both yield the
	// identity function (no masking).
	for _, values := range [][]string{nil, {}, {"", ""}} {
		redact := newRedactor(values)
		const in = "nothing to hide: token=abc123"
		if got := redact(in); got != in {
			t.Errorf("newRedactor(%v): got %q, want identity %q", values, got, in)
		}
	}
}

func TestNewRedactorMasks(t *testing.T) {
	cases := []struct {
		name    string
		secrets []string
		in      string
		want    string
	}{
		{"single", []string{"sk-123"}, "token sk-123 done", "token *** done"},
		{"empty-ignored", []string{"", "sec"}, "a sec b", "a *** b"},
		{"multiple", []string{"aaa", "bbb"}, "aaa mid bbb", "*** mid ***"},
		{"repeated-occurrence", []string{"pw"}, "pw and pw", "*** and ***"},
		// Overlapping: "foo" and "bar" both appear back-to-back and are each
		// masked independently.
		{"adjacent", []string{"foo", "bar"}, "foobar", "******"},
		// Argument-order wins at a position: "ab" is tried before "abc", so
		// "abc" masks its "ab" prefix and leaves the trailing "c" (documents
		// strings.Replacer's leftmost, argument-order, non-overlapping rule).
		{"substring-argorder", []string{"ab", "abc"}, "abc", "***c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := newRedactor(tc.secrets)(tc.in); got != tc.want {
				t.Errorf("redact(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestErrorRecordShape(t *testing.T) {
	b := errorRecord("my-flow", "step-3", "boom: it broke", "2026-07-21T00:00:00Z")

	if b.Len() != 1 {
		t.Fatalf("errorRecord batch len = %d, want 1", b.Len())
	}
	rec := b.Record(0)
	if rec.Len() != 4 {
		t.Fatalf("record has %d fields, want exactly 4 (no payload)", rec.Len())
	}

	want := map[string]string{
		"flow":  "my-flow",
		"step":  "step-3",
		"error": "boom: it broke",
		"at":    "2026-07-21T00:00:00Z",
	}
	// Assert exactly these keys and values — no more, no fewer.
	seen := map[string]bool{}
	for i := 0; i < rec.Len(); i++ {
		key := string(rec.KeyAt(i))
		exp, ok := want[key]
		if !ok {
			t.Fatalf("unexpected field %q in error record", key)
		}
		if seen[key] {
			t.Fatalf("duplicate field %q", key)
		}
		seen[key] = true
		if got := rec.Index(i).String(); got != exp {
			t.Errorf("field %q = %q, want %q", key, got, exp)
		}
	}
	for _, k := range []string{"flow", "step", "error", "at"} {
		if v, ok := rec.Field(k); !ok {
			t.Errorf("missing field %q", k)
		} else if v.String() != want[k] {
			t.Errorf("Field(%q) = %q, want %q", k, v.String(), want[k])
		}
	}
}

func TestStatusFreshService(t *testing.T) {
	// Explicit small budgets make the derived values crisp:
	// taskCost = watermark + overhead = 40; maxByMem = budget/taskCost = 2.
	svc := newIdleService(t, Options{MemBudget: 100, TaskWatermark: 30, TaskOverhead: 10})

	st := svc.Status()
	if st.Governor.Budget != 100 {
		t.Errorf("Governor.Budget = %d, want 100", st.Governor.Budget)
	}
	if st.Governor.Used != 0 || st.Governor.Peak != 0 {
		t.Errorf("fresh governor used=%d peak=%d, want 0/0", st.Governor.Used, st.Governor.Peak)
	}
	if st.TaskCost != 40 {
		t.Errorf("TaskCost = %d, want 40", st.TaskCost)
	}
	if svc.taskCost() != 40 {
		t.Errorf("taskCost() = %d, want 40", svc.taskCost())
	}
	if st.MaxByMem != 2 {
		t.Errorf("MaxByMem = %d, want 2 (100/40)", st.MaxByMem)
	}
	if st.Totals != (task.Totals{}) {
		t.Errorf("fresh Totals = %+v, want zero", st.Totals)
	}
	if len(st.Connectors) != 0 {
		t.Errorf("fresh Connectors = %+v, want empty", st.Connectors)
	}
	if st.Benchmark != nil || st.BenchBusy {
		t.Errorf("fresh benchmark = %+v busy=%v, want nil/false", st.Benchmark, st.BenchBusy)
	}
	if st.Tiered != nil || st.TieredBusy {
		t.Errorf("fresh tiered = %+v busy=%v, want nil/false", st.Tiered, st.TieredBusy)
	}
}

func TestStatusDefaultsDeriveMaxByMem(t *testing.T) {
	// With defaults: budget 1 GiB, taskCost 80 MiB => maxByMem = 12.
	svc := newIdleService(t, Options{})
	st := svc.Status()
	if st.TaskCost != (64<<20)+(16<<20) {
		t.Errorf("default TaskCost = %d", st.TaskCost)
	}
	if want := st.Governor.Budget / st.TaskCost; st.MaxByMem != want {
		t.Errorf("MaxByMem = %d, want %d", st.MaxByMem, want)
	}
}

func TestTasksAndTaskDelegation(t *testing.T) {
	svc := newIdleService(t, Options{})

	// Empty store: no tasks, unknown lookup is not found.
	if got := svc.Tasks(10); len(got) != 0 {
		t.Errorf("Tasks on empty = %+v, want none", got)
	}
	if _, ok := svc.Task("does-not-exist"); ok {
		t.Error("Task(missing) reported found")
	}

	// Populate the store directly (same package) — no flow execution needed.
	t1 := &task.Task{ID: "t1", Flow: "f1", State: task.StateWaiting, Submitted: time.Now()}
	t2 := &task.Task{ID: "t2", Flow: "f2", State: task.StateWaiting, Submitted: time.Now()}
	svc.store.Add(t1)
	svc.store.Add(t2)

	got, ok := svc.Task("t2")
	if !ok || got.Flow != "f2" {
		t.Fatalf("Task(t2) = %+v ok=%v", got, ok)
	}

	// Recent is newest-first.
	recent := svc.Tasks(10)
	if len(recent) != 2 || recent[0].ID != "t2" || recent[1].ID != "t1" {
		t.Fatalf("Tasks order = %+v, want [t2 t1]", recent)
	}

	// Totals delegate through Status: two submitted, both waiting.
	st := svc.Status()
	if st.Totals.Submitted != 2 || st.Totals.Waiting != 2 {
		t.Errorf("Totals = %+v, want submitted=2 waiting=2", st.Totals)
	}
}

func TestCloseIdleServiceIsClean(t *testing.T) {
	// A service that never ran a task closes without error and rejects new
	// work afterwards. (newIdleService's cleanup would also Close; a second
	// Close here confirms it and the drain gate.)
	svc := New(Options{ConnectorDir: t.TempDir()})
	if err := svc.Close(time.Second); err != nil {
		t.Fatalf("Close idle: %v", err)
	}
	if _, err := svc.Submit(genFlow(1), false); err == nil {
		t.Error("Submit after Close should be rejected (draining)")
	}
}
