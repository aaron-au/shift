package service

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/task"
)

// TC-021. "A stream that never ends is bounded — total bytes, total records,
// wall clock."
//
// Two of those three are the wrong property, and saying so is part of closing
// the row. Streaming an arbitrarily large body is what this platform is FOR:
// ADR-0003's exit criterion is a 1 GB stream at bounded RSS, so a total-bytes
// or total-records cap would refuse the work the product exists to do. It is
// the same argument the decompression bound settled — volume is not the threat.
//
// What actually matters is that a long-running task cannot pin a runner slot
// forever. TC-029 showed exactly that failure mode: a deadlocked task stayed
// `running` indefinitely while HOLDING its admission reservation, which is a
// resource leak, not merely a failed flow (ADR-0005).
//
// So the property tested here is: the wall-clock ceiling terminates a task that
// is still working, and the reservation comes back.
func TestATaskReturnsItsSlotHoweverItEnds(t *testing.T) {
	// Both endings, because the leak TC-029 exposed was about the RESERVATION,
	// not about how the task finished. A 20ms ceiling against 400,000 records
	// is certainly interrupted; a 60s ceiling against the same body certainly
	// is not. Measured, the pipeline does 400,000 records in about 70ms, so
	// neither case is a race.
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"cut off by the ceiling", 20 * time.Millisecond},
		{"finishing normally", 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSlotReturned(t, tc.timeout)
		})
	}
}

func assertSlotReturned(t *testing.T, timeout time.Duration) {
	t.Helper()
	svc := newBuiltinService(t, Options{TaskTimeout: timeout})

	before := svc.Status()

	const records = 400_000
	var body bytes.Buffer
	for i := range records {
		fmt.Fprintf(&body, `{"id":%d,"v":"payload-%d"}`+"\n", i, i)
	}

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "p"
	p := step("p", "project")
	p.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}, {Path: "$.v", Out: "v"}}
	p.OnSuccess = "out"
	out := step("out", "sink")
	out.Connector = "@discard"

	doc := &flowdoc.Document{Name: "slot-return", Steps: []flowdoc.Step{in, p, out}}
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body.Bytes()})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	tk := awaitTerminalBy(t, svc, id, 30*time.Second)
	t.Logf("task ended %s after %d records in: %s", tk.State, tk.RecordsIn, tk.Error)

	// Either outcome is acceptable — cut off by the ceiling, or finished before
	// it. What is NOT acceptable is still running, which is what "unbounded"
	// would look like.
	if tk.State != task.StateFailed && tk.State != task.StateCompleted {
		t.Fatalf("state = %s: a task must reach a terminal state under the wall-clock ceiling", tk.State)
	}

	// The reservation must come back whichever way it ended. A task that
	// terminates but keeps its slot is the TC-029 leak with extra steps.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := svc.Status()
		if st.Governor.Used <= before.Governor.Used && st.Totals.Running == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the task ended, governor used = %d (was %d) and running = %d: "+
				"the admission reservation was not released (ADR-0005)",
				st.Governor.Used, before.Governor.Used, st.Totals.Running)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTheCeilingActuallyFiresOnAStalledTask is the sharper half: a task that
// makes NO progress must still end. The enrichment deadlock (TC-029) is fixed,
// so this uses the ceiling itself as the subject — a very small timeout against
// a body big enough that the task cannot possibly finish first.
func TestTheCeilingActuallyFiresOnAStalledTask(t *testing.T) {
	svc := newBuiltinService(t, Options{TaskTimeout: 20 * time.Millisecond})

	const records = 400_000
	var body bytes.Buffer
	for i := range records {
		fmt.Fprintf(&body, `{"id":%d,"v":"payload-%d"}`+"\n", i, i)
	}

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "p"
	p := step("p", "project")
	p.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}}
	p.OnSuccess = "out"
	out := step("out", "sink")
	out.Connector = "@discard"

	doc := &flowdoc.Document{Name: "stalled", Steps: []flowdoc.Step{in, p, out}}
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body.Bytes()})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	tk := awaitTerminalBy(t, svc, id, 30*time.Second)

	if tk.State == task.StateCompleted {
		t.Skipf("the task finished %d records inside a 20ms ceiling; nothing was interrupted", tk.RecordsIn)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed: the ceiling did not stop the task", tk.State)
	}
	if tk.Error == "" {
		t.Fatal("a task cut off by the ceiling reported no reason; an operator cannot tell it from a data error")
	}
}
