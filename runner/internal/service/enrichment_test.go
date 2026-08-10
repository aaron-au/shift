package service

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/task"
)

// TC-029. The enrichment shape: ONE source, teed into a probe branch and a
// build branch, rejoined by a keyed join.
//
//	src → tee → [probe, build] → join → sink
//
// This is the shape ADR-0029 names as the motivating case for `join` — enrich a
// stream against a lookup derived from the same stream — and it is the one
// topology in the corpus that does not merely fail, it HANGS.
//
// Why it hangs. A join is a blocking operator on its build side: it consumes
// the whole build input before emitting anything. Both branches are fed by one
// tee, and both the tee's per-branch queue and the branch pipe are bounded (4
// batches each). So the probe branch backs up, the tee blocks trying to hand it
// batch nine, and the build side it must also feed never receives another
// record. Neither side can advance. Nothing times out, nothing errors, and the
// task holds its admission reservation forever (ADR-0005) — a permanent
// resource leak on the runner.
//
// The generative topology suite (TC-005) cannot hold this: it caps its input at
// 9 records precisely so that the corpus fails rather than hangs. This test
// exists to hold the property directly, and it uses a hard deadline so a
// regression is a FAILURE rather than a hung suite.
func TestTheEnrichmentShapeCompletesAboveTheQueueDepth(t *testing.T) {
	// 12k records is well past the measured hang threshold (between 5 and 12
	// batches of 1024). At 5k the old code completed; at 12k it never did.
	const records = 12000

	svc := newBuiltinService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"probe", "build"}
	// Both branches carry an operator so each ends at a pipe rather than
	// feeding the merge directly — the shape an author actually draws, and the
	// one where the branch pipe adds its own four batches of slack.
	probe := step("probe", "project")
	probe.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}}
	probe.OnSuccess = "j"
	build := step("build", "project")
	build.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}, {Path: "$.v", Out: "v"}}
	build.OnSuccess = "j"
	j := step("j", "merge")
	j.Inputs, j.Mode, j.Build, j.As, j.OnSuccess = []string{"probe", "build"}, "join", "build", "match", "out"
	j.JoinType = "inner"
	j.On = &flowdoc.JoinOn{Left: "$.id", Right: "$.id"}
	out := step("out", "sink")
	out.Connector = "@discard"

	doc := &flowdoc.Document{Name: "enrichment", Steps: []flowdoc.Step{in, tee, probe, build, j, out}}
	if err := doc.Validate(); err != nil {
		t.Fatalf("the shape under test must be a VALID document, else it proves nothing: %v", err)
	}

	var body bytes.Buffer
	for i := range records {
		fmt.Fprintf(&body, `{"id":"k%d","v":%d}`+"\n", i, i)
	}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body.Bytes()})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// A hard deadline, because the failure mode under test is a hang. Without
	// this the regression is "the suite never finishes", which tells a future
	// reader nothing about which property broke.
	deadline := time.Now().Add(60 * time.Second)
	var tk task.Task
	for {
		tk = mustTask(t, svc, id)
		if tk.State == task.StateCompleted || tk.State == task.StateFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the enrichment shape did not terminate within 60s at %d records (state %s): "+
				"the probe branch has backed up behind the join's blocking build side and the tee can no longer feed either. "+
				"The task is still holding its admission reservation (ADR-0005)", records, tk.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
}

// mustTask fetches a task or fails the test.
func mustTask(t *testing.T, svc *Service, id string) task.Task {
	t.Helper()
	tk, ok := svc.Task(id)
	if !ok {
		t.Fatalf("task %s vanished from the ring store", id)
	}
	return tk
}

// awaitTerminalBy waits for a terminal state with a hard deadline, so a
// topology that deadlocks fails this test rather than hanging the package.
func awaitTerminalBy(t *testing.T, svc *Service, id string, within time.Duration) task.Task {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		tk := mustTask(t, svc, id)
		if tk.State == task.StateCompleted || tk.State == task.StateFailed {
			return tk
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not reach a terminal state within %s (state %s)", id, within, tk.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTheEnrichmentShapeStillJoinsCorrectly guards the other half: whatever
// makes the shape terminate must not cost correctness. Every probe record has
// exactly one build match here, so the output count equals the input count and
// every record carries its enrichment.
func TestTheEnrichmentShapeStillJoinsCorrectly(t *testing.T) {
	const records = 12000

	svc := newBuiltinService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"probe", "build"}
	probe := step("probe", "project")
	probe.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}}
	probe.OnSuccess = "j"
	build := step("build", "project")
	build.Fields = []flowdoc.ProjectField{{Path: "$.id", Out: "id"}, {Path: "$.v", Out: "v"}}
	build.OnSuccess = "j"
	j := step("j", "merge")
	j.Inputs, j.Mode, j.Build, j.As, j.OnSuccess = []string{"probe", "build"}, "join", "build", "match", "out"
	j.JoinType = "inner"
	j.On = &flowdoc.JoinOn{Left: "$.id", Right: "$.id"}
	out := step("out", "sink")
	out.Connector = "@response"

	doc := &flowdoc.Document{Name: "enrichment-correct", Steps: []flowdoc.Step{in, tee, probe, build, j, out}}
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}

	var body bytes.Buffer
	for i := range records {
		fmt.Fprintf(&body, `{"id":"k%d","v":%d}`+"\n", i, i)
	}

	// Deliberately NOT RunSync: it waits without a deadline, so a regression of
	// TC-029 would hang the suite instead of failing it. The failure mode under
	// test is a hang; the test must be able to survive it.
	var resp bytes.Buffer
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body.Bytes(), Response: &resp})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminalBy(t, svc, id, 60*time.Second)
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	got := strings.Count(resp.String(), "\n")
	if got != records {
		t.Fatalf("join emitted %d records, want %d — records were lost or duplicated crossing the buffered branch", got, records)
	}
	if !strings.Contains(resp.String(), `"match":{"id":"k0","v":0}`) {
		t.Fatalf("output is missing its enrichment; first 200 bytes:\n%.200s", resp.String())
	}
}
