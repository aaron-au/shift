package service

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// Regressions for the DAG compiler's edge-key convention and its build order.
//
// All four shapes below are documents pkg/flowdoc validates and the studio can
// draw, which the runner then refused (or silently mis-executed) — ADR-0029's
// "any validated topology executes" was false for each of them. They were found
// by the generator in topology_gen_test.go, and they are pinned here as named
// shapes because a generated corpus tells you THAT something broke, not what.
//
// The common cause is that `edgeIn` — the map carrying a fan-out branch's
// output into a downstream merge or fan-out — was written under one key and
// read under two others. dag.go now states the convention where the key is
// made; these tests are what stops it drifting again.

// dagBody is n records with a distinct id each, as ndjson for @webhook.
func dagBody(n int) []byte {
	var out []byte
	for i := range n {
		out = append(out, fmt.Sprintf("{\"id\":%d}\n", i)...)
	}
	return out
}

// runDAG submits one document and returns its terminal task, failing the test
// on anything but completion. The shapes here are all about whether the flow
// RUNS, so a failure message that carries the executor's own error is the
// useful one.
func runDAG(t *testing.T, svc *Service, doc *flow.Document, o SubmitOpts) task.Task {
	t.Helper()
	if err := doc.Validate(); err != nil {
		t.Fatalf("%s: the shape under test must be a VALID document, else it proves nothing: %v", doc.Name, err)
	}
	id, err := svc.SubmitWith(doc, o)
	if err != nil {
		t.Fatalf("%s: submit: %v", doc.Name, err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != task.StateCompleted {
		t.Fatalf("%s: state = %s: %s", doc.Name, tk.State, tk.Error)
	}
	return tk
}

// TC-027. A fan-out branch that reaches a merge with NO operator between them:
// the fan-out is then the merge's own producer, and the branch pipe has to be
// keyed by the fan-out. It used to be keyed by the merge's id on both sides of
// the arrow, which nothing looks up, so every such flow died with
// `fan-out %q has no branch feeding %q`.
//
// Tee and router are both covered because they register their branches through
// the same code and a fix that only reached one would be a half fix.
func TestAFanOutBranchMayReachAMergeWithNoOperatorBetween(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	for _, fanOut := range []string{"tee", "router"} {
		t.Run(fanOut, func(t *testing.T) {
			in := step("in", "source")
			in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "f"
			f := step("f", fanOut)
			if fanOut == "tee" {
				f.Branches = []string{"a", "m"}
			} else {
				// A router reaches every record through some arm, so the counts
				// stay the tee's: one arm takes the even ids, the default the
				// rest, and both ends up at the merge.
				f.Routes = []flow.Route{{Path: "$.id", Cmp: "exists", To: "a"}}
				f.Default = "m"
			}
			// One leg carries an operator; the other is the empty branch under
			// test, straight from the fan-out into the merge.
			a := step("a", "filter")
			a.Path, a.Cmp, a.OnSuccess = "$.id", "exists", "m"
			m := step("m", "merge")
			m.Inputs, m.Mode, m.OnSuccess = []string{"a", "f"}, "concat", "out"
			out := step("out", "sink")
			out.Connector = "@discard"

			doc := &flow.Document{Name: "empty-branch-" + fanOut, Steps: []flow.Step{in, f, a, m, out}}
			tk := runDAG(t, svc, doc, SubmitOpts{WebhookBody: dagBody(3)})

			// A tee duplicates (3 down each leg = 6); a router partitions, and
			// every record matches the first route, so the empty leg gets none.
			want := int64(6)
			if fanOut == "router" {
				want = 3
			}
			if tk.SinkConfirmed != want {
				t.Fatalf("sink confirmed %d, want %d", tk.SinkConfirmed, want)
			}
		})
	}
}

// TC-028. A fan-out downstream of a merge that is itself fed by a fan-out —
// ADR-0029's enrichment shape with a second branch after it.
//
// This one was NON-DETERMINISTIC: compile() iterated plan.Nodes, a Go map, so
// whether the upstream tee had registered its branch pipes before the
// downstream tee went looking for them was a coin flip, measured at 9 failures
// in 20 runs of this exact document. One execution therefore proves nothing;
// the loop is the test. At a per-run failure probability anywhere near a half,
// twenty clean runs is a ~1-in-a-million accident.
func TestAFanOutBelowAFanOutFedMergeCompilesRegardlessOfMapOrder(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	// Rebuilt per iteration: submitting the same *Document twice would share
	// step slices, and the point here is a fresh compilation each time.
	build := func() *flow.Document {
		in := step("in", "source")
		in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t1"
		t1 := step("t1", "tee")
		t1.Branches = []string{"a", "b"}
		a := step("a", "filter")
		a.Path, a.Cmp, a.OnSuccess = "$.id", "exists", "m"
		b := step("b", "filter")
		b.Path, b.Cmp, b.OnSuccess = "$.id", "exists", "m"
		m := step("m", "merge")
		m.Inputs, m.Mode, m.OnSuccess = []string{"a", "b"}, "concat", "t2"
		t2 := step("t2", "tee")
		t2.Branches = []string{"o1", "o2"}
		o1 := step("o1", "sink")
		o1.Connector = "@discard"
		o2 := step("o2", "sink")
		o2.Connector = "@discard"
		return &flow.Document{Name: "fanout-below-merge", Steps: []flow.Step{in, t1, a, b, m, t2, o1, o2}}
	}

	const iterations = 20
	for i := range iterations {
		// 3 records → teed to 2 legs → concatenated to 6 → teed to 2 sinks = 12.
		if tk := runDAG(t, svc, build(), SubmitOpts{WebhookBody: dagBody(3)}); tk.SinkConfirmed != 12 {
			t.Fatalf("run %d: sink confirmed %d, want 12", i, tk.SinkConfirmed)
		}
	}
}

// A nested fan-out with more than one operator between the two: the branch
// pipe ends at the LAST of them, and the walk back from the inner fan-out used
// to keep going to the FIRST before looking the pipe up, so it asked about an
// edge that was never registered.
//
// One operator is the case that accidentally worked (first and last are the
// same step), which is why this uses two.
func TestANestedFanOutSeesOperatorsBetweenItAndItsParent(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t1"
	t1 := step("t1", "tee")
	t1.Branches = []string{"s0", "f1"}
	s0 := step("s0", "sink")
	s0.Connector = "@discard"
	f1 := step("f1", "filter")
	f1.Path, f1.Cmp, f1.OnSuccess = "$.id", "exists", "f2"
	f2 := step("f2", "filter")
	f2.Path, f2.Cmp, f2.OnSuccess = "$.id", "exists", "t2"
	t2 := step("t2", "tee")
	t2.Branches = []string{"o1", "o2"}
	o1 := step("o1", "sink")
	o1.Connector = "@discard"
	o2 := step("o2", "sink")
	o2.Connector = "@discard"

	doc := &flow.Document{Name: "nested-two-ops", Steps: []flow.Step{in, t1, s0, f1, f2, t2, o1, o2}}
	// 3 records reach 3 sinks: s0 direct, o1 and o2 through the inner tee.
	if tk := runDAG(t, svc, doc, SubmitOpts{WebhookBody: dagBody(3)}); tk.SinkConfirmed != 9 {
		t.Fatalf("sink confirmed %d, want 9", tk.SinkConfirmed)
	}
}

// The same mismatch had a quieter form: with exactly one operator on the
// branch, the key the walk-back guessed HAPPENED to match, and the operator
// was then applied a second time on top of a pipe that already carried it.
// Nothing errored — the records were simply wrong.
//
// A rename is the assertion because it is the opposite of idempotent: the
// second application looks for `$.id`, which the first one already renamed
// away, and every record comes out null. Filters and pass-through projections
// hide this, which is how it survived.
func TestABranchOperatorIsAppliedOnceIntoANestedFanOut(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t1"
	t1 := step("t1", "tee")
	t1.Branches = []string{"s0", "p1"}
	s0 := step("s0", "sink")
	s0.Connector = "@discard"
	p1 := step("p1", "project")
	p1.Fields = []flow.ProjectField{{Path: "$.id", Out: "nid"}}
	p1.OnSuccess = "t2"
	t2 := step("t2", "tee")
	t2.Branches = []string{"o1", "o2"}
	o1 := step("o1", "sink")
	o1.Connector = "@response"
	o2 := step("o2", "sink")
	o2.Connector = "@discard"

	var resp bytes.Buffer
	doc := &flow.Document{Name: "nested-one-op", Steps: []flow.Step{in, t1, s0, p1, t2, o1, o2}}
	runDAG(t, svc, doc, SubmitOpts{WebhookBody: dagBody(2), Response: &resp})

	want := `{"nid":0}` + "\n" + `{"nid":1}` + "\n"
	if resp.String() != want {
		t.Fatalf("branch output = %q, want %q (a null id means the project ran twice)", resp.String(), want)
	}
}
