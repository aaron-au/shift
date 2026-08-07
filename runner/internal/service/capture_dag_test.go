package service

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// capturedBy indexes a task's capture by step id.
func capturedBy(t *testing.T, tk task.Task) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, c := range tk.Captured {
		lines := make([]string, 0, len(c.Records))
		for _, r := range c.Records {
			lines = append(lines, string(r))
		}
		out[c.StepID] = lines
	}
	return out
}

// Capture on a fan-out flow (#60). It used to come back EMPTY for anything
// using tee/router/merge, because executeMulti discarded the sampler — and an
// empty capture reads as "no records flowed", not "nobody was watching".
//
// Fan-out is where capture earns the most: "which branch did this record take"
// is unanswerable from the upstream sample alone.
func TestCaptureOnAFanOutFlow(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "r"
	rt := step("r", "router")
	rt.Routes = []flow.Route{{Path: "$.keep", Cmp: "eq", Value: []byte("true"), To: "yes"}}
	rt.Default = "no"
	yes := step("yes", "sink")
	yes.Connector = "@discard"
	no := step("no", "sink")
	no.Connector = "@discard"
	doc := &flow.Document{Name: "capture-router", Steps: []flow.Step{in, rt, yes, no}}

	body := []byte(`{"keep":true,"id":1}` + "\n" + `{"keep":false,"id":2}` + "\n" + `{"keep":true,"id":3}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body, Capture: true, CaptureMax: 10})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if len(tk.Captured) == 0 {
		t.Fatal("capture is empty on a DAG flow; it was silently dropped (#60)")
	}

	by := capturedBy(t, tk)
	if len(by["in"]) != 3 {
		t.Fatalf("upstream capture = %d records, want 3: %v", len(by["in"]), by["in"])
	}
	// The point of the fix: each BRANCH reports separately, so the capture
	// answers which way each record went.
	if len(by["yes"]) != 2 {
		t.Fatalf("matched branch captured %d, want 2: %v", len(by["yes"]), by["yes"])
	}
	if len(by["no"]) != 1 {
		t.Fatalf("default branch captured %d, want 1: %v", len(by["no"]), by["no"])
	}
	if !strings.Contains(strings.Join(by["no"], ""), `"id":2`) {
		t.Fatalf("the record that took the default branch is not in its capture: %v", by["no"])
	}
}

// Capture on a fan-IN flow: every input samples, and so does the downstream
// after the merge. "Did the join match?" is the question, and it needs both
// sides plus the result.
func TestCaptureOnAFanInFlow(t *testing.T) {
	svc := newTestService(t, Options{})

	left := step("left", "source")
	left.Connector, left.Action, left.OnSuccess = "@webhook", "ndjson", "m"
	right := step("right", "source")
	right.Connector, right.Action, right.OnSuccess = "@webhook", "ndjson", "m"
	mg := step("m", "merge")
	mg.Mode = "concat"
	mg.Inputs = []string{"left", "right"}
	mg.OnSuccess = "out"
	out := step("out", "sink")
	out.Connector = "@discard"
	doc := &flow.Document{Name: "capture-merge", Steps: []flow.Step{left, right, mg, out}}

	body := []byte(`{"n":1}` + "\n" + `{"n":2}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body, Capture: true, CaptureMax: 10})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	by := capturedBy(t, tk)
	for _, want := range []string{"left", "right", "m"} {
		if len(by[want]) == 0 {
			t.Fatalf("no capture for %q on a fan-in flow: %v", want, by)
		}
	}
	// The merge stage sees both inputs' records.
	if len(by["m"]) != 4 {
		t.Fatalf("merge captured %d records, want 4 (2 per input): %v", len(by["m"]), by["m"])
	}
}

// Capture stays OFF unless asked for. It is payload, bounded and runner-only
// (ADR-0014), and a DAG flow must not start collecting it just because it has
// branches.
func TestCaptureStaysOffOnDagFlowsUnlessRequested(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"a", "b"}
	a := step("a", "sink")
	a.Connector = "@discard"
	b := step("b", "sink")
	b.Connector = "@discard"
	doc := &flow.Document{Name: "no-capture", Steps: []flow.Step{in, tee, a, b}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"n":1}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if len(tk.Captured) != 0 {
		t.Fatalf("captured %d steps without being asked: %+v", len(tk.Captured), tk.Captured)
	}
}

// The bound holds per step across branches: capture is a sample, and a DAG
// must not multiply it by the number of paths.
func TestCaptureBoundHoldsPerBranch(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"a", "b"}
	a := step("a", "sink")
	a.Connector = "@discard"
	b := step("b", "sink")
	b.Connector = "@discard"
	doc := &flow.Document{Name: "bounded", Steps: []flow.Step{in, tee, a, b}}

	var body strings.Builder
	for i := range 50 {
		body.WriteString(`{"n":`)
		body.WriteString(strconv.Itoa(i))
		body.WriteString("}\n")
	}
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(body.String()), Capture: true, CaptureMax: 5})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	for _, c := range tk.Captured {
		if len(c.Records) > 5 {
			t.Fatalf("step %q captured %d records, over the bound of 5", c.StepID, len(c.Records))
		}
		if !c.More {
			t.Fatalf("step %q captured %d of 50 records but does not report More", c.StepID, len(c.Records))
		}
	}
}
