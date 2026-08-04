package service

import (
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
)

// A linear flow terminating at @stop ends as a SUCCESS with the marker set.
// The task state is the load-bearing assertion: if a stop were recorded as
// failed, the hub would re-dispatch a flow the author deliberately ended.
func TestStopSinkCompletesRatherThanFails(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "halt"
	halt := step("halt", "sink")
	halt.Connector = "@stop"
	doc := &flow.Document{Name: "stopper", Steps: []flow.Step{in, halt}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"n":1}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s (%s), want completed — a stop is a success", tk.State, tk.Error)
	}
	if !tk.Stopped {
		t.Fatal("Stopped marker not set; a stop is indistinguishable from a clean run")
	}
	if tk.StopStep != "halt" {
		t.Fatalf("StopStep = %q, want the step that stopped", tk.StopStep)
	}
	if tk.Error != "" {
		t.Fatalf("Error = %q, want empty on a deliberate stop", tk.Error)
	}
}

// The shape @stop exists for: a router arm ends the flow when a record matches,
// while the other arm does real work. The stop tears down the topology, so the
// sibling branch sees context.Canceled — and that must not surface.
func TestRouterArmToStopEndsTheFlowCleanly(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "r"
	r := step("r", "router")
	r.Routes = []flow.Route{{Path: "$.halt", Cmp: "eq", Value: []byte(`true`), To: "halt"}}
	r.Default = "keep"
	halt := step("halt", "sink")
	halt.Connector = "@stop"
	keep := step("keep", "sink")
	keep.Connector = "@discard"
	doc := &flow.Document{Name: "router-stop", Steps: []flow.Step{in, r, halt, keep}}

	body := []byte(`{"halt":false}` + "\n" + `{"halt":true}` + "\n" + `{"halt":false}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s (%s), want completed", tk.State, tk.Error)
	}
	if !tk.Stopped || tk.StopStep != "halt" {
		t.Fatalf("stopped=%v step=%q, want the halt arm recorded", tk.Stopped, tk.StopStep)
	}
}

// A flow that never routes anything into @stop is completely unaffected: the
// terminal is only reached if a record arrives.
func TestStopNeverReachedLeavesFlowUnaffected(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "r"
	r := step("r", "router")
	r.Routes = []flow.Route{{Path: "$.halt", Cmp: "eq", Value: []byte(`true`), To: "halt"}}
	r.Default = "keep"
	halt := step("halt", "sink")
	halt.Connector = "@stop"
	keep := step("keep", "sink")
	keep.Connector = "@discard"
	doc := &flow.Document{Name: "no-stop", Steps: []flow.Step{in, r, halt, keep}}

	body := []byte(`{"halt":false}` + "\n" + `{"halt":false}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s (%s), want completed", tk.State, tk.Error)
	}
	if tk.Stopped {
		t.Fatal("Stopped set on a flow whose @stop was never reached")
	}
	if tk.SinkConfirmed != 2 {
		t.Fatalf("sink confirmed = %d, want 2 — both records took the default arm", tk.SinkConfirmed)
	}
}

// A genuine failure must still fail, with the real cause reported — the
// canonical-error rule must not turn errors into successes on its way to
// making stops successes.
func TestGenuineFailureStillFailsUnderFanOut(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"a", "b"}
	a := step("a", "sink")
	a.Connector = "@discard"
	// A connector that does not exist fails when the branch binds its sink.
	b := step("b", "sink")
	b.Connector, b.Action = "no-such-connector", "put"
	doc := &flow.Document{Name: "boom", Steps: []flow.Step{in, tee, a, b}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"n":1}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" {
		t.Fatalf("state = %s, want failed — a real error must not be classified away", tk.State)
	}
	if tk.Stopped {
		t.Fatal("a failing flow was marked stopped")
	}
	if !strings.Contains(tk.Error, "no-such-connector") {
		t.Fatalf("error = %q, want the real cause named", tk.Error)
	}
}

// A stop resolved by the fan-out executor must survive the runner's own
// classification pass, even when the caller's context is also done. The
// executor reports a stop with a NIL error; re-deriving the outcome from that
// nil while the parent context is cancelled would report a genuine
// cancellation and fail a deliberately-stopped task.
func TestStopSurvivesAConcurrentCallerCancellation(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "r"
	r := step("r", "router")
	r.Routes = []flow.Route{{Path: "$.halt", Cmp: "eq", Value: []byte(`true`), To: "halt"}}
	r.Default = "keep"
	halt := step("halt", "sink")
	halt.Connector = "@stop"
	keep := step("keep", "sink")
	keep.Connector = "@discard"
	doc := &flow.Document{Name: "stop-vs-cancel", Steps: []flow.Step{in, r, halt, keep}}

	res := execResult{stopped: true, stopStep: "halt"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err := doc.Plan()
	if err != nil {
		t.Fatal(err)
	}
	got, runErr := svc.routeMultiError(ctx, plan, doc, func(s string) string { return s }, res, nil)
	if runErr != nil {
		t.Fatalf("runErr = %v, want nil — the recorded stop is final", runErr)
	}
	if !got.stopped || got.stopStep != "halt" {
		t.Fatalf("stopped=%v step=%q, want the stop preserved", got.stopped, got.stopStep)
	}
}
