package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
)

// The connectors here are deliberately NOT INSTALLED. That makes every
// assertion below unambiguous: if a run completes, the diversion held and the
// connector was never touched; if it fails naming the connector, the real one
// was used. Nothing depends on which binaries happen to exist.
const absentConnector = "not-installed"

// divertedFlow: a real connector source with test input, through a probe, to a
// real connector sink with a mock. Both connectors stay in the document — that
// is the whole point of the revised §5.
func divertedFlow(records string) *flow.Document {
	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = absentConnector, "get", "p"
	in.TestInput = &flowdoc.TestInput{Enabled: true}
	if records != "" {
		_ = json.Unmarshal([]byte(`[`+records+`]`), &in.TestInput.Records)
	}
	probe := step("p", "probe")
	probe.OnSuccess = "out"
	out := step("out", "sink")
	out.Connector, out.Action = absentConnector, "put"
	out.Mock = &flowdoc.Mock{Enabled: true}
	return &flow.Document{Name: "diverted", Steps: []flow.Step{in, probe, out}}
}

// The revision in one test: a test run drives NEITHER connector, and both are
// still in the document. It completes only because the diversions held —
// neither connector exists to be launched.
func TestTestRunDivertsBothEndsWithoutTouchingTheConnectors(t *testing.T) {
	svc := newTestService(t, Options{})
	doc := divertedFlow(`{"id":1},{"id":2},{"id":3}`)

	id, err := svc.SubmitWith(doc, SubmitOpts{Test: true, Capture: true, CaptureMax: 10})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s — a diverted test run must not reach the connectors", tk.State, tk.Error)
	}
	if tk.SinkConfirmed != 3 {
		t.Fatalf("mock recorded %d records, want the 3 supplied as test input", tk.SinkConfirmed)
	}
	by := capturedBy(t, tk)
	if len(by["p"]) != 3 {
		t.Fatalf("probe captured %d, want 3: %v", len(by["p"]), by["p"])
	}
	if !strings.Contains(strings.Join(by["p"], ""), `"id":2`) {
		t.Fatalf("probe capture does not carry the test input: %v", by["p"])
	}
}

// Deployed, both options are inert and the REAL connectors are used — proven
// by the failure naming the connector it tried to launch. An inert mock does
// not quietly stand in for a sink that cannot be reached.
func TestDeployedRunUsesTheRealConnectors(t *testing.T) {
	svc := newTestService(t, Options{})
	doc := divertedFlow(`{"id":1}`)

	id, err := svc.SubmitWith(doc, SubmitOpts{}) // no Test
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" {
		t.Fatalf("state = %s; a deployed flow must drive the real connector, not the diversion", tk.State)
	}
	if !strings.Contains(tk.Error, absentConnector) {
		t.Fatalf("the failure does not name the connector it tried to use: %q", tk.Error)
	}
	if tk.SinkConfirmed != 0 {
		t.Fatalf("a deployed mock recorded %d records; it must be inert", tk.SinkConfirmed)
	}
}

// A mock switched OFF drives the real connector even in a test run — how an
// author says "hitting the real system IS the test". The source diversion is
// left on, so this isolates the sink.
func TestAnUncheckedMockDrivesTheRealSinkInTest(t *testing.T) {
	svc := newTestService(t, Options{})
	doc := divertedFlow(`{"id":1}`)
	doc.Steps[2].Mock.Enabled = false

	id, err := svc.SubmitWith(doc, SubmitOpts{Test: true})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" {
		t.Fatalf("state = %s; an unchecked mock must let the real sink run", tk.State)
	}
	if !strings.Contains(tk.Error, absentConnector) {
		t.Fatalf("the failure does not name the real sink: %q", tk.Error)
	}
}

// "Strictly inert" is a claim about the compiled PIPELINE, not just its output:
// deployed, a probe is not an operator that does nothing, it is no operator at
// all. Otherwise every probe left on a canvas would cost a deployed flow a
// batch hand-off and a telemetry row for ever.
func TestAProbeCompilesToNothingWhenDeployed(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "p"
	probe := step("p", "probe")
	probe.OnSuccess = "out"
	out := step("out", "sink")
	out.Connector = "@discard"
	doc := &flow.Document{Name: "probe-inert", Steps: []flow.Step{in, probe, out}}
	body := []byte(`{"n":1}` + "\n" + `{"n":2}` + "\n")

	deployed, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	dt := awaitTerminal(t, svc, deployed)
	if dt.State != "completed" {
		t.Fatalf("state = %s: %s", dt.State, dt.Error)
	}
	for _, op := range dt.Ops {
		if op.StepID == "p" {
			t.Fatalf("a deployed probe compiled to an operator: %+v", op)
		}
	}
	if dt.SinkConfirmed != 2 {
		t.Fatalf("the deployed flow moved %d records, want 2", dt.SinkConfirmed)
	}

	// In test it IS there, and the record count is identical — additive, not
	// altering.
	tested, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body, Test: true})
	if err != nil {
		t.Fatal(err)
	}
	tt := awaitTerminal(t, svc, tested)
	if tt.SinkConfirmed != dt.SinkConfirmed {
		t.Fatalf("test run moved %d records, deployed moved %d; a probe must not change what flows",
			tt.SinkConfirmed, dt.SinkConfirmed)
	}
	found := false
	for _, op := range tt.Ops {
		if op.StepID == "p" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no probe operator in a test run: %+v", tt.Ops)
	}
}

// An author who has ticked the box but not filled it in yet is halfway through
// building a canvas, not shipping.
func TestEmptyTestInputEmitsNothing(t *testing.T) {
	svc := newTestService(t, Options{})
	doc := divertedFlow("")

	id, err := svc.SubmitWith(doc, SubmitOpts{Test: true})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if tk.SinkConfirmed != 0 {
		t.Fatalf("empty test input produced %d records", tk.SinkConfirmed)
	}
}
