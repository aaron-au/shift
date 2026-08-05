package service

import (
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
)

// The diagnostic fields must actually reach the task record. They were
// measured by the engine before this and dropped in the conversion, which is
// the failure mode worth pinning: the data existed and nobody could see it.
func TestExecutionReportCarriesDiagnosticDetail(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "out"
	out := step("out", "sink")
	out.Connector = "@discard"
	doc := &flow.Document{Name: "diag", Steps: []flow.Step{in, out}}

	body := []byte(`{"n":1,"pad":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` + "\n" +
		`{"n":2,"pad":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if len(tk.Ops) == 0 {
		t.Fatal("no per-step stats recorded")
	}

	src := tk.Ops[0]
	if src.Batches == 0 {
		t.Error("Batches = 0; batching pathology would be undetectable")
	}
	// The source figure is the exact one — it is the flow's real input size,
	// and it is the dimension record counts hide.
	if src.Bytes == 0 {
		t.Error("Bytes = 0 at the source; record counts alone cannot distinguish 2 tiny records from 2 huge ones")
	}
	// Inclusive time must be at least the step's own work, by definition.
	if src.WallSeconds < src.Seconds {
		t.Errorf("WallSeconds (%v) < Seconds (%v); inclusive time cannot be less than exclusive", src.WallSeconds, src.Seconds)
	}
}

// Phases answer "where did the wall time go" without a log pipeline: a fixed
// handful of numbers per execution.
func TestPhasesRecordWhereTheTimeWent(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "out"
	out := step("out", "sink")
	out.Connector = "@discard"
	doc := &flow.Document{Name: "phases", Steps: []flow.Step{in, out}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"n":1}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if tk.Phases.TotalMS <= 0 {
		t.Fatal("TotalMS not recorded")
	}
	// Total spans submit to terminal, so it cannot be less than the run it
	// contains — a violation would mean the spans are measuring the wrong
	// things rather than merely being imprecise.
	if tk.Phases.RunMS > tk.Phases.TotalMS {
		t.Errorf("RunMS (%v) > TotalMS (%v); the run cannot outlast the execution containing it",
			tk.Phases.RunMS, tk.Phases.TotalMS)
	}
	if tk.Phases.AdmissionMS < 0 || tk.Phases.BindMS < 0 {
		t.Errorf("negative phase: admission=%v bind=%v", tk.Phases.AdmissionMS, tk.Phases.BindMS)
	}
}
