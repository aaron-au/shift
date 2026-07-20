package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// awaitTerminal polls until the task reaches a terminal state (unlike
// awaitTask, it returns the failed task rather than an error).
func awaitTerminal(t *testing.T, svc *Service, id string) task.Task {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		tk, ok := svc.Task(id)
		if ok && (tk.State == task.StateFailed || tk.State == task.StateCompleted) {
			return tk
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never reached terminal state: %+v", tk)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// coerceFailFlow is a v2 graph: gen source → a coerce that always fails
// (name is a non-numeric string coerced to int) → discard sink, with a
// dead-letter handler hung off the source's onFailure.
func coerceFailFlow(handler bool) *flow.Document {
	cfg, _ := json.Marshal(map[string]any{"records": 100})

	src := flow.Step{ID: "in", Connector: "gen", Action: "gen", Config: cfg, OnSuccess: "bad"}
	src.Type = "source"
	if handler {
		src.OnFailure = "dead"
	}
	bad := flow.Step{ID: "bad", OnComplete: "out"}
	bad.Op = flow.Op{Type: "coerce", Rules: []flow.CoerceRule{{Field: "name", To: "int"}}}
	out := flow.Step{ID: "out", Connector: "gen", Action: "discard"}
	out.Type = "sink"

	steps := []flow.Step{src, bad, out}
	if handler {
		dead := flow.Step{ID: "dead", Connector: "gen", Action: "discard"}
		dead.Type = "sink"
		steps = append(steps, dead)
	}
	return &flow.Document{Name: "err-flow", Start: "in", Steps: steps}
}

// TestErrorRoutingAndRedaction proves the whole outcome-edge path: a mid-
// pipeline step failure routes to its onFailure handler, the task ends
// failed-but-handled, and any resolved secret value is redacted from the
// error text (the coerce error embeds the failing record's name).
func TestErrorRoutingAndRedaction(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	// "customer-000000" is the first record's name; mark it secret so we can
	// assert it is masked out of the error the failing coerce produces.
	id, err := svc.SubmitWith(coerceFailFlow(true), SubmitOpts{SecretValues: []string{"customer-000000"}})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if !tk.Handled || tk.HandlerStep != "dead" {
		t.Fatalf("handler not recorded: handled=%v step=%q", tk.Handled, tk.HandlerStep)
	}
	if strings.Contains(tk.Error, "customer-000000") {
		t.Fatalf("secret leaked into error: %q", tk.Error)
	}
	if !strings.Contains(tk.Error, "***") {
		t.Fatalf("error not redacted: %q", tk.Error)
	}
	// The failing step is attributed by id (OpError → step id).
	if !strings.Contains(tk.Error, "bad") {
		t.Fatalf("error missing failing step id: %q", tk.Error)
	}
}

// TestNoHandlerFailsAsBefore is the regression: without an onFailure edge,
// a failing step fails the task exactly as it did pre-M5a (unhandled).
func TestNoHandlerFailsAsBefore(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	id, err := svc.Submit(coerceFailFlow(false), false)
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" || tk.Handled {
		t.Fatalf("want unhandled failure, got state=%s handled=%v", tk.State, tk.Handled)
	}
}

// TestErrorRecordIsPayloadFree checks the record handed to a handler
// carries only metadata — never any of the flow's payload fields.
func TestErrorRecordIsPayloadFree(t *testing.T) {
	b := errorRecord("f", "bad", "boom ***", "2026-07-20T00:00:00Z")
	if b.Len() != 1 {
		t.Fatalf("want exactly one record, got %d", b.Len())
	}
	rec := b.Record(0)
	if rec.Kind() != record.KindMap {
		t.Fatalf("record kind = %v", rec.Kind())
	}
	want := map[string]bool{"flow": true, "step": true, "error": true, "at": true}
	for i := range rec.Len() {
		key := string(rec.KeyAt(i))
		if !want[key] {
			t.Fatalf("unexpected key %q in error record (payload leak?)", key)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing keys %v", want)
	}
}
