package service

import (
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
)

// TestWebhookSourceExecution: a flow whose source is the built-in @webhook
// runs over the injected request body (no source connector spawned).
func TestWebhookSourceExecution(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	doc := &flow.Document{
		Name:   "hook-flow",
		Source: flow.Endpoint{Connector: "@webhook", Action: "ndjson"},
		Ops:    []flow.Op{{Type: "filter", Path: "$.keep", Cmp: "eq", Value: []byte("true")}},
		Sink:   flow.Endpoint{Connector: "gen", Action: "discard"},
	}
	body := []byte(`{"keep":true,"n":1}` + "\n" + `{"keep":false,"n":2}` + "\n" + `{"keep":true,"n":3}` + "\n")

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if tk.RecordsIn != 3 {
		t.Errorf("records in = %d, want 3 (whole body)", tk.RecordsIn)
	}
	if tk.SinkConfirmed != 2 { // filter keeps two
		t.Errorf("sink confirmed = %d, want 2", tk.SinkConfirmed)
	}
}

// TestWebhookSourceRequiresBody: a @webhook flow with no body fails cleanly.
func TestWebhookSourceRequiresBody(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})
	doc := &flow.Document{
		Name:   "hook-flow",
		Source: flow.Endpoint{Connector: "@webhook", Action: "ndjson"},
		Sink:   flow.Endpoint{Connector: "gen", Action: "discard"},
	}
	id, err := svc.SubmitWith(doc, SubmitOpts{}) // no WebhookBody
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "failed" {
		t.Fatalf("state = %s, want failed", tk.State)
	}
}
