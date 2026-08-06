package gwclient

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
)

// ADR-0042 §1: async is the DEFAULT. A flow with nothing to say to the caller
// answers 202 immediately and runs with the caller gone, so the gateway holds
// an exchange for "validate and accept" rather than for the whole flow.
//
// The mode is decided by the terminal node, not by a flag — which is why this
// test and the one below differ only in their sink.
func TestAFlowWithNoResponseSinkIsAcceptedAsynchronously(t *testing.T) {
	done := make(chan task.Task, 1)
	l := loopFor(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard"}
	}`, func(tk task.Task) { done <- tk })

	status, body, ctype := l.execute(t.Context(), &inbound{id: "req-1", flow: "orders",
		body: []byte(`{"order_id":"ORD-1"}`)})

	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", status, body)
	}
	if ctype != "application/json" {
		t.Errorf("content-type = %q, want application/json", ctype)
	}
	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("202 body is not valid JSON: %v (%s)", err, body)
	}
	if got.Task == "" {
		t.Error("202 carries no task id, so the caller has nothing to correlate with")
	}
	if got.Flow != "orders" || got.Status != "accepted" {
		t.Errorf("202 body = %+v, want the flow name and status accepted", got)
	}
	if got.AcceptedAt == "" {
		t.Error("202 carries no accepted_at")
	}

	// The flow really did run — AFTER the response was produced. That ordering
	// is the whole point: the caller is not waiting for this.
	select {
	case tk := <-done:
		if tk.State != task.StateCompleted {
			t.Errorf("task state = %q, want completed", tk.State)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the accepted flow never completed")
	}
}

// The synchronous opt-in, unchanged: a flow terminating at @response keeps the
// caller's exchange open and returns the flow's output.
func TestAFlowWithAResponseSinkStaysSynchronous(t *testing.T) {
	l := loopFor(t, `{
		"name": "echo",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@response"}
	}`, nil)

	status, body, ctype := l.execute(t.Context(), &inbound{id: "req-1", flow: "echo",
		body: []byte(`{"order_id":"ORD-1"}`)})

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", status, body)
	}
	if ctype != "application/x-ndjson" {
		t.Errorf("content-type = %q, want application/x-ndjson", ctype)
	}
	if len(body) == 0 {
		t.Error("a synchronous flow returned an empty body")
	}
}

// Verification runs BEFORE the accept decision, so an async flow still rejects
// a malformed request rather than answering 202 and dead-lettering it. This is
// what makes the 202 mean "this will run" rather than "the bytes arrived".
func TestAnAsyncFlowStillRejectsABadRequest(t *testing.T) {
	l := loopFor(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"schema": {"type":"object","required":["order_id"]}}},
		"sink": {"connector": "@discard"}
	}`, func(task.Task) { t.Error("a rejected request was executed") })

	status, body, _ := l.execute(t.Context(), &inbound{id: "req-1", flow: "orders",
		body: []byte(`{"nope":1}`)})

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", status, body)
	}
	// Give the (absent) execution a moment to prove it is absent.
	time.Sleep(50 * time.Millisecond)
}

// A flow the runner does not hold is configuration drift, and must not become
// a 202: acknowledging work nobody will do is the worst possible answer.
func TestAnUnknownFlowIsNotAccepted(t *testing.T) {
	l := loopFor(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, nil)

	status, _, _ := l.execute(t.Context(), &inbound{id: "req-1", flow: "somewhere-else", body: []byte(`{}`)})
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
}

// loopFor builds a Loop serving exactly one flow, with a real service.
func loopFor(t *testing.T, doc string, onDone func(task.Task)) *Loop {
	t.Helper()
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parsing the flow: %v", err)
	}
	svc := service.New(service.Options{})
	t.Cleanup(func() { _ = svc.Close(5 * time.Second) })

	return &Loop{
		log: discardLog(),
		opts: Options{
			Service: svc,
			Lookup:  func(name string) (*flowdoc.Document, bool) { return d, name == d.Name },
			OnDone:  onDone,
		},
	}
}
