package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// A flow whose source is the built-in @webhook and whose sink is a built-in
// terminal (@discard / @response) launches NO connector subprocess: the whole
// execution path — admission, pipeline build, run, result recording — is
// in-process and deterministic, so these run in the coverage pass too.

// newBuiltinService builds a service pointed at an empty connector dir. Every
// flow here is connector-free, so nothing is ever launched.
func newBuiltinService(t *testing.T, opts Options) *Service {
	t.Helper()
	if opts.ConnectorDir == "" {
		opts.ConnectorDir = t.TempDir()
	}
	svc := New(opts)
	t.Cleanup(func() {
		if err := svc.Close(5 * time.Second); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return svc
}

// hookDoc is a @webhook → ops → sink flow in the linear (sugar) form, whose
// synthesized step ids are source / op0… / sink.
func hookDoc(name string, ops []flow.Op, sink string) *flow.Document {
	return &flow.Document{
		Name:   name,
		Source: flow.Endpoint{Connector: flowdoc.WebhookSource, Action: "ndjson"},
		Ops:    ops,
		Sink:   flow.Endpoint{Connector: sink},
	}
}

func ndjsonBody(lines ...string) []byte {
	return []byte(strings.Join(lines, "\n") + "\n")
}

func filterOp(path, value string) flow.Op {
	return flow.Op{Type: "filter", Path: path, Cmp: "eq", Value: json.RawMessage(value)}
}

// TestRunSyncResponseSinkStreamsOutput is the synchronous request-reply path
// (ADR-0024): the flow's terminal stream comes back to the caller as NDJSON,
// runner-side, in the same call.
func TestRunSyncResponseSinkStreamsOutput(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	doc := hookDoc("hook-response", []flow.Op{filterOp("$.keep", "true")}, flowdoc.ResponseSink)
	body := ndjsonBody(`{"keep":true,"n":1}`, `{"keep":false,"n":2}`, `{"keep":true,"n":3}`)

	var out bytes.Buffer
	tk, err := svc.RunSync(doc, SubmitOpts{WebhookBody: body, Response: &out})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
	if tk.RecordsIn != 3 {
		t.Errorf("records in = %d, want 3 (the whole body)", tk.RecordsIn)
	}
	if tk.RecordsOut != 2 || tk.SinkConfirmed != 2 {
		t.Errorf("out = %d confirmed = %d, want 2/2 (filter keeps two)", tk.RecordsOut, tk.SinkConfirmed)
	}

	// The body is exactly the kept records, one JSON object per line.
	got := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(got) != 2 {
		t.Fatalf("response lines = %d (%q), want 2", len(got), out.String())
	}
	if !strings.Contains(got[0], `"n":1`) || !strings.Contains(got[1], `"n":3`) {
		t.Errorf("response body = %q, want the kept records in order", out.String())
	}
	if strings.Contains(out.String(), `"n":2`) {
		t.Errorf("filtered-out record reached the caller: %q", out.String())
	}

	// Per-step telemetry is keyed by the synthesized step ids.
	var ids []string
	for _, op := range tk.Ops {
		ids = append(ids, op.StepID)
	}
	if len(ids) != 3 || ids[0] != "source" || ids[1] != "op0" || ids[2] != "sink" {
		t.Errorf("step ids = %v, want [source op0 sink]", ids)
	}
	if st := svc.Status(); st.Totals.Completed != 1 || st.Governor.Used != 0 {
		t.Errorf("status after RunSync: %+v (admission not released?)", st)
	}
}

// TestRunSyncDiscardSinkCounts: @discard drops the stream but still reports an
// honest record count, so a source-side-only flow has a real result.
func TestRunSyncDiscardSinkCounts(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	body := ndjsonBody(`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`)

	tk, err := svc.RunSync(hookDoc("hook-discard", nil, flowdoc.DiscardSink), SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
	if tk.RecordsIn != 4 || tk.RecordsOut != 4 || tk.SinkConfirmed != 4 {
		t.Errorf("in=%d out=%d confirmed=%d, want 4/4/4", tk.RecordsIn, tk.RecordsOut, tk.SinkConfirmed)
	}
	if len(tk.Ops) != 2 {
		t.Errorf("ops = %+v, want source + sink only", tk.Ops)
	}
}

// TestRunSyncResponseSinkWithoutWriterDegrades: a @response flow submitted
// with nowhere to write (e.g. the async path) must still run and count, not
// fail or panic — the documented degradation to a counting drop.
func TestRunSyncResponseSinkWithoutWriterDegrades(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	body := ndjsonBody(`{"n":1}`, `{"n":2}`, `{"n":3}`)

	tk, err := svc.RunSync(hookDoc("hook-noresp", nil, flowdoc.ResponseSink), SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
	if tk.SinkConfirmed != 3 {
		t.Errorf("confirmed = %d, want 3 (counted even with no writer)", tk.SinkConfirmed)
	}
}

// TestWebhookFlowWithoutBodyFails: the body is the source. Without one the
// task fails cleanly rather than running on an empty stream.
func TestWebhookFlowWithoutBodyFails(t *testing.T) {
	svc := newBuiltinService(t, Options{})

	tk, err := svc.RunSync(hookDoc("hook-nobody", nil, flowdoc.DiscardSink), SubmitOpts{})
	if err != nil {
		t.Fatalf("RunSync returned an error instead of a failed task: %v", err)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if !strings.Contains(tk.Error, "requires a request body") {
		t.Errorf("error = %q, want the missing-body reason", tk.Error)
	}
	if tk.SinkConfirmed != 0 {
		t.Errorf("confirmed = %d, want 0", tk.SinkConfirmed)
	}
}

// TestRunSyncRejectsInvalidDocument: validation happens before registration,
// so a bad document never becomes a task.
func TestRunSyncRejectsInvalidDocument(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	bad := hookDoc("hook-bad", []flow.Op{{Type: "warp-speed"}}, flowdoc.DiscardSink)

	tk, err := svc.RunSync(bad, SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`)})
	if err == nil {
		t.Fatal("invalid document accepted")
	}
	if tk.ID != "" {
		t.Errorf("task returned for an invalid document: %+v", tk)
	}
	if got := svc.Status().Totals.Submitted; got != 0 {
		t.Errorf("invalid flow was recorded: submitted = %d", got)
	}
}

// TestMissingSinkConnectorFails: a built-in source with a real sink connector
// that is not installed fails closed, naming the connector.
func TestMissingSinkConnectorFails(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	doc := hookDoc("hook-nosink", nil, "")
	doc.Sink = flow.Endpoint{Connector: "not-installed-connector", Action: "put"}

	tk, err := svc.RunSync(doc, SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if !strings.Contains(tk.Error, "not-installed-connector") {
		t.Errorf("error = %q, want the missing connector named", tk.Error)
	}
}

// TestSecretRedactedFromTaskError: a resolved secret that leaks into an engine
// error (here the coerce error quotes the offending value) is masked before
// the error is recorded — the task record travels to the hub (ADR-0010).
func TestSecretRedactedFromTaskError(t *testing.T) {
	const secret = "sk-live-do-not-log" //nolint:gosec // G101: test fixture standing in for a resolved secret, not a real credential
	svc := newBuiltinService(t, Options{})
	doc := hookDoc("hook-secret", []flow.Op{
		{Type: "coerce", Rules: []flow.CoerceRule{{Field: "token", To: "int"}}},
	}, flowdoc.DiscardSink)

	tk, err := svc.RunSync(doc, SubmitOpts{
		WebhookBody:  ndjsonBody(`{"token":"` + secret + `"}`),
		SecretValues: []string{secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if strings.Contains(tk.Error, secret) {
		t.Fatalf("secret leaked into the task error: %q", tk.Error)
	}
	if !strings.Contains(tk.Error, "***") {
		t.Errorf("error = %q, want the secret masked", tk.Error)
	}
	if !strings.Contains(tk.Error, "op0") {
		t.Errorf("error = %q, want the failing step id", tk.Error)
	}
}

// TestCaptureIsBoundedAndRedacted: test-mode capture keeps a bounded, redacted
// sample per step. It is payload, so it stays runner-side and masked (ADR-0014).
func TestCaptureIsBoundedAndRedacted(t *testing.T) {
	const secret = "tenant-secret-value"
	svc := newBuiltinService(t, Options{})
	body := ndjsonBody(
		`{"n":1,"token":"`+secret+`"}`,
		`{"n":2,"token":"`+secret+`"}`,
		`{"n":3,"token":"`+secret+`"}`,
		`{"n":4,"token":"`+secret+`"}`,
		`{"n":5,"token":"`+secret+`"}`,
	)

	tk, err := svc.RunSync(hookDoc("hook-capture", nil, flowdoc.DiscardSink), SubmitOpts{
		WebhookBody:  body,
		SecretValues: []string{secret},
		Capture:      true,
		CaptureMax:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
	if len(tk.Captured) == 0 {
		t.Fatal("capture requested but nothing was sampled")
	}
	for _, step := range tk.Captured {
		if len(step.Records) > 2 {
			t.Errorf("step %s captured %d records, want at most CaptureMax=2", step.StepID, len(step.Records))
		}
		if !step.More {
			t.Errorf("step %s: More not set despite 5 records through a max of 2", step.StepID)
		}
		for _, rec := range step.Records {
			if strings.Contains(string(rec), secret) {
				t.Fatalf("step %s: secret leaked into capture: %s", step.StepID, rec)
			}
			if !strings.Contains(string(rec), "***") {
				t.Errorf("step %s: capture not redacted: %s", step.StepID, rec)
			}
		}
	}
}

// TestNoCaptureWhenNotRequested: capture is opt-in; a normal run records none.
func TestNoCaptureWhenNotRequested(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	tk, err := svc.RunSync(hookDoc("hook-nocapture", nil, flowdoc.DiscardSink),
		SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(tk.Captured) != 0 {
		t.Errorf("captured %d steps without Capture set", len(tk.Captured))
	}
}

// TestFailureRoutesToHandler: a step failure is routed to the onFailure
// handler named by the plan, and the handler's OWN failure is recorded
// separately rather than swallowed — the task still fails either way.
func TestFailureRoutesToHandler(t *testing.T) {
	const secret = "sk-handler-secret" //nolint:gosec // G101: test fixture standing in for a resolved secret, not a real credential
	svc := newBuiltinService(t, Options{})

	src := flow.Step{ID: "in", Connector: flowdoc.WebhookSource, Action: "ndjson",
		OnSuccess: "bad", OnFailure: "dead"}
	src.Type = "source"
	bad := flow.Step{ID: "bad", OnComplete: "out"}
	bad.Op = flow.Op{Type: "coerce", Rules: []flow.CoerceRule{{Field: "token", To: "int"}}}
	out := flow.Step{ID: "out", Connector: flowdoc.DiscardSink}
	out.Type = "sink"
	dead := flow.Step{ID: "dead", Connector: "dead-letter-connector", Action: "put"}
	dead.Type = "sink"

	doc := &flow.Document{Name: "handled", Start: "in", Steps: []flow.Step{src, bad, out, dead}}
	tk, err := svc.RunSync(doc, SubmitOpts{
		WebhookBody:  ndjsonBody(`{"token":"` + secret + `"}`),
		SecretValues: []string{secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed (a handled failure is still a failure)", tk.State)
	}
	if !tk.Handled || tk.HandlerStep != "dead" {
		t.Fatalf("handled = %v step = %q, want true/dead", tk.Handled, tk.HandlerStep)
	}
	// The handler connector is not installed, so its own error is recorded.
	if !strings.Contains(tk.HandlerError, "dead-letter-connector") {
		t.Errorf("handler error = %q, want the handler's own failure recorded", tk.HandlerError)
	}
	if strings.Contains(tk.Error, secret) || strings.Contains(tk.HandlerError, secret) {
		t.Fatalf("secret leaked: error=%q handler=%q", tk.Error, tk.HandlerError)
	}
}

// TestUnhandledFailureRecordsNoHandler: without an onFailure edge the failure
// is plain — nothing is marked handled.
func TestUnhandledFailureRecordsNoHandler(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	doc := hookDoc("hook-unhandled", []flow.Op{
		{Type: "coerce", Rules: []flow.CoerceRule{{Field: "token", To: "int"}}},
	}, flowdoc.DiscardSink)

	tk, err := svc.RunSync(doc, SubmitOpts{WebhookBody: ndjsonBody(`{"token":"nope"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if tk.Handled || tk.HandlerStep != "" || tk.HandlerError != "" {
		t.Errorf("unhandled failure reported a handler: %+v", tk)
	}
}

// TestSubmitWithRunsAsynchronously: Submit returns immediately with an id and
// the task reaches its terminal state on its own goroutine.
func TestSubmitWithRunsAsynchronously(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	id, err := svc.SubmitWith(hookDoc("hook-async", nil, flowdoc.DiscardSink),
		SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`, `{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("SubmitWith returned an empty task id")
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
	if tk.SinkConfirmed != 2 {
		t.Errorf("confirmed = %d, want 2", tk.SinkConfirmed)
	}
}

// TestTaskTimeoutDoesNotDisturbFastTask: an opt-in wall-clock cap bounds a
// task without changing the outcome of one that finishes inside it.
func TestTaskTimeoutDoesNotDisturbFastTask(t *testing.T) {
	svc := newBuiltinService(t, Options{TaskTimeout: 30 * time.Second})

	tk, err := svc.RunSync(hookDoc("hook-timeout", nil, flowdoc.DiscardSink),
		SubmitOpts{WebhookBody: ndjsonBody(`{"n":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != task.StateCompleted {
		t.Fatalf("state = %s, error = %q", tk.State, tk.Error)
	}
}

// TestAdmissionAbortsOnDrain: a task that cannot be admitted waits (capacity,
// never a count cap — ADR-0005), and a draining service unblocks it instead of
// stranding it forever.
func TestAdmissionAbortsOnDrain(t *testing.T) {
	// A budget below one task's cost: the reservation can never succeed.
	svc := New(Options{ConnectorDir: t.TempDir(), MemBudget: 1})

	id, err := svc.Submit(hookDoc("hook-stuck", nil, flowdoc.DiscardSink), false)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		tk, ok := svc.Task(id)
		return ok && tk.State == task.StateWaiting
	})

	// Close's graceful window elapses, the base context is cancelled, and the
	// waiting task unwinds.
	if err := svc.Close(50 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != task.StateFailed {
		t.Fatalf("state = %s, want failed", tk.State)
	}
	if !strings.Contains(tk.Error, "admission aborted") {
		t.Errorf("error = %q, want the admission abort reason", tk.Error)
	}
	if used := svc.Status().Governor.Used; used != 0 {
		t.Errorf("governor used = %d after abort, want 0", used)
	}
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// --- built-in terminals, unit level -----------------------------------------

func TestDiscardSinkCountsAcrossBatches(t *testing.T) {
	d := &discardSink{}
	if err := d.Write(t.Context(), genBatch(3)); err != nil {
		t.Fatal(err)
	}
	if err := d.Write(t.Context(), genBatch(2)); err != nil {
		t.Fatal(err)
	}
	if err := d.Write(t.Context(), genBatch(0)); err != nil {
		t.Fatal(err)
	}
	if d.n != 5 {
		t.Errorf("discard counted %d, want 5", d.n)
	}
	if err := d.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestResponseSinkWritesNDJSON(t *testing.T) {
	var buf bytes.Buffer
	r := newResponseSink(&buf)
	if err := r.Write(t.Context(), genBatch(2)); err != nil {
		t.Fatal(err)
	}
	if err := r.Write(t.Context(), genBatch(1)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil { // Close flushes
		t.Fatal(err)
	}
	if r.n != 3 {
		t.Errorf("counted %d records, want 3", r.n)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("wrote %d lines (%q), want 3", len(lines), buf.String())
	}
	if lines[0] != `{"id":0}` {
		t.Errorf("line 0 = %q, want a JSON object per record", lines[0])
	}
}

// A @response flow with nowhere to write still counts what it consumed, so
// the execution report stays honest instead of reading as an empty run.
func TestNewResponseSinkNilWriterStillCounts(t *testing.T) {
	r := newResponseSink(nil)
	if err := r.Write(t.Context(), genBatch(4)); err != nil {
		t.Fatal(err)
	}
	if r.n != 4 {
		t.Errorf("counted %d records with a nil writer, want 4", r.n)
	}
	if err := r.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

// A failed write is not counted as delivered.
func TestResponseSinkWriteErrorNotCounted(t *testing.T) {
	var buf bytes.Buffer
	r := newResponseSink(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.Write(ctx, genBatch(3))
	if err == nil {
		t.Fatal("write on a cancelled context succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if r.n != 0 {
		t.Errorf("counted %d records after a failed write, want 0", r.n)
	}
}
