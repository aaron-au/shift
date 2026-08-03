package api

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/webhook"
)

// Two costs the trigger paths used to carry per invocation, both removed:
// a goroutine polling the task store every 200ms until the task finished,
// and a full re-parse of a flow document that had not changed since it was
// registered.

const triggerFlow = `{"name":"f",
  "source":{"connector":"gen","action":"gen","config":{"records":1}},
  "sink":{"connector":"gen","action":"discard"}}`

// The execution report used to arrive ~200ms late: reportWhenDone checked
// once (task still waiting), slept 200ms, then saw the terminal state. It is
// a direct completion callback now, so it lands as soon as the task ends.
//
// The bound below is the whole point of the change and is chosen to FAIL
// against the old polling implementation, which could not beat its own
// 200ms tick.
func TestDirectExecutionReportsWithoutPolling(t *testing.T) {
	const oldPollInterval = 200 * time.Millisecond
	reported := make(chan task.Task, 4)
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: webhook.NewRegistry(),
		Report: func(tk task.Task, trigger string) {
			if trigger != "api" {
				t.Errorf("trigger = %q, want api", trigger)
			}
			reported <- tk
		},
	})
	start := time.Now()
	if rec := serve(h, req(http.MethodPost, "/api/flows/execute", triggerFlow)); rec.Code != http.StatusAccepted {
		t.Fatalf("execute = %d, want 202", rec.Code)
	}
	select {
	case tk := <-reported:
		if tk.State != task.StateCompleted && tk.State != task.StateFailed {
			t.Fatalf("reported state = %q, want terminal", tk.State)
		}
		if elapsed := time.Since(start); elapsed >= oldPollInterval {
			t.Fatalf("report took %s; a completion callback must beat the %s poll it replaced",
				elapsed, oldPollInterval)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no execution report — the completion callback never fired")
	}
}

// The webhook path reports too, and must report exactly once: the callback
// replaced a poller, it did not join it.
func TestWebhookReportsExactlyOnce(t *testing.T) {
	var count atomic.Int64
	reported := make(chan struct{}, 4)
	reg := webhook.NewRegistry()
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: reg,
		Report: func(task.Task, string) {
			count.Add(1)
			reported <- struct{}{}
		},
	})
	hookDoc := `{"document":{"name":"hook",
	  "source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"gen","action":"discard"}}}`
	if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", hookDoc)); rec.Code != http.StatusOK {
		t.Fatalf("register = %d", rec.Code)
	}
	if rec := serve(h, req(http.MethodPost, "/hooks/ingest", `{"a":1}`)); rec.Code != http.StatusAccepted {
		t.Fatalf("hook = %d, want 202", rec.Code)
	}
	select {
	case <-reported:
	case <-time.After(10 * time.Second):
		t.Fatal("webhook execution was never reported")
	}
	// Give a duplicate every chance to arrive.
	time.Sleep(300 * time.Millisecond)
	if n := count.Load(); n != 1 {
		t.Fatalf("reported %d times, want exactly 1", n)
	}
}

// A runner with no hub must not pay for reporting machinery it cannot use.
func TestNoReporterMeansNoCallback(t *testing.T) {
	if onDone(nil, "api") != nil {
		t.Fatal("a nil reporter must yield a nil callback so the service skips it entirely")
	}
	if onDone(func(task.Task, string) {}, "api") == nil {
		t.Fatal("a real reporter must yield a callback")
	}
}

// Registration parses the document once; the trigger endpoint — the hottest
// path on the runner — must reuse that work rather than re-decoding and
// re-validating an unchanged document on every inbound POST.
func TestWebhookDocumentParsedOnceAtRegistration(t *testing.T) {
	reg := webhook.NewRegistry()
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: reg,
	})
	hookDoc := `{"document":{"name":"hook",
	  "source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"gen","action":"discard"}}}`
	if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", hookDoc)); rec.Code != http.StatusOK {
		t.Fatalf("register = %d", rec.Code)
	}
	hook, ok := reg.Get("ingest")
	if !ok {
		t.Fatal("hook not registered")
	}
	if hook.Parsed == nil {
		t.Fatal("registration stored no parsed document; the trigger path would re-parse per request")
	}
	if hook.Parsed.Name != "hook" {
		t.Fatalf("parsed name = %q, want hook", hook.Parsed.Name)
	}
}

// The parsed document is shared by every concurrent invocation, so it must
// be treated as immutable. Under -race this is the guard against a future
// change that mutates it on the execution path.
func TestConcurrentWebhookInvocationsShareTheParsedDocument(t *testing.T) {
	reg := webhook.NewRegistry()
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: reg,
	})
	hookDoc := `{"document":{"name":"hook",
	  "source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"gen","action":"discard"}}}`
	if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", hookDoc)); rec.Code != http.StatusOK {
		t.Fatalf("register = %d", rec.Code)
	}
	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			serve(h, req(http.MethodPost, "/hooks/ingest", `{"a":1}`))
		}()
	}
	for range 8 {
		<-done
	}
}
