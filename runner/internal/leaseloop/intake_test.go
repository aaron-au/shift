package leaseloop

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// leasedTask is one hub task carrying the given flow document.
func leasedTask(id, doc string) *hubclient.LeasedTask {
	return &hubclient.LeasedTask{ID: id, Document: json.RawMessage(doc)}
}

// These cases exercise the intake's deterministic paths — no connector
// subprocess, no reliance on how long a real task takes — so they run in the
// coverage pass too (see coverage_skip_test.go).

// TestSubmitRejectionReportsFail: a task the service refuses (the runner is
// draining) must be reported to the hub, not dropped. Losing it here would
// strand the lease until it expired.
func TestSubmitRejectionReportsFail(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.task = leasedTask("drain-me",
		`{"name":"x","source":{"connector":"nope","action":"gen"},"sink":{"connector":"nope2","action":"discard"}}`)
	svc := newService(t, t.TempDir(), 0)
	// Drain before the loop starts: SubmitWith rejects everything from now on.
	if err := svc.Close(time.Second); err != nil {
		t.Fatal(err)
	}

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	if msg := hub.failMsgs()[0]; !strings.Contains(msg, "draining") {
		t.Fatalf("fail msg = %q, want the draining rejection", msg)
	}
}

// TestInvalidSecretNameReportsFail: a document carrying a malformed
// {"$secret": …} name never reaches the hub's secret API — it is rejected
// locally and reported by name only.
func TestInvalidSecretNameReportsFail(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.task = leasedTask("bad-secret-name",
		`{"name":"s","source":{"connector":"http","action":"get","config":{"token":{"$secret":"not a valid name!"}}},"sink":{"connector":"gen","action":"discard"}}`)
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	waitFor(t, 3*time.Second, func() bool { return len(hub.failMsgs()) == 1 })
	msg := hub.failMsgs()[0]
	if !strings.Contains(msg, "secret resolution") || !strings.Contains(msg, "invalid secret name") {
		t.Fatalf("fail msg = %q, want an invalid-secret-name rejection", msg)
	}
}

// TestReportRetriesTransientFailure: a 5xx on the terminal report is NOT
// final (unlike a 409 lease-lost) — the loop retries until the hub records
// the outcome, counting the failed attempt.
func TestReportRetriesTransientFailure(t *testing.T) {
	hub, client := newLeaseHub(t)
	hub.failStatus = http.StatusInternalServerError
	hub.failStatusFirst = 1 // only the first attempt fails
	hub.task = leasedTask("flaky-report", `{"name":""}`)
	svc := newService(t, t.TempDir(), 0)

	l := newLoop(client, svc)
	runLoop(t, l)

	// The retry sleeps 1s after the first attempt (time.Second << 0).
	waitFor(t, 10*time.Second, func() bool { return len(hub.failMsgs()) >= 2 })
	time.Sleep(200 * time.Millisecond)
	msgs := hub.failMsgs()
	if len(msgs) != 2 {
		t.Fatalf("fail attempts = %d, want exactly 2 (one retry, then accepted)", len(msgs))
	}
	if msgs[0] != msgs[1] {
		t.Fatalf("retry changed the reported error: %q then %q", msgs[0], msgs[1])
	}
	if l.Status().Errors < 1 {
		t.Fatalf("Errors = %d, want the failed report attempt counted", l.Status().Errors)
	}
}
