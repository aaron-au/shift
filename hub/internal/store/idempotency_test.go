package store_test

import (
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// TC-010 (ADR-0002). The queue re-dispatch tests next door prove a task comes
// BACK; the derivation tests in pkg/flowdoc prove a key is BUILT. Neither
// asserts the join: that the key handed to the runner on attempt N+1 — and
// therefore, via leaseloop's WithSinkConfig injection, to the sink — is the
// same bytes as on attempt N. That join is where at-least-once silently
// becomes at-least-twice-under-different-keys, i.e. duplicate side effects on
// a receiver that was doing exactly what it was told.
//
// These are store-level: the key's stability is a property of the queue row,
// and nothing between Claim and the sink recomputes it (the API marshals
// store.Task straight onto the lease response; leaseloop reads the field and
// injects it verbatim). hub/e2e/idempotency_test.go closes the remaining span
// by reading the key off the wire at a real HTTP sink across a real crash.

// dispatchKey is deliberately not a bare word: a key that is regenerated
// rather than carried would very likely still be non-empty, so the assertion
// has to be on the exact bytes.
const dispatchKey = "orders:2026-08-09T00:15:00Z:batch-7"

func TestTheIdempotencyKeySurvivesLeaseExpiryRedispatchByteForByte(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deadRunner, _ := registerRunner(t, s, "dead")
	liveRunner, _ := registerRunner(t, s, "live")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, dispatchKey, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1: claimed with a lease too short to outlive the sleep, and
	// never heartbeated — a crashed runner.
	first, err := s.Claim(ctx, deadRunner, 50*time.Millisecond)
	if err != nil || first == nil {
		t.Fatalf("claim 1 = %+v, %v", first, err)
	}
	if first.IdempotencyKey != dispatchKey {
		t.Fatalf("attempt 1 key = %q, want %q", first.IdempotencyKey, dispatchKey)
	}
	time.Sleep(100 * time.Millisecond)

	// Attempt 2 on a different runner. The attempt counter is the proof this
	// is a genuine re-dispatch and not the same lease handed back.
	second, err := s.Claim(ctx, liveRunner, 30*time.Second)
	if err != nil || second == nil {
		t.Fatalf("claim 2 = %+v, %v", second, err)
	}
	if second.ID != id || second.Attempt != 2 || first.Attempt != 1 {
		t.Fatalf("re-dispatch: id %q attempts %d→%d, want %q 1→2",
			second.ID, first.Attempt, second.Attempt, id)
	}
	if second.IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("key changed across re-dispatch: %q → %q", first.IdempotencyKey, second.IdempotencyKey)
	}
	if second.IdempotencyKey != dispatchKey {
		t.Fatalf("attempt 2 key = %q, want %q", second.IdempotencyKey, dispatchKey)
	}
}

func TestTheIdempotencyKeySurvivesTheFailAndRequeuePathToo(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "flaky")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, dispatchKey, 3)
	if err != nil {
		t.Fatal(err)
	}

	// A reported failure is a different SQL path from lease reaping — same
	// claim, but requeued by Fail rather than ReapExpired. It has to preserve
	// the key just as faithfully, and it writes to the task row, so an
	// over-broad UPDATE there could plausibly clear it.
	var keys []string
	for attempt := 1; attempt <= 3; attempt++ {
		lt, err := s.Claim(ctx, runnerID, 30*time.Second)
		if err != nil || lt == nil {
			t.Fatalf("claim %d = %+v, %v", attempt, lt, err)
		}
		if lt.ID != id || lt.Attempt != attempt {
			t.Fatalf("claim %d: id %q attempt %d", attempt, lt.ID, lt.Attempt)
		}
		keys = append(keys, lt.IdempotencyKey)

		requeued, err := s.Fail(ctx, lt.ID, runnerID, "boom")
		if err != nil {
			t.Fatal(err)
		}
		if want := attempt < 3; requeued != want {
			t.Fatalf("fail %d: requeued = %v, want %v", attempt, requeued, want)
		}
	}
	for i, k := range keys {
		if k != dispatchKey {
			t.Fatalf("attempt %d key = %q, want %q (keys: %q)", i+1, k, dispatchKey, keys)
		}
	}
}

func TestAKeylessTaskStaysKeylessAcrossAttempts(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "worker")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, "", 3)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails and is requeued; attempt 2's lease expires and is
	// reaped. Both re-dispatch paths, one task.
	first, err := s.Claim(ctx, runnerID, 30*time.Second)
	if err != nil || first == nil {
		t.Fatalf("claim 1 = %+v, %v", first, err)
	}
	if _, err := s.Fail(ctx, first.ID, runnerID, "boom"); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(ctx, runnerID, 50*time.Millisecond)
	if err != nil || second == nil {
		t.Fatalf("claim 2 = %+v, %v", second, err)
	}
	time.Sleep(100 * time.Millisecond)
	third, err := s.Claim(ctx, runnerID, 30*time.Second)
	if err != nil || third == nil {
		t.Fatalf("claim 3 = %+v, %v", third, err)
	}
	if third.Attempt != 3 {
		t.Fatalf("attempt = %d, want 3", third.Attempt)
	}

	// Keyless must mean keyless every time. A re-dispatch that MINTED a key
	// would be worse than none: the sink would see a stable-looking header
	// that changed between attempts, which is the exact failure this row
	// exists to rule out.
	for i, lt := range []*store.Task{first, second, third} {
		if lt.IdempotencyKey != "" {
			t.Fatalf("attempt %d acquired key %q; keyless tasks must stay keyless", i+1, lt.IdempotencyKey)
		}
		// The runner falls back to the task id when the key is empty
		// (leaseloop.execute), so the id is what the sink actually sees here —
		// and it is the same id on every attempt by construction.
		if lt.ID != id {
			t.Fatalf("attempt %d id = %q, want %q", i+1, lt.ID, id)
		}
	}
}
