package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// registerRunner mints a token and registers one runner, returning its id.
func newRunner(t *testing.T, s *store.Store, name string) string {
	t.Helper()
	token, _, err := s.CreateRegistrationToken(t.Context(), time.Hour)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	id, _, err := s.RegisterRunnerCert(t.Context(), token, name)
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return id
}

// A test runner takes test-marked work ONLY (ADR-0048 §3). The tier is read
// from the roster, never from anything the runner sends — a runner that could
// name its own tier could be handed work it should not see.
func TestATestRunnerClaimsOnlyTestWork(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	v := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("publish: %v", err)
	}

	prod := newRunner(t, s, "prod-1")
	test := newRunner(t, s, "test-1")
	if err := s.SetRunnerTier(ctx, test, store.TierTest); err != nil {
		t.Fatalf("set tier: %v", err)
	}

	// Only production work on the queue: the test runner must find nothing.
	if _, err := s.Enqueue(ctx, "orders", 0, "prod-a", 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.Claim(ctx, test, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got != nil {
		t.Fatalf("a test runner claimed production work (%s); it would run against live systems", got.ID)
	}

	// The production runner takes it.
	got, err = s.Claim(ctx, prod, time.Minute)
	if err != nil || got == nil {
		t.Fatalf("production claim = %v, %v", got, err)
	}
	if got.Test {
		t.Fatal("a production enqueue was marked test")
	}

	// Now a test-marked task: the test runner takes it.
	if _, err := s.EnqueueTest(ctx, "orders", 0, "test-a", 1); err != nil {
		t.Fatalf("enqueue test: %v", err)
	}
	got, err = s.Claim(ctx, test, time.Minute)
	if err != nil || got == nil {
		t.Fatalf("test claim = %v, %v", got, err)
	}
	if !got.Test {
		t.Fatal("the claimed task is not marked test")
	}
}

// The converse is deliberately NOT true: test-marked work may also run on a
// production runner. Forbidding it would mean run-now stops working entirely
// in every deployment that has not registered a test runner — turning an
// additive capability into a breaking change.
func TestProductionRunnersMayStillTakeTestWork(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	v := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	prod := newRunner(t, s, "prod-1") // the only runner: no test capacity deployed

	if _, err := s.EnqueueTest(ctx, "orders", 0, "test-a", 1); err != nil {
		t.Fatalf("enqueue test: %v", err)
	}
	got, err := s.Claim(ctx, prod, time.Minute)
	if err != nil || got == nil {
		t.Fatalf("claim = %v, %v; a hub with no test runners could not run a test at all", got, err)
	}
	// It stays MARKED, so metering still knows what it was.
	if !got.Test {
		t.Fatal("the test marker was lost when a production runner claimed it")
	}
}

// A runner registers as production. Test capacity is something an
// administrator grants, not something a runner arrives with.
func TestRunnersRegisterAsProduction(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newRunner(t, s, "r1")

	tier, err := s.RunnerTier(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if tier != store.TierProduction {
		t.Fatalf("a fresh runner is %q, want production", tier)
	}
	rs, err := s.Runners(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].Tier != store.TierProduction {
		t.Fatalf("roster = %+v", rs)
	}

	if err := s.SetRunnerTier(ctx, id, store.TierTest); err != nil {
		t.Fatalf("set tier: %v", err)
	}
	if tier, _ := s.RunnerTier(ctx, id); tier != store.TierTest {
		t.Fatalf("tier = %q after being set to test", tier)
	}
	// Only the two tiers exist; an unknown one is refused rather than stored,
	// or "tier" stops meaning anything on the claim path.
	if err := s.SetRunnerTier(ctx, id, "staging"); err == nil {
		t.Fatal("an unknown tier was accepted")
	}
	if err := s.SetRunnerTier(ctx, "00000000-0000-0000-0000-000000000000", store.TierTest); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("tier on a missing runner = %v, want ErrNotFound", err)
	}
}

// A scheduled flow is never test-marked. A schedule firing onto test capacity
// is a production flow metered wrong (ADR-0048 §3) — and this asserts the path
// that does the enqueueing, not just the API in front of it.
func TestScheduledWorkIsNeverTestMarked(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	v := deploy(t, s, "nightly", `{
	  "name": "nightly",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "nightly", v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Due in the past so the very next tick fires it.
	if _, err := s.UpsertSchedule(ctx, "nightly", "* * * * *", true, 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if _, err := s.FireDue(ctx, minuteLater, 50); err != nil {
		t.Fatalf("fire: %v", err)
	}

	test := newRunner(t, s, "test-1")
	if err := s.SetRunnerTier(ctx, test, store.TierTest); err != nil {
		t.Fatalf("set tier: %v", err)
	}
	got, err := s.Claim(ctx, test, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got != nil {
		t.Fatal("a test runner claimed scheduled work; schedules must never be test-marked")
	}
}
