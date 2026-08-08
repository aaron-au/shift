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

// Test executions are metered SEPARATELY and excluded from billing
// (ADR-0048 §4). There is no quota on test usage, so the measurement is the
// whole control — a billable total that silently included test runs would
// invoice somebody for trying something out.
func TestTestExecutionsAreMeteredApartFromBillable(t *testing.T) {
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
	runner := newRunner(t, s, "r1")

	// Two production runs and one test run, all completing.
	run := func(test bool, key string) {
		t.Helper()
		enqueue := s.Enqueue
		if test {
			enqueue = s.EnqueueTest
		}
		if _, err := enqueue(ctx, "orders", 0, key, 1); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		claimed, err := s.Claim(ctx, runner, time.Minute)
		if err != nil || claimed == nil {
			t.Fatalf("claim: %v, %v", claimed, err)
		}
		if err := s.Complete(ctx, claimed.ID, runner,
			[]byte(`{"records_in":10,"records_out":10}`)); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}
	run(false, "p1")
	run(false, "p2")
	run(true, "t1")

	rep, err := s.Usage(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Totals.Executions != 2 {
		t.Fatalf("billable executions = %d, want 2 — a test run was billed", rep.Totals.Executions)
	}
	if rep.Totals.RecordsIn != 20 {
		t.Fatalf("billable records = %d, want 20 (test records must not count)", rep.Totals.RecordsIn)
	}
	if rep.Test.Executions != 1 {
		t.Fatalf("test executions = %d, want 1 — test usage must be VISIBLE, not just excluded", rep.Test.Executions)
	}
	if rep.Test.RecordsIn != 10 {
		t.Fatalf("test records = %d, want 10", rep.Test.RecordsIn)
	}

	// Per flow, both are visible: "who is hammering test mode" is the question
	// the visibility exists to answer, and an account total cannot answer it.
	if len(rep.ByFlow) != 1 {
		t.Fatalf("by_flow = %+v", rep.ByFlow)
	}
	if rep.ByFlow[0].Executions != 2 || rep.ByFlow[0].TestExecutions != 1 {
		t.Fatalf("by_flow = %+v, want 2 billable and 1 test", rep.ByFlow[0])
	}

	// The export carries the flag per ROW rather than dropping test rows: the
	// billing platform must be able to classify what it receives, and dropping
	// them would hide the usage this exists to surface.
	events, err := s.UsageEventsSince(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("exported %d events, want all 3", len(events))
	}
	marked := 0
	for _, e := range events {
		if e.Test {
			marked++
		}
	}
	if marked != 1 {
		t.Fatalf("%d exported events are test-marked, want exactly 1", marked)
	}
}
