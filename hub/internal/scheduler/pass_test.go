package scheduler

// Deterministic, pgtest-backed coverage of the firing loop. These tests
// drive pass()/Run() directly against a real Postgres (the store owns
// correctness, ADR-0012) and never spawn runners or the HTTP API — the
// e2e test (hub/e2e/schedule_test.go) exercises the full replica stack;
// this file locks in the same invariants without the process machinery.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

var flowDoc = json.RawMessage(`{"name":"tick",
  "source":{"connector":"gen","action":"gen","config":{"records":10}},
  "sink":{"connector":"gen","action":"discard"}}`)

// openStore opens + migrates a throwaway database.
func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// deployPublished deploys flowDoc under name and publishes it.
func deployPublished(t *testing.T, s *store.Store, name string) {
	t.Helper()
	v, err := s.DeployFlow(t.Context(), name, flowDoc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PublishFlow(t.Context(), name, v); err != nil {
		t.Fatal(err)
	}
}

// backdate (re)creates the flow's schedule at a fire time already in the
// past, so the next pass sees it due immediately.
func backdate(t *testing.T, s *store.Store, flow string, due time.Time) store.Schedule {
	t.Helper()
	sc, err := s.UpsertSchedule(t.Context(), flow, "* * * * *", true, 3, due)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// schedTaskCount counts queued tasks whose idempotency key marks them as
// schedule-fired.
func schedTaskCount(t *testing.T, s *store.Store) int {
	t.Helper()
	tasks, err := s.Tasks(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, task := range tasks {
		if strings.HasPrefix(task.IdempotencyKey, "sched:") {
			n++
		}
	}
	return n
}

func TestNewDefaults(t *testing.T) {
	// Zero options fall back to the documented defaults.
	s := New(nil, Options{})
	if s.opts.Interval != 5*time.Second {
		t.Errorf("default Interval = %v, want 5s", s.opts.Interval)
	}
	if s.opts.Batch != 50 {
		t.Errorf("default Batch = %d, want 50", s.opts.Batch)
	}
	// Negative values are treated as unset.
	s = New(nil, Options{Interval: -1, Batch: -1})
	if s.opts.Interval != 5*time.Second || s.opts.Batch != 50 {
		t.Errorf("negative options not defaulted: %+v", s.opts)
	}
	// Explicit values are preserved.
	s = New(nil, Options{Interval: 250 * time.Millisecond, Batch: 7})
	if s.opts.Interval != 250*time.Millisecond || s.opts.Batch != 7 {
		t.Errorf("explicit options overwritten: %+v", s.opts)
	}
	// Fresh scheduler reports an empty status.
	if got := s.Status(); got != (Status{}) {
		t.Errorf("initial Status = %+v, want zero", got)
	}
}

func TestPassFiresDuePublished(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{})
	deployPublished(t, s, "tick")
	schedule := backdate(t, s, "tick", time.Now().Add(-time.Second))

	sc.pass(t.Context())

	// Exactly one task, carrying the sched:<id>:<tick> idempotency key.
	if n := schedTaskCount(t, s); n != 1 {
		t.Fatalf("scheduled tasks after pass = %d, want 1", n)
	}
	got, err := s.GetSchedule(t.Context(), "tick")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastTaskID == "" || got.LastError != "" {
		t.Fatalf("post-fire bookkeeping = %+v", got)
	}
	// The tick advanced strictly into the future.
	if !got.NextFire.After(schedule.NextFire) {
		t.Fatalf("tick did not advance: %v -> %v", schedule.NextFire, got.NextFire)
	}
	task, err := s.GetTask(t.Context(), got.LastTaskID)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := "sched:" + schedule.ID + ":"
	if !strings.HasPrefix(task.IdempotencyKey, wantPrefix) {
		t.Fatalf("idempotency key %q missing prefix %q", task.IdempotencyKey, wantPrefix)
	}
	if task.State != "queued" {
		t.Fatalf("task state = %q, want queued", task.State)
	}

	// Status reflects the pass: one fired, no error, timestamped.
	st := sc.Status()
	if st.LastFired != 1 || st.LastError != "" || st.LastPass.IsZero() {
		t.Fatalf("Status = %+v", st)
	}
}

func TestPassExactlyOnceSameTick(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{})
	deployPublished(t, s, "tick")

	// A fixed due time makes both passes target the identical tick, so the
	// second must collapse onto the first via the sched:<id>:<tick>
	// idempotency key — no duplicate task.
	due := time.Now().Add(-time.Second)
	backdate(t, s, "tick", due)
	sc.pass(t.Context())
	if n := schedTaskCount(t, s); n != 1 {
		t.Fatalf("after first pass = %d tasks, want 1", n)
	}

	// Re-arm the SAME tick (same stored next_fire_at) and fire again.
	backdate(t, s, "tick", due)
	sc.pass(t.Context())
	if n := schedTaskCount(t, s); n != 1 {
		t.Fatalf("exactly-once violated: %d tasks after re-firing same tick, want 1", n)
	}
}

func TestPassNotDueNoFire(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{})
	deployPublished(t, s, "tick")

	// Fire time in the future — the pass must not fire it.
	if _, err := s.UpsertSchedule(t.Context(), "tick", "* * * * *", true, 3, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	sc.pass(t.Context())
	if n := schedTaskCount(t, s); n != 0 {
		t.Fatalf("not-due schedule fired: %d tasks", n)
	}
	if st := sc.Status(); st.LastFired != 0 || st.LastError != "" {
		t.Fatalf("Status = %+v", st)
	}
}

func TestPassDisabledNoFire(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{})
	deployPublished(t, s, "tick")

	// Backdated but disabled: never fires.
	if _, err := s.UpsertSchedule(t.Context(), "tick", "* * * * *", false, 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	sc.pass(t.Context())
	if n := schedTaskCount(t, s); n != 0 {
		t.Fatalf("disabled schedule fired: %d tasks", n)
	}
}

func TestPassUnpublishedParks(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{})

	// Deploy a draft but never publish it.
	if _, err := s.DeployFlow(t.Context(), "draft", flowDoc); err != nil {
		t.Fatal(err)
	}
	backdate(t, s, "draft", time.Now().Add(-time.Second))

	sc.pass(t.Context())

	// No task, schedule parked with an explanatory error, tick advanced so
	// the pass never wedges on it.
	if n := schedTaskCount(t, s); n != 0 {
		t.Fatalf("unpublished flow fired: %d tasks", n)
	}
	got, err := s.GetSchedule(t.Context(), "draft")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastError == "" || got.LastTaskID != "" {
		t.Fatalf("parked schedule = %+v", got)
	}
	if !got.NextFire.After(time.Now()) {
		t.Fatalf("parked schedule did not advance: %+v", got)
	}
	// Parking is not a pass-level error — the loop stays healthy.
	if st := sc.Status(); st.LastError != "" || st.LastFired != 0 {
		t.Fatalf("Status = %+v", st)
	}
}

func TestPassBatchDrains(t *testing.T) {
	s := openStore(t)
	// Small batch forces the drain loop (fire Batch, follow up until a
	// short pass) to run several iterations.
	sc := New(s, Options{Batch: 2})

	const n = 5
	names := make([]string, n)
	due := time.Now().Add(-time.Second)
	for i := range n {
		name := "flow-" + string(rune('a'+i))
		names[i] = name
		deployPublished(t, s, name)
		backdate(t, s, name, due)
	}

	sc.pass(t.Context())

	if got := schedTaskCount(t, s); got != n {
		t.Fatalf("batch drain fired %d tasks, want %d", got, n)
	}
	// The pass total (sum across drain iterations) is reported.
	if st := sc.Status(); st.LastFired != n {
		t.Fatalf("Status.LastFired = %d, want %d", st.LastFired, n)
	}
	// Every schedule advanced and recorded a task.
	for _, name := range names {
		got, err := s.GetSchedule(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if got.LastTaskID == "" || got.LastError != "" {
			t.Fatalf("schedule %s = %+v", name, got)
		}
	}
}

// TestPassConcurrentExactlyOnce runs two independent stores ("replicas")
// hammering pass() over the same database. The advisory lock + SKIP
// LOCKED + idempotency key must collapse every tick to exactly one task,
// race-clean, regardless of interleaving.
func TestPassConcurrentExactlyOnce(t *testing.T) {
	dsn := pgtest.DSN(t)
	open := func(migrate bool) *store.Store {
		st, err := store.Open(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(st.Close)
		if migrate {
			if err := st.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		return st
	}
	s1 := open(true)
	s2 := open(false)

	const n = 12
	due := time.Now().Add(-time.Second)
	for i := range n {
		name := "flow-" + string(rune('a'+i))
		deployPublished(t, s1, name)
		backdate(t, s1, name, due)
	}

	sc1 := New(s1, Options{Batch: 3})
	sc2 := New(s2, Options{Batch: 3})

	var wg sync.WaitGroup
	fire := func(sc *Scheduler) {
		defer wg.Done()
		for range 20 {
			sc.pass(t.Context())
		}
	}
	wg.Add(2)
	go fire(sc1)
	go fire(sc2)
	wg.Wait()

	if got := schedTaskCount(t, s1); got != n {
		t.Fatalf("exactly-once violated: %d scheduled tasks for %d schedules", got, n)
	}
	// No duplicate idempotency keys survived the race.
	tasks, err := s1.Tasks(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, task := range tasks {
		if !strings.HasPrefix(task.IdempotencyKey, "sched:") {
			continue
		}
		if seen[task.IdempotencyKey] {
			t.Fatalf("duplicate idempotency key %q", task.IdempotencyKey)
		}
		seen[task.IdempotencyKey] = true
	}
}

// TestRunFiresAndStops covers the ticker-driven loop and clean shutdown.
func TestRunFiresAndStops(t *testing.T) {
	s := openStore(t)
	sc := New(s, Options{Interval: 20 * time.Millisecond})
	deployPublished(t, s, "tick")
	backdate(t, s, "tick", time.Now().Add(-time.Second))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// Wait for the loop to fire the backdated schedule.
	deadline := time.Now().Add(5 * time.Second)
	for schedTaskCount(t, s) == 0 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("Run did not fire the due schedule")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel returns the loop promptly.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if st := sc.Status(); st.LastPass.IsZero() {
		t.Fatalf("Status not recorded after Run: %+v", st)
	}
}
