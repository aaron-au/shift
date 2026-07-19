package store_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// minuteLater is a trivial nextFn: one tick per pass, one minute apart.
func minuteLater(_ string, after time.Time) (time.Time, error) {
	return after.Add(time.Minute), nil
}

func TestScheduleCRUD(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")

	next := time.Now().Add(time.Hour)
	sc, err := s.UpsertSchedule(ctx, "orders", "*/5 * * * *", true, 2, next)
	if err != nil || sc.Cron != "*/5 * * * *" || !sc.Enabled {
		t.Fatalf("upsert = %+v, %v", sc, err)
	}
	// Replace (same flow) keeps one schedule per flow.
	if _, err := s.UpsertSchedule(ctx, "orders", "0 * * * *", false, 1, next); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSchedule(ctx, "orders")
	if err != nil || got.Cron != "0 * * * *" || got.Enabled {
		t.Fatalf("get = %+v, %v", got, err)
	}
	list, err := s.Schedules(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := s.UpsertSchedule(ctx, "ghost", "* * * * *", true, 1, next); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("upsert unknown flow: %v", err)
	}
	if err := s.DeleteSchedule(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, "orders"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}

func TestFireDueSingle(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")

	due := time.Now().Add(-time.Second)
	sc, err := s.UpsertSchedule(ctx, "orders", "* * * * *", true, 3, due)
	if err != nil {
		t.Fatal(err)
	}

	fired, err := s.FireDue(ctx, minuteLater, 50)
	if err != nil || fired != 1 {
		t.Fatalf("FireDue = %d, %v (want 1)", fired, err)
	}
	// The task exists, carries the schedule-derived idempotency key, and
	// the schedule advanced with bookkeeping.
	got, err := s.GetSchedule(ctx, "orders")
	if err != nil || got.LastTaskID == "" || got.LastError != "" || !got.NextFire.After(due) {
		t.Fatalf("post-fire schedule = %+v, %v", got, err)
	}
	task, err := s.GetTask(ctx, got.LastTaskID)
	if err != nil || task.State != "queued" {
		t.Fatalf("task = %+v, %v", task, err)
	}
	wantPrefix := "sched:" + sc.ID + ":"
	if !strings.HasPrefix(task.IdempotencyKey, wantPrefix) {
		t.Fatalf("idempotency key %q missing prefix %q", task.IdempotencyKey, wantPrefix)
	}

	// Not due again: nothing fires.
	if fired, err := s.FireDue(ctx, minuteLater, 50); err != nil || fired != 0 {
		t.Fatalf("second FireDue = %d, %v (want 0)", fired, err)
	}
}

func TestFireDueUnpublishedParks(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	// Draft only — publish, schedule, then roll the flow back to no
	// published version via a fresh flow that never publishes.
	if _, err := s.DeployFlow(ctx, "draft-flow", flowDoc); err != nil {
		t.Fatal(err)
	}
	// UpsertSchedule requires only that the flow exists; simulate a
	// schedule created before publish was revoked.
	if _, err := s.UpsertSchedule(ctx, "draft-flow", "* * * * *", true, 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	fired, err := s.FireDue(ctx, minuteLater, 50)
	if err != nil || fired != 0 {
		t.Fatalf("FireDue = %d, %v (want 0)", fired, err)
	}
	got, err := s.GetSchedule(ctx, "draft-flow")
	if err != nil || got.LastError == "" || got.LastTaskID != "" {
		t.Fatalf("parked schedule = %+v, %v", got, err)
	}
	// It advanced — no wedged hot loop on a broken schedule.
	if !got.NextFire.After(time.Now()) {
		t.Fatalf("parked schedule did not advance: %+v", got)
	}
}

// TestFireDueExactlyOnceUnderContention is the store-layer proof of the
// exit criterion: two stores (two "hub replicas") hammer FireDue over
// the same DB; every due schedule yields exactly one task.
func TestFireDueExactlyOnceUnderContention(t *testing.T) {
	dsn := pgtest.DSN(t)
	s1, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s1.Close)
	if err := s1.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.Close)

	ctx := t.Context()
	const n = 20
	for i := range n {
		name := "orders-" + string(rune('a'+i))
		v, err := s1.DeployFlow(ctx, name, flowDoc)
		if err != nil {
			t.Fatal(err)
		}
		if err := s1.PublishFlow(ctx, name, v); err != nil {
			t.Fatal(err)
		}
		if _, err := s1.UpsertSchedule(ctx, name, "* * * * *", true, 3, time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	// Both replicas hammer FireDue concurrently until the queue is
	// drained. A fixed pass count would be flaky: pg_try_advisory_xact_lock
	// fails fast, so the replica that loses the lock burns iterations
	// doing nothing while the holder works — the loop must be bounded by
	// "drained", not by count. The invariant under test is that the race
	// never double-fires a tick (asserted below), regardless of interleaving.
	deadline := time.Now().Add(10 * time.Second)
	var wg sync.WaitGroup
	fire := func(s *store.Store) {
		defer wg.Done()
		for time.Now().Before(deadline) {
			if _, err := s.FireDue(ctx, minuteLater, 7); err != nil {
				t.Errorf("FireDue: %v", err)
				return
			}
			tasks, err := s.Tasks(ctx, 500)
			if err != nil {
				t.Errorf("Tasks: %v", err)
				return
			}
			if len(tasks) >= n {
				return
			}
		}
		t.Error("FireDue did not drain all schedules before deadline")
	}
	wg.Add(2)
	go fire(s1)
	go fire(s2)
	wg.Wait()

	tasks, err := s1.Tasks(ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != n {
		t.Fatalf("exactly-once violated: %d tasks for %d due schedules", len(tasks), n)
	}
	seen := map[string]bool{}
	for _, task := range tasks {
		if !strings.HasPrefix(task.IdempotencyKey, "sched:") {
			t.Fatalf("task %s has key %q", task.ID, task.IdempotencyKey)
		}
		if seen[task.IdempotencyKey] {
			t.Fatalf("duplicate idempotency key %q", task.IdempotencyKey)
		}
		seen[task.IdempotencyKey] = true
	}
}
