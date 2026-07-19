package store_test

import (
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, secret := registerRunner(t, s, "runner-a")
	// AuthRunner stamps last_seen_at — that is what "active" tracks.
	if _, _, err := s.AuthRunner(ctx, secret); err != nil {
		t.Fatal(err)
	}
	deployPublished(t, s, "orders")

	// Two queued, one leased.
	for range 3 {
		if _, err := s.Enqueue(ctx, "orders", 0, "", 3); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Claim(ctx, runnerID, time.Minute); err != nil {
		t.Fatal(err)
	}

	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Tasks["queued"] != 2 || st.Tasks["leased"] != 1 {
		t.Fatalf("task counts = %v", st.Tasks)
	}
	if st.Flows != 1 {
		t.Fatalf("flows = %d, want 1", st.Flows)
	}
	if st.RunnersTotal != 1 || st.RunnersActive != 1 {
		t.Fatalf("runners = %d/%d", st.RunnersActive, st.RunnersTotal)
	}
	if st.OldestQueuedSec < 0 {
		t.Fatalf("oldest queued = %v", st.OldestQueuedSec)
	}

	// A schedule shows in the counts.
	if _, err := s.UpsertSchedule(ctx, "orders", "* * * * *", true, 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	st, err = s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Schedules != 1 || st.SchedulesDue != 1 {
		t.Fatalf("schedules = %d due %d", st.Schedules, st.SchedulesDue)
	}
}
