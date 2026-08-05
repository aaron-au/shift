package store_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// The property resume exists for: the runner that recorded a position is
// usually NOT the runner that resumes from it, because runners are
// replaceable by design. The cursor must survive the handover.
func TestCheckpointSurvivesRedispatchToAnotherRunner(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	first, _ := registerRunner(t, s, "first")
	second, _ := registerRunner(t, s, "second")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, "", 3)
	if err != nil {
		t.Fatal(err)
	}

	lt, _ := s.Claim(ctx, first, 30*time.Second)
	if lt == nil || lt.ID != id {
		t.Fatalf("claim = %+v", lt)
	}
	if len(lt.Checkpoint) != 0 {
		t.Fatalf("first attempt carried a cursor: %q", lt.Checkpoint)
	}

	cur := []byte(`{"v":1,"n":420}`)
	if err := s.HeartbeatWithCheckpoint(ctx, id, first, 30*time.Second,
		store.Checkpoint{Cursor: cur, Connector: "fs", Version: "0.2.0"}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// The first runner dies; the task is requeued and a DIFFERENT runner takes it.
	if _, err := s.Fail(ctx, id, first, "runner died"); err != nil {
		t.Fatal(err)
	}
	lt2, _ := s.Claim(ctx, second, 30*time.Second)
	if lt2 == nil {
		t.Fatal("re-dispatch produced no task")
	}
	if !bytes.Equal(lt2.Checkpoint, cur) {
		t.Fatalf("checkpoint = %q, want %q — the replacement runner must resume where the first got to", lt2.Checkpoint, cur)
	}
	if lt2.CheckpointConnector != "fs" || lt2.CheckpointVersion != "0.2.0" {
		t.Fatalf("identity = %q/%q, want fs/0.2.0 — a cursor is only readable by the build that produced it",
			lt2.CheckpointConnector, lt2.CheckpointVersion)
	}
}

// A heartbeat with nothing new to say must not erase real progress: a
// resumable source reports positions only at safe points, so most heartbeats
// legitimately carry none.
func TestEmptyCheckpointDoesNotEraseStoredProgress(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "runner")

	deployPublished(t, s, "orders")
	id, _ := s.Enqueue(ctx, "orders", 0, "", 3)
	if _, err := s.Claim(ctx, runnerID, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	cur := []byte("position-1")
	if err := s.HeartbeatWithCheckpoint(ctx, id, runnerID, 30*time.Second,
		store.Checkpoint{Cursor: cur, Connector: "fs", Version: "0.2.0"}); err != nil {
		t.Fatal(err)
	}
	// A plain heartbeat, and one with an explicitly empty cursor.
	if err := s.Heartbeat(ctx, id, runnerID, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.HeartbeatWithCheckpoint(ctx, id, runnerID, 30*time.Second, store.Checkpoint{}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Fail(ctx, id, runnerID, "died"); err != nil {
		t.Fatal(err)
	}
	lt, _ := s.Claim(ctx, runnerID, 30*time.Second)
	if !bytes.Equal(lt.Checkpoint, cur) {
		t.Fatalf("checkpoint = %q, want %q retained across cursor-free heartbeats", lt.Checkpoint, cur)
	}
	if lt.CheckpointConnector != "fs" {
		t.Fatalf("connector = %q, want it retained too", lt.CheckpointConnector)
	}
}

// A runner whose lease expired has been superseded. Letting it record a
// position would let a zombie rewind the attempt that replaced it, so the
// cursor write is gated by exactly the same lease check as the heartbeat.
func TestCheckpointFromAnExpiredLeaseIsRejected(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	dead, _ := registerRunner(t, s, "dead")
	live, _ := registerRunner(t, s, "live")

	deployPublished(t, s, "orders")
	id, _ := s.Enqueue(ctx, "orders", 0, "", 3)
	if _, err := s.Claim(ctx, dead, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// The lease lapses; the next claim reaps it and re-dispatches (reap-at-claim).
	time.Sleep(100 * time.Millisecond)
	lt, _ := s.Claim(ctx, live, 30*time.Second)
	if lt == nil {
		t.Fatal("re-dispatch produced no task")
	}

	err := s.HeartbeatWithCheckpoint(ctx, id, dead, 30*time.Second,
		store.Checkpoint{Cursor: []byte("stale"), Connector: "fs", Version: "0.2.0"})
	if err == nil {
		t.Fatal("a zombie runner recorded a checkpoint over a live attempt")
	}

	// And the live attempt's view is untouched.
	if err := s.HeartbeatWithCheckpoint(ctx, id, live, 30*time.Second,
		store.Checkpoint{Cursor: []byte("good"), Connector: "fs", Version: "0.2.0"}); err != nil {
		t.Fatalf("live heartbeat: %v", err)
	}
}
