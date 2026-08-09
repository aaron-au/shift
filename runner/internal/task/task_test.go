package task

import (
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"
)

// add registers a waiting task with the given id, the shape the runner always
// creates (service.register).
func add(s *Store, id string) *Task {
	t := &Task{ID: id, Flow: "f", State: StateWaiting, Submitted: time.Now()}
	s.Add(t)
	return t
}

func ids(ts []Task) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.ID)
	}
	return out
}

// The ring is the runner's only task history, and it is unbounded input: a
// long-lived runner submits tasks forever. If eviction dropped the NEWEST
// entry the dashboard would freeze on the runner's first `limit` tasks and
// never show what is happening now — the failure mode a bounded buffer exists
// to prevent. Checked either side of the boundary, because off-by-one here is
// invisible at "add lots".
func TestTheOldestTaskIsEvictedOnceTheRingIsFull(t *testing.T) {
	const limit = 4
	for _, n := range []int{limit - 1, limit, limit + 1, limit + 3} {
		s := NewStore(limit)
		for i := range n {
			add(s, fmt.Sprintf("t%d", i))
		}

		kept := min(n, limit)
		if got := len(s.Recent(0)); got != kept {
			t.Fatalf("n=%d: ring holds %d tasks, want %d", n, got, kept)
		}
		// The newest is always present; the oldest survives only while the
		// ring has not overflowed.
		newest := fmt.Sprintf("t%d", n-1)
		if _, ok := s.Get(newest); !ok {
			t.Errorf("n=%d: newest task %s was evicted", n, newest)
		}
		for i := range n {
			id := fmt.Sprintf("t%d", i)
			_, ok := s.Get(id)
			wantKept := i >= n-kept
			if ok != wantKept {
				t.Errorf("n=%d: task %s present=%v, want %v", n, id, ok, wantKept)
			}
		}
	}
}

// The runner constructs the store from an operator-supplied history size
// (service.Options.TaskHistory), which is simply absent in most configs. A
// zero must not mean "keep nothing" — that would silently disable the
// dashboard and every task lookup the API does after a run.
func TestAnUnsetHistorySizeFallsBackToTheDefaultRing(t *testing.T) {
	for _, limit := range []int{0, -1} {
		s := NewStore(limit)
		for i := range 501 {
			add(s, fmt.Sprintf("t%d", i))
		}
		if got := len(s.Recent(0)); got != 500 {
			t.Errorf("NewStore(%d) kept %d tasks, want the 500 default", limit, got)
		}
		if _, ok := s.Get("t0"); ok {
			t.Errorf("NewStore(%d): task 501 did not evict the first task", limit)
		}
	}
}

// Get hands a task to callers that are outside the store's lock (the API
// handler, the lease loop's execution report). If it handed back the stored
// pointer, any of them could rewrite another component's view of a task — and
// would do so while a task goroutine mutates the same record under Update,
// which is a data race, not merely a surprise.
func TestGetReturnsACopyACallerCannotWriteBackThrough(t *testing.T) {
	s := NewStore(4)
	add(s, "a")
	s.Update("a", func(t *Task) {
		t.State = StateRunning
		t.RecordsIn = 7
		t.Ops = []OpStat{{Name: "source", RecordsIn: 7}}
	})

	got, ok := s.Get("a")
	if !ok {
		t.Fatal("task a missing")
	}
	got.State = StateFailed
	got.Error = "caller scribble"
	got.RecordsIn = 999
	got.Ops = []OpStat{{Name: "rewritten"}}

	stored, _ := s.Get("a")
	if stored.State != StateRunning || stored.Error != "" || stored.RecordsIn != 7 {
		t.Errorf("caller mutation reached the store: %+v", stored)
	}
	if len(stored.Ops) != 1 || stored.Ops[0].Name != "source" {
		t.Errorf("caller replacing Ops reached the store: %+v", stored.Ops)
	}
	// NOTE: the copy is shallow. Replacing a slice field (above) is safe, but
	// writing THROUGH one — got.Ops[0].Name = "x", or *got.Started — still
	// reaches the stored task. That is not asserted here in either direction:
	// it is a reported gap, not a guarantee to pin.

	if _, ok := s.Get("nope"); ok {
		t.Error("Get invented a task that was never added")
	}
}

// Recent feeds the dashboard's task list and the API. Newest-first is the
// order the code builds (it walks the ring backwards) and the only order that
// makes a bounded list useful — an operator watching a busy runner needs the
// task that just failed, not the oldest one still in the ring.
func TestRecentReturnsTheNewestTasksFirst(t *testing.T) {
	s := NewStore(10)
	for i := range 5 {
		add(s, fmt.Sprintf("t%d", i))
	}
	want := []string{"t4", "t3", "t2", "t1", "t0"}
	if got := ids(s.Recent(5)); !equal(got, want) {
		t.Errorf("Recent(5) = %v, want %v", got, want)
	}
	if got := ids(s.Recent(2)); !equal(got, []string{"t4", "t3"}) {
		t.Errorf("Recent(2) = %v, want the two newest", got)
	}
}

// n comes straight from a query string, so every degenerate value has to be
// answered with data rather than a panic or an empty list.
func TestRecentClampsTheRequestedCount(t *testing.T) {
	empty := NewStore(10)
	for _, n := range []int{-1, 0, 1, 100} {
		if got := empty.Recent(n); len(got) != 0 {
			t.Errorf("empty store: Recent(%d) = %v, want none", n, got)
		}
	}

	s := NewStore(10)
	for i := range 3 {
		add(s, fmt.Sprintf("t%d", i))
	}
	for _, n := range []int{-5, 0, 3, 50} {
		got := ids(s.Recent(n))
		if !equal(got, []string{"t2", "t1", "t0"}) {
			t.Errorf("Recent(%d) = %v, want all three newest-first", n, got)
		}
	}
}

// Same aliasing hazard as Get, on the path that is called most: the API
// serializes this slice with no lock held.
func TestRecentReturnsCopiesNotTheStoredTasks(t *testing.T) {
	s := NewStore(4)
	add(s, "a")

	got := s.Recent(1)
	got[0].State = StateFailed
	got[0].Error = "caller scribble"

	stored, _ := s.Get("a")
	if stored.State != StateWaiting || stored.Error != "" {
		t.Errorf("mutating a Recent entry reached the store: %+v", stored)
	}
}

// Update is driven from task goroutines that hold only an id. The task may be
// gone — evicted by newer work, or never registered because the submission was
// rejected — and there is no path for that goroutine to find out first. A
// no-op is the only safe answer; a panic here would take down a runner that is
// merely busy.
func TestUpdatingATaskTheStoreNoLongerHasIsANoOp(t *testing.T) {
	s := NewStore(2)
	add(s, "a")
	s.Update("a", func(t *Task) { t.State = StateRunning })
	add(s, "b")
	add(s, "c") // evicts a

	before := s.Totals()
	called := false
	s.Update("a", func(t *Task) { called = true; t.State = StateCompleted })
	s.Update("never-existed", func(t *Task) { called = true })
	if called {
		t.Error("Update ran the callback for a task the store does not hold")
	}
	if s.Totals() != before {
		t.Errorf("Update on a missing id moved the totals: %+v -> %+v", before, s.Totals())
	}
	// The evicted task was still Running, so its gauge contribution can now
	// never be retired — see the reported eviction/gauge gap. Asserted here
	// only as "Update does not touch totals for a task it cannot find", which
	// is Update's own contract either way.
}

// The totals are what the dashboard and the capacity story are read from, so
// they have to survive the exact transition sequence the runner drives:
// waiting -> running -> terminal, with RecordsIn folded in at the completing
// update (service sets the counter inside that same callback).
func TestTotalsFollowTheLifecycleTheRunnerDrives(t *testing.T) {
	s := NewStore(10)
	add(s, "ok")
	add(s, "bad")
	add(s, "stopped")
	add(s, "queued") // never leaves waiting

	if got := (Totals{Submitted: 4, Waiting: 4}); s.Totals() != got {
		t.Fatalf("after four submissions totals = %+v, want %+v", s.Totals(), got)
	}

	for _, id := range []string{"ok", "bad", "stopped"} {
		s.Update(id, func(t *Task) { t.State = StateRunning })
	}
	if tot := s.Totals(); tot.Waiting != 1 || tot.Running != 3 {
		t.Fatalf("after three admissions totals = %+v, want waiting 1 running 3", tot)
	}

	s.Update("ok", func(t *Task) { t.State = StateCompleted; t.RecordsIn = 10 })
	s.Update("bad", func(t *Task) { t.State = StateFailed; t.RecordsIn = 3; t.Error = "boom" })
	s.Update("stopped", func(t *Task) { t.State = StateCompleted; t.RecordsIn = 5; t.Stopped = true })

	want := Totals{
		Submitted: 4,
		Completed: 2, // a deliberate @stop IS a completion (ADR-0031 §3)
		Failed:    1,
		Stopped:   1, // ...and is counted again here, as a subset of Completed
		Waiting:   1,
		Running:   0,
		RecordsIn: 15, // only the two completions; the failed task's records are not counted
	}
	if got := s.Totals(); got != want {
		t.Errorf("totals = %+v, want %+v", got, want)
	}
	if got := s.Totals().Completed - s.Totals().Stopped; got != 1 {
		t.Errorf("Completed minus Stopped = %d, want 1 plain completion", got)
	}
}

// RecordsIn is folded into the running total by the state transition, not by
// the field write, so a counter set in a LATER update is silently dropped.
// Pinned because it is a live constraint on callers: the runner must set
// RecordsIn inside the same Update that marks the task completed.
func TestRecordsInIsCountedOnlyByTheCompletingUpdate(t *testing.T) {
	s := NewStore(10)
	add(s, "a")
	s.Update("a", func(t *Task) { t.State = StateCompleted })
	s.Update("a", func(t *Task) { t.RecordsIn = 100 }) // too late: state did not change

	if got := s.Totals().RecordsIn; got != 0 {
		t.Errorf("RecordsIn total = %d, want 0 — a post-completion write must not be counted", got)
	}
	if got := s.Totals().Completed; got != 1 {
		t.Errorf("Completed = %d, want 1 — a no-state-change update must not re-count", got)
	}
	// And the task's own record still carries the value, so the ring and the
	// lifetime counters can legitimately disagree.
	if tk, _ := s.Get("a"); tk.RecordsIn != 100 {
		t.Errorf("task RecordsIn = %d, want the value the caller wrote", tk.RecordsIn)
	}
}

// Totals are documented as lifetime counters while the ring is a fixed-size
// window, so the two must decouple: work done by an evicted task stays
// counted. If eviction unwound the counters, a busy runner's "completed" would
// drift down towards the ring size and understate everything it had done.
func TestEvictionDoesNotUnwindTheLifetimeTotals(t *testing.T) {
	s := NewStore(2)
	for i := range 5 {
		id := fmt.Sprintf("t%d", i)
		add(s, id)
		s.Update(id, func(t *Task) { t.State = StateRunning })
		s.Update(id, func(t *Task) { t.State = StateCompleted; t.RecordsIn = 2 })
	}

	want := Totals{Submitted: 5, Completed: 5, RecordsIn: 10}
	if got := s.Totals(); got != want {
		t.Errorf("totals after eviction = %+v, want %+v", got, want)
	}
	if got := len(s.Recent(0)); got != 2 {
		t.Errorf("ring holds %d tasks, want the 2 it is bounded to", got)
	}
}

// Every task goroutine updates its own record while the API reads the ring
// (ADR-0005: a task per goroutine, no fixed cap), so this is the store's
// actual usage. Run under -race, the value is the detector, not the asserts —
// but the counters are checked too, because a lost increment under contention
// is a silent, permanent miscount.
func TestTheStoreIsSafeUnderConcurrentUse(t *testing.T) {
	const workers, per = 8, 50
	s := NewStore(workers * per) // no eviction: every update finds its task

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				id := fmt.Sprintf("w%d-%d", w, i)
				add(s, id)
				s.Update(id, func(t *Task) { t.State = StateRunning })
				s.Get(id)
				s.Recent(5)
				s.Totals()
				s.Update(id, func(t *Task) { t.State = StateCompleted; t.RecordsIn = 1 })
			}
		}()
	}
	// Readers racing the writers, as the dashboard poll does.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range per {
				s.Recent(10)
				s.Totals()
				s.Get("w0-0")
			}
		}()
	}
	wg.Wait()

	want := Totals{Submitted: workers * per, Completed: workers * per, RecordsIn: workers * per}
	if got := s.Totals(); got != want {
		t.Errorf("totals = %+v, want %+v", got, want)
	}
}

// Concurrency while the ring is overflowing exercises the eviction path under
// contention — the one that mutates both the order slice and the map.
func TestConcurrentAddsOverflowingTheRingStaySelfConsistent(t *testing.T) {
	const workers, per, limit = 8, 50, 16
	s := NewStore(limit)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				add(s, fmt.Sprintf("w%d-%d", w, i))
				s.Recent(limit)
			}
		}()
	}
	wg.Wait()

	if got := s.Totals().Submitted; got != workers*per {
		t.Errorf("Submitted = %d, want %d", got, workers*per)
	}
	recent := s.Recent(0)
	if len(recent) != limit {
		t.Fatalf("ring holds %d tasks, want %d", len(recent), limit)
	}
	// Every entry the ring reports must still be resolvable by id: the order
	// slice and the map are two halves of one structure.
	seen := map[string]bool{}
	for _, tk := range recent {
		if seen[tk.ID] {
			t.Errorf("task %s listed twice", tk.ID)
		}
		seen[tk.ID] = true
		if _, ok := s.Get(tk.ID); !ok {
			t.Errorf("task %s listed by Recent but not gettable", tk.ID)
		}
	}
}

// The id is the only handle a task goroutine, the API and the hub's execution
// report share. A collision would merge two tenants' executions in the ring,
// so uniqueness matters more than the encoding — but the encoding is a
// contract too: it goes into URLs and log lines, so it must stay
// URL-safe hex of a full 16 bytes of entropy.
func TestNewIDIsUniqueAndFullEntropyHex(t *testing.T) {
	const n = 20000
	seen := make(map[string]bool, n)
	for range n {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
		if len(id) != 32 {
			t.Fatalf("id %q is %d chars, want 32 (16 random bytes)", id, len(id))
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("id %q is not hex: %v", id, err)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Waiting and Running are gauges — tasks in flight right now — while
// Submitted/Completed/Failed are lifetime counters. Evicting a task that never
// finished used to drop the record without retiring its gauge, and the Update
// that would have decremented then found nothing, so the count could only
// climb. On a long-lived runner that is a dashboard reading "37 running" on an
// idle box, which is worse than no number at all.
func TestEvictingAnUnfinishedTaskRetiresItsInFlightGauge(t *testing.T) {
	s := NewStore(2)
	add(s, "old-waiting")
	add(s, "old-running")
	s.Update("old-running", func(t *Task) { t.State = StateRunning })

	// Two more admissions evict both of the above while they are still in
	// flight — the case the runner hits whenever a task outlives `limit`
	// newer ones.
	add(s, "new-1")
	add(s, "new-2")

	got := s.Totals()
	if got.Waiting != 2 || got.Running != 0 {
		t.Errorf("in-flight gauges = waiting %d / running %d, want 2 / 0: only the two "+
			"tasks still in the ring are actually in flight", got.Waiting, got.Running)
	}
	if got.Submitted != 4 {
		t.Errorf("Submitted = %d, want 4: eviction must not unwind a lifetime counter", got.Submitted)
	}
}

// A task that has already finished must not be counted a second time under a
// different outcome. Both totals are monotonic, so a double count is permanent.
func TestATerminalTaskCannotBeReTerminated(t *testing.T) {
	s := NewStore(4)
	add(s, "t1")
	s.Update("t1", func(t *Task) { t.State = StateRunning })
	s.Update("t1", func(t *Task) { t.State = StateCompleted })
	s.Update("t1", func(t *Task) { t.State = StateFailed; t.Error = "late failure" })

	got := s.Totals()
	if got.Completed != 1 || got.Failed != 0 {
		t.Errorf("totals = completed %d / failed %d, want 1 / 0: a finished task counted "+
			"twice inflates both figures for the life of the process", got.Completed, got.Failed)
	}
	if task, ok := s.Get("t1"); !ok || task.State != StateCompleted {
		t.Errorf("state = %v, want the terminal state to stand", task.State)
	}
}
