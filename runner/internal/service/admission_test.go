package service

import (
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/task"
)

// Admission is governed by real resource signals and NEVER by a fixed
// task-count cap (ADR-0005, CLAUDE.md: "a task must never wait on another task
// unless the machine is genuinely out of resources").
//
// TestAdmissionSerializesWhenBudgetIsOneTask already proves the pressure half —
// that a one-task budget serializes. It is the free half that carries the
// doctrine, and it was the half nothing asserted: a regression to a hardcoded
// `maxConcurrent = 4` in the admission loop passes every other test in this
// package unchanged, because no test has ever required more than a handful of
// tasks to be resident at once.
//
// The test below requires it. The tasks block on a shared barrier that only
// releases once ALL of them have arrived, so a serializing admission path can
// never satisfy it — it fails on the deadline rather than on a comparison, and
// the peak assertion pins the exact number that were resident together.

// barrier holds every arriving task until n of them are resident, then lets
// them all go. It records the peak residency so the test asserts the observed
// concurrency rather than merely that everything eventually finished.
type barrier struct {
	mu       sync.Mutex
	inFlight int
	peak     int

	arrived     chan struct{} // one token per task that reached the barrier
	release     chan struct{} // closed once; frees every waiter
	releaseOnce sync.Once
}

func newBarrier(n int) *barrier {
	return &barrier{arrived: make(chan struct{}, n), release: make(chan struct{})}
}

func (b *barrier) wait() {
	b.mu.Lock()
	b.inFlight++
	if b.inFlight > b.peak {
		b.peak = b.inFlight
	}
	b.mu.Unlock()
	b.arrived <- struct{}{}
	<-b.release
	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
}

func (b *barrier) releaseAll() { b.releaseOnce.Do(func() { close(b.release) }) }

func (b *barrier) peakSeen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

// barrierWriter is one task's @response destination. The sink's writer is the
// only injectable I/O on a built-in-only flow, which makes it the place to
// hold a task INSIDE its admission reservation — a task parked here still
// holds the budget it reserved, so anything gating on a count cannot let the
// next one in.
type barrierWriter struct {
	b    *barrier
	once sync.Once
}

func (w *barrierWriter) Write(p []byte) (int, error) {
	w.once.Do(w.b.wait) // block once per task; later flushes pass straight through
	return len(p), nil
}

// TestManyTasksRunConcurrentlyWhenMemoryIsFree: with a budget generous enough
// for all of them, N tasks are admitted and resident SIMULTANEOUSLY. N is set
// well above any plausible hidden cap (and above the default budget's own
// arithmetic, 1 GiB / 80 MiB = 12 tasks) so that a cap of 4, 8 or 16 is caught.
func TestManyTasksRunConcurrentlyWhenMemoryIsFree(t *testing.T) {
	const tasks = 40

	// Explicit, tiny per-task costs. The governor ACCOUNTS for bytes, it does
	// not allocate them, so the numbers only have to make the admission
	// arithmetic (budget / taskCost) unambiguous: 40 tasks cost 80 MiB against
	// a 320 MiB budget, leaving 4x headroom. Nothing here is memory-bound, so
	// a task that waits is waiting on another task.
	opts := Options{TaskWatermark: 1 << 20, TaskOverhead: 1 << 20}
	opts.MemBudget = int64(tasks) * (opts.TaskWatermark + opts.TaskOverhead) * 4
	svc := newBuiltinService(t, opts)

	if max := opts.MemBudget / svc.taskCost(); max < tasks {
		t.Fatalf("budget admits %d tasks, need at least %d", max, tasks)
	}

	b := newBarrier(tasks)
	// However the test exits, waiters must be freed or the service Close in the
	// cleanup would block on them for its full drain timeout.
	t.Cleanup(b.releaseAll)

	doc := hookDoc("admission-concurrency", nil, flowdoc.ResponseSink)
	body := ndjsonBody(`{"n":1}`)

	ids := make([]string, 0, tasks)
	for range tasks {
		id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body, Response: &barrierWriter{b: b}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Wait for every task to reach the barrier. Serialized admission cannot get
	// past the first, so this is where a count cap fails the test.
	deadline := time.After(30 * time.Second)
	for got := range tasks {
		select {
		case <-b.arrived:
		case <-deadline:
			t.Fatalf("only %d of %d tasks reached the barrier together — admission is gating on a count, not on resources (peak residency %d)",
				got, tasks, b.peakSeen())
		}
	}
	// Every task is parked in the barrier right now: none can have decremented,
	// because nothing has been released yet.
	if peak := b.peakSeen(); peak != tasks {
		t.Errorf("peak concurrent tasks = %d, want %d", peak, tasks)
	}

	b.releaseAll()
	for _, id := range ids {
		if tk := awaitTerminal(t, svc, id); tk.State != task.StateCompleted {
			t.Fatalf("task %s: state = %s: %s", id, tk.State, tk.Error)
		}
	}
}
