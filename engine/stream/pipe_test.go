package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Pipe is what makes nested and mixed topologies executable (issue #59): it
// gives a fan-out branch a Sink to end at and gives the merge downstream a
// Source to read, with both stages running concurrently. The tests below are
// about the three properties that make that safe.

// writeStrings pushes each string as a one-record batch through a sink, reusing
// ONE batch throughout — which is the batch-lifetime contract a real source
// obeys, and the thing a naive pipe would get wrong.
func writeStrings(t *testing.T, sink Sink, ss ...string) {
	t.Helper()
	b := record.NewBatch()
	for _, s := range ss {
		b.Reset()
		bld := b.Builder()
		bld.BeginMap()
		bld.KeyLiteral("s")
		bld.StringLiteral(s)
		bld.EndMap()
		b.Append(bld.Finish())
		if err := sink.Write(context.Background(), b); err != nil {
			t.Fatalf("write %q: %v", s, err)
		}
	}
}

// drain reads a source to EOF and returns the "s" field of every record.
func drain(t *testing.T, src Source) []string {
	t.Helper()
	var got []string
	for {
		b, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, r := range b.Records() {
			f, _ := r.Field("s")
			got = append(got, f.String())
		}
	}
}

// The batch-lifetime rule, which is the whole reason the pipe copies: a batch
// handed to Write is valid only until the writer's next Next. A pipe that
// queued the caller's batch by reference would hand the reader whatever the
// writer overwrote it with — every record identical to the last one written,
// with nothing failing to say so.
func TestAPipeCopiesBecauseTheWritersBatchIsReused(t *testing.T) {
	p := NewPipe(8) // deep enough that the writer never blocks
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		got = drain(t, p.Source())
	}()

	writeStrings(t, p.Sink(), "a", "b", "c")
	_ = p.Sink().Close()
	<-done

	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("read %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("read %v, want %v — the pipe queued the writer's batch by reference", got, want)
		}
	}
}

// The queue is bounded, so a slow reader makes the writer wait rather than
// letting a stage buffer a whole stream in memory. That is flow control within
// one task (ADR-0005), not a gate between tasks.
func TestAFullPipeBlocksTheWriter(t *testing.T) {
	p := NewPipe(1)
	src := p.Source()

	blocked := make(chan struct{})
	go func() {
		// One batch fills the queue; the second must not be accepted until
		// the reader takes the first.
		writeStrings(t, p.Sink(), "a", "b")
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("a depth-1 pipe accepted two batches without a read: it is not bounded")
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := src.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the writer did not resume after the reader drained a batch")
	}
	_ = p.Sink().Close()
	_ = src.Close()
}

// The no-deadlock guarantee. If the reader stops — its pipeline failed, or
// something downstream errored — a writer blocked on a full queue would
// otherwise hang forever, and the caller joining that stage would hang with
// it. This is the difference between a failing flow and a wedged runner.
func TestClosingTheReaderReleasesABlockedWriter(t *testing.T) {
	p := NewPipe(1)
	src := p.Source()

	errc := make(chan error, 1)
	go func() {
		b := record.NewBatch()
		bld := b.Builder()
		bld.BeginMap()
		bld.KeyLiteral("s")
		bld.StringLiteral("x")
		bld.EndMap()
		b.Append(bld.Finish())
		// The first write fills the queue; the second blocks.
		for {
			if err := p.Sink().Write(context.Background(), b); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Let the writer reach its blocked state, then walk away.
	time.Sleep(50 * time.Millisecond)
	_ = src.Close()

	select {
	case err := <-errc:
		if !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("blocked writer released with %v, want ErrPipeClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing the reader did not release the blocked writer: the producing stage would hang forever")
	}
}

// A cancelled context releases a blocked writer too — task cancellation must
// reach a stage that is waiting on a queue, not just one that is computing.
func TestACancelledContextReleasesABlockedWriter(t *testing.T) {
	p := NewPipe(1)
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() {
		b := record.NewBatch()
		bld := b.Builder()
		bld.BeginMap()
		bld.KeyLiteral("s")
		bld.StringLiteral("x")
		bld.EndMap()
		b.Append(bld.Finish())
		for {
			if err := p.Sink().Write(ctx, b); err != nil {
				errc <- err
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked writer released with %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not reach a writer blocked on a full queue")
	}
}

// Closing the sink ends the stream cleanly, and the reader drains what was
// already queued before seeing EOF — a stage's last batches are not lost
// because the producer finished first.
func TestClosingTheSinkDrainsThenEnds(t *testing.T) {
	p := NewPipe(8)
	writeStrings(t, p.Sink(), "a", "b")
	_ = p.Sink().Close()

	if got := drain(t, p.Source()); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("read %v after close, want both queued batches then EOF", got)
	}
}

// An empty batch carries nothing and must not become a queue entry: a stage
// that filters everything out would otherwise fill its consumer's queue with
// nothing, and a depth-N pipe would stall behind N empty batches.
func TestEmptyBatchesAreNotQueued(t *testing.T) {
	p := NewPipe(1)
	empty := record.NewBatch()
	for range 5 {
		if err := p.Sink().Write(context.Background(), empty); err != nil {
			t.Fatalf("writing an empty batch: %v", err)
		}
	}
	_ = p.Sink().Close()
	if got := drain(t, p.Source()); len(got) != 0 {
		t.Fatalf("empty writes produced %v", got)
	}
}

// Both halves are closed by the machinery around them — a driver closes the
// sink, a pipeline closes its source — often more than once, and on both the
// success and the error path. Neither may panic.
func TestBothHalvesAreSafeToCloseTwice(t *testing.T) {
	p := NewPipe(4)
	src := p.Source()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			_ = p.Sink().Close()
			_ = src.Close()
		})
	}
	wg.Wait()
}
