package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// SpillBuffer's whole purpose is the SPILL path, and the DAG test that motivated
// it (the enrichment shape) stays under the governor budget — so until these
// tests existed the spilling half had never executed anywhere. That is the
// register's own question with the answer "no": nothing would have failed if it
// were broken.

func newBuffer(t *testing.T, budget int64) (*SpillBuffer, *spill.Store) {
	t.Helper()
	store, err := spill.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b, err := NewSpillBuffer(store, mem.New(budget))
	if err != nil {
		t.Fatal(err)
	}
	return b, store
}

// writeRecords feeds n records of the form {"i": <index>} in batches of size.
func writeRecords(t *testing.T, sink Sink, n, size int) {
	t.Helper()
	ctx := context.Background()
	b := record.NewBatch()
	for i := range n {
		if b.Len() == size {
			if err := sink.Write(ctx, b); err != nil {
				t.Fatalf("write: %v", err)
			}
			b.Reset()
		}
		bld := b.Builder()
		bld.BeginMap()
		bld.Key([]byte("i"))
		bld.Int(int64(i))
		bld.EndMap()
		b.Append(bld.Finish())
	}
	if b.Len() > 0 {
		if err := sink.Write(ctx, b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// drainInts reads every record's "i" field in order.
func drainInts(t *testing.T, src Source) []int64 {
	t.Helper()
	ctx := context.Background()
	var got []int64
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("read after %d records: %v", len(got), err)
		}
		for _, r := range b.Records() {
			v, ok := r.Field("i")
			if !ok {
				t.Fatalf("record %d has no i field", len(got))
			}
			got = append(got, v.Int())
		}
	}
}

func assertInOrder(t *testing.T, got []int64, want int) {
	t.Helper()
	if len(got) != want {
		t.Fatalf("read %d records, want %d", len(got), want)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("record %d is %d: order was not preserved", i, v)
		}
	}
}

// TestABufferThatFitsInMemoryNeverTouchesTheStore is the fast path: a small
// branch must not pay for scratch it does not need.
func TestABufferThatFitsInMemoryNeverTouchesTheStore(t *testing.T) {
	b, store := newBuffer(t, 1<<30) // budget far above the data
	const n = 500

	writeRecords(t, b.Sink(), n, 64)
	assertInOrder(t, drainInts(t, b.Source()), n)

	if store.BytesWritten() != 0 {
		t.Fatalf("wrote %d bytes of scratch for a buffer that fit in memory", store.BytesWritten())
	}
}

// TestABufferLargerThanTheBudgetSpillsAndStillReadsInOrder is the path that had
// never run. The governor budget is set so low that the first write is admitted
// and the rest are refused, which is exactly the memory-then-disk boundary the
// design depends on.
func TestABufferLargerThanTheBudgetSpillsAndStillReadsInOrder(t *testing.T) {
	b, store := newBuffer(t, 4*perRecordCost) // room for a handful of records
	const n = 5000

	writeRecords(t, b.Sink(), n, 64)

	if store.BytesWritten() == 0 {
		t.Fatal("nothing was spilled: the governor refusal did not switch the buffer to scratch")
	}
	t.Logf("spilled %d bytes for %d records", store.BytesWritten(), n)

	assertInOrder(t, drainInts(t, b.Source()), n)
}

// TestEverySpilledBufferReadsBackInOrder covers the boundary itself across a
// range of budgets, because the ordering rule — memory holds the EARLIEST
// records, so drain memory then disk — is only interesting where the split
// actually falls.
func TestEverySpilledBufferReadsBackInOrder(t *testing.T) {
	for _, budget := range []int64{0 + perRecordCost, 10 * perRecordCost, 100 * perRecordCost, 1000 * perRecordCost} {
		t.Run(fmt.Sprintf("budget-%d-records", budget/perRecordCost), func(t *testing.T) {
			b, _ := newBuffer(t, budget)
			const n = 3000
			writeRecords(t, b.Sink(), n, 32)
			assertInOrder(t, drainInts(t, b.Source()), n)
		})
	}
}

// TestAnEmptyBufferEndsImmediately: a branch that carries no records must not
// leave its reader waiting.
func TestAnEmptyBufferEndsImmediately(t *testing.T) {
	b, _ := newBuffer(t, 1<<20)
	if err := b.Sink().Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Source().Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next on an empty buffer = %v, want io.EOF", err)
	}
}

// TestAnEmptyBatchIsNotABatch: operators legitimately produce empty batches
// (a filter that matched nothing), and each must not become a record.
func TestAnEmptyBatchIsNotABatch(t *testing.T) {
	b, _ := newBuffer(t, 1<<20)
	ctx := context.Background()
	if err := b.Sink().Write(ctx, record.NewBatch()); err != nil {
		t.Fatal(err)
	}
	if err := b.Sink().Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Source().Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after only empty batches = %v, want io.EOF", err)
	}
}

// TestTheReaderWaitsForTheWriterRatherThanSeeingAShortStream. The handover is
// the sink's Close, and a reader that returned EOF early would silently drop
// the tail of a branch — a data-loss bug with no error attached.
func TestTheReaderWaitsForTheWriterRatherThanSeeingAShortStream(t *testing.T) {
	b, _ := newBuffer(t, 1<<20)
	const n = 200

	done := make(chan []int64, 1)
	go func() { done <- drainInts(t, b.Source()) }()

	writeRecords(t, b.Sink(), n, 16)
	assertInOrder(t, <-done, n)
}

// TestACancelledReaderDoesNotBlockForever: the reader parks until the writer
// finishes, so it must honour its context or a torn-down topology would hang.
func TestACancelledReaderDoesNotBlockForever(t *testing.T) {
	b, _ := newBuffer(t, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Source().Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next with a cancelled context = %v, want context.Canceled", err)
	}
}

// TestClosingTheReaderReleasesTheReservation: the buffer charges the governor
// for what it holds, and a branch that ended without giving that back would
// shrink the runner's admission budget for the life of the process (ADR-0005).
func TestClosingTheReaderReleasesTheReservation(t *testing.T) {
	store, err := spill.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	gov := mem.New(1 << 30)
	b, err := NewSpillBuffer(store, gov)
	if err != nil {
		t.Fatal(err)
	}
	writeRecords(t, b.Sink(), 500, 64)
	if gov.Used() == 0 {
		t.Fatal("the buffer held records without charging the governor")
	}
	if err := b.Source().Close(); err != nil {
		t.Fatal(err)
	}
	if used := gov.Used(); used != 0 {
		t.Fatalf("governor still holds %d bytes after the reader closed", used)
	}
}

// TestABufferNeedsBothAStoreAndAGovernor: a buffer with nowhere to spill is the
// unbounded queue the design exists to refuse, so it must not be constructible.
func TestABufferNeedsBothAStoreAndAGovernor(t *testing.T) {
	store, err := spill.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if _, err := NewSpillBuffer(nil, mem.New(1<<20)); err == nil {
		t.Error("a buffer was created with no store")
	}
	if _, err := NewSpillBuffer(store, nil); err == nil {
		t.Error("a buffer was created with no governor")
	}
}
