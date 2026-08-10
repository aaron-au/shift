package stream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// SpillBuffer is a fan-out branch buffer whose WRITER NEVER BLOCKS (TC-029).
//
// # The deadlock it exists to remove
//
// A Pipe is bounded on purpose: a full queue means the consumer is behind, and
// backpressure is how one task's stages stay in step without buffering a
// stream. That is right whenever the consumer is eventually going to read.
//
// It is wrong when the consumer CANNOT read yet. A join is a blocking operator
// on its build side — it consumes the whole build input before emitting
// anything — so in the enrichment shape
//
//	src → tee → [probe, build] → join → sink
//
// the join will not touch the probe branch until the build branch has ended.
// Both branches are fed by one tee, and both the tee's per-branch queue and the
// branch pipe are bounded, so the probe side fills, the tee blocks trying to
// hand it another batch, and the build side it must also feed never receives
// another record. Neither side can advance, nothing times out, and the task
// holds its admission reservation forever (ADR-0005). Measured: the shape
// completed at 5k records and hung permanently at 12k.
//
// Backpressure cannot fix this, because there is no rate at which the producer
// could feed a consumer that is not reading at all. The probe stream has to go
// somewhere until the join is ready for it.
//
// # Bounded, not unbounded
//
// "Never blocks the writer" is not "buffers without limit". Records stay in
// memory only while mem.Governor grants them; the first refusal switches the
// buffer to the scratch store and everything after it is spilled. So memory is
// governed exactly like the aggregate and the join, and the growth lands on
// disk, which is the trade the doctrine already makes for blocking operators.
//
// # Order
//
// Once spilling starts it never stops, so the in-memory batches are always the
// EARLIEST records and draining memory-then-spill preserves arrival order. A
// buffer that returned to memory after spilling would have to merge two ordered
// runs to say the same thing, which is a sort, not a buffer.
type SpillBuffer struct {
	store *spill.Store
	gov   *mem.Governor

	mu       sync.Mutex
	done     chan struct{} // closed when the writer finishes
	doneOnce sync.Once

	// Written by the sink, read by the source after done is closed. The
	// handover is one-way and the channel close is the barrier, so the source
	// needs no lock for these.
	mem      []*record.Batch
	reserved int64
	spilling bool
	w        *spill.Writer
	buf      *bufio.Writer
	enc      *spill.Encoder
	seg      spill.Segment
	spilled  int64
	writeErr error

	// read state
	memIdx  int
	dec     *spill.Decoder
	decLeft int64
	out     *record.Batch
}

// NewSpillBuffer creates a buffer backed by store, admitting records to memory
// while gov allows. Both are required: a buffer with nowhere to spill is the
// unbounded queue this design refuses to introduce.
func NewSpillBuffer(store *spill.Store, gov *mem.Governor) (*SpillBuffer, error) {
	if store == nil || gov == nil {
		return nil, errors.New("stream: spill buffer needs both a store and a governor")
	}
	return &SpillBuffer{store: store, gov: gov, done: make(chan struct{})}, nil
}

// Sink returns the writing half.
func (b *SpillBuffer) Sink() Sink { return (*spillBufferSink)(b) }

// Source returns the reading half.
func (b *SpillBuffer) Source() Source { return (*spillBufferSource)(b) }

// perRecordCost is the heuristic memory charge for one buffered record. It
// matches the join's build-row charge: precise sizing is not needed, only a
// bound on growth.
const perRecordCost = 256

type spillBufferSink SpillBuffer

// Write takes a copy of b — the caller owns it only until their next Next —
// and never blocks on a consumer.
func (s *spillBufferSink) Write(_ context.Context, b *record.Batch) error {
	if b.Len() == 0 {
		return nil
	}
	if s.writeErr != nil {
		return s.writeErr
	}
	cost := int64(b.Len()) * perRecordCost
	if !s.spilling && s.gov.TryReserve(cost) {
		out := record.NewBatch()
		for _, r := range b.Records() {
			out.Append(record.CopyValue(out, r))
		}
		s.mem = append(s.mem, out)
		s.reserved += cost
		return nil
	}
	// The governor said stop. From here everything goes to disk, so the
	// in-memory prefix stays the earliest records and order is preserved.
	if err := s.startSpilling(); err != nil {
		s.writeErr = err
		return err
	}
	for _, r := range b.Records() {
		if err := s.enc.Encode(r); err != nil {
			s.writeErr = fmt.Errorf("stream: spill buffer: %w", err)
			return s.writeErr
		}
		s.spilled++
	}
	return nil
}

func (s *spillBufferSink) startSpilling() error {
	if s.spilling {
		return nil
	}
	s.w = s.store.NewWriter()
	s.buf = bufio.NewWriterSize(s.w, 256<<10)
	s.enc = spill.NewEncoder(s.buf)
	s.spilling = true
	return nil
}

// Close seals the buffer and releases the reader. It is the barrier: the source
// returns nothing until the whole branch has been written, which is exactly the
// point — the consumer was never going to read early anyway.
func (s *spillBufferSink) Close() error {
	defer (*SpillBuffer)(s).finish()
	if !s.spilling {
		return s.writeErr
	}
	if err := s.buf.Flush(); err != nil {
		s.writeErr = fmt.Errorf("stream: spill buffer flush: %w", err)
		return s.writeErr
	}
	seg, err := s.w.Finish()
	if err != nil {
		s.writeErr = err
		return err
	}
	s.seg = seg
	return s.writeErr
}

func (b *SpillBuffer) finish() { b.doneOnce.Do(func() { close(b.done) }) }

type spillBufferSource SpillBuffer

// Next waits for the writer to finish, then drains memory first and the spilled
// segment second.
func (s *spillBufferSource) Next(ctx context.Context) (*record.Batch, error) {
	select {
	case <-s.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.writeErr != nil {
		return nil, s.writeErr
	}
	if s.memIdx < len(s.mem) {
		b := s.mem[s.memIdx]
		s.mem[s.memIdx] = nil // release as we go; the reader owns the drain
		s.memIdx++
		return b, nil
	}
	if s.spilled == 0 {
		return nil, io.EOF
	}
	if s.dec == nil {
		s.dec = spill.NewDecoder(bufio.NewReaderSize(s.store.OpenSegment(s.seg), 256<<10), 0)
		s.decLeft = s.spilled
		s.out = record.NewBatch()
	}
	if s.decLeft == 0 {
		return nil, io.EOF
	}
	s.out.Reset()
	// Rebuild in batches so the reader sees the same shape it would have from a
	// pipe rather than one enormous batch.
	const batchSize = 1024
	for range batchSize {
		if s.decLeft == 0 {
			break
		}
		bld := s.out.Builder()
		if err := s.dec.Decode(bld); err != nil {
			return nil, fmt.Errorf("stream: spill buffer read: %w", err)
		}
		s.out.Append(bld.Finish())
		s.decLeft--
	}
	if s.out.Len() == 0 {
		return nil, io.EOF
	}
	return s.out, nil
}

// Close releases the memory reservation. It is safe before the writer finishes:
// the reservation is the sink's, and releasing it early would only under-report.
func (s *spillBufferSource) Close() error {
	b := (*SpillBuffer)(s)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reserved > 0 {
		b.gov.Release(b.reserved)
		b.reserved = 0
	}
	b.mem = nil
	return nil
}
