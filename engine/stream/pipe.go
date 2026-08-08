package stream

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/aaron-au/shift/engine/record"
)

// Pipe joins a Sink to a Source across a bounded queue, so one stage's output
// becomes another stage's input while both run concurrently.
//
// It is what makes NESTED and MIXED topologies executable (ADR-0029, issue
// #59). RunTee and RunRouter are drivers: they own the upstream pull loop and
// each branch terminates at a Sink, which is exactly right when a fan-out
// feeds N sinks and useless when a branch feeds a merge. A pipe gives that
// branch a Sink to end at and gives the merge a Source to read, and because
// the queue is bounded the two stages exert backpressure on each other rather
// than buffering a stream.
//
// Two rules the implementation exists to honour:
//
//   - BATCH LIFETIME. A batch handed to Write is valid only until the caller's
//     next Next, so the pipe copies into its own batch before queueing. That
//     copy is the price of crossing a concurrency boundary; it is the same
//     copy a copy-on-write fan-out branch already pays.
//   - NO DEADLOCK ON EARLY EXIT. If the reader stops (its pipeline failed, or
//     downstream errored), a writer blocked on a full queue would hang its
//     whole stage forever — and the caller joining that stage would hang with
//     it. Closing the Source unblocks writers with ErrPipeClosed, so the
//     producing stage tears down and its error surfaces.
//
// Errors do NOT travel through the pipe. A stage's failure is reported by the
// driver that ran it; the reader simply sees the stream end. The caller joins
// both and lets the upstream error win — the pipe carries records, not
// outcomes, and giving it two jobs would make the error ordering racy.
type Pipe struct {
	ch   chan *record.Batch
	done chan struct{} // closed by the reader; releases a blocked writer
	pool *batchPool

	closeOnce sync.Once
	doneOnce  sync.Once
}

// ErrPipeClosed is a write to a pipe whose reader has gone away.
var ErrPipeClosed = errors.New("stream: pipe closed by its reader")

// NewPipe creates a pipe with the given queue depth (<=0 uses a small
// default). Depth is flow control within one task, never a gate between tasks
// (ADR-0005): a full queue means the consumer is genuinely behind.
func NewPipe(depth int) *Pipe {
	if depth <= 0 {
		depth = defaultQueueDepth
	}
	return &Pipe{
		ch:   make(chan *record.Batch, depth),
		done: make(chan struct{}),
		pool: &batchPool{},
	}
}

// Sink returns the writing half.
func (p *Pipe) Sink() Sink { return (*pipeSink)(p) }

// Source returns the reading half.
func (p *Pipe) Source() Source { return &pipeSource{p: p} }

type pipeSink Pipe

// Write copies b into a pooled batch and queues it. The copy is required: b
// belongs to the caller only until its next Next.
func (s *pipeSink) Write(ctx context.Context, b *record.Batch) error {
	if b.Len() == 0 {
		return nil
	}
	out := s.pool.get()
	for _, r := range b.Records() {
		out.Append(record.CopyValue(out, r))
	}
	select {
	case s.ch <- out:
		return nil
	case <-s.done:
		// The reader is gone. Returning an error (rather than dropping the
		// batch) is what tears the producing stage down instead of letting it
		// run to completion writing into nothing.
		s.pool.put(out)
		return ErrPipeClosed
	case <-ctx.Done():
		s.pool.put(out)
		return ctx.Err()
	}
}

// Close ends the stream. The reader sees io.EOF once the queue drains.
func (s *pipeSink) Close() error {
	s.closeOnce.Do(func() { close(s.ch) })
	return nil
}

type pipeSource struct {
	p    *Pipe
	held *record.Batch // returned to the pool on the next pull
}

func (r *pipeSource) Next(ctx context.Context) (*record.Batch, error) {
	if r.held != nil {
		r.p.pool.put(r.held)
		r.held = nil
	}
	select {
	case b, ok := <-r.p.ch:
		if !ok {
			return nil, io.EOF
		}
		r.held = b
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close signals that no further batches will be read. It is what keeps a
// failing downstream from hanging the stage upstream of it.
func (r *pipeSource) Close() error {
	if r.held != nil {
		r.p.pool.put(r.held)
		r.held = nil
	}
	r.p.doneOnce.Do(func() { close(r.p.done) })
	return nil
}
