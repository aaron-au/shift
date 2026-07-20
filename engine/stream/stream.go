// Package stream is the engine's pull-based pipeline layer: Sources produce
// record batches, operators transform them in place, Sinks consume them.
// One batch flows through the whole pipeline at a time, so memory is
// bounded by batch size plus explicit operator state (which the mem
// governor watches). Batch boundaries are where metrics and backpressure
// happen (ADR-0004).
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Source produces record batches. Next returns io.EOF after the final
// batch. The returned batch is valid only until the next Next or Close call
// — sources reuse batches (see record.Batch lifetime contract).
type Source interface {
	Next(ctx context.Context) (*record.Batch, error)
	Close() error
}

// Sink consumes record batches. Write must not retain the batch. Close
// flushes.
type Sink interface {
	Write(ctx context.Context, b *record.Batch) error
	Close() error
}

// OpStats are per-operator counters for honest pipeline metrics.
type OpStats struct {
	Name       string
	Batches    int64
	RecordsIn  int64
	RecordsOut int64
	// Nanos is time spent inside this operator only (upstream excluded).
	Nanos int64
}

// Report summarizes a pipeline run.
type Report struct {
	Ops        []OpStats
	RecordsOut int64
	WallNanos  int64
}

// OpError tags a run failure with the operator (Op == the OpStats.Name,
// which the runner sets to the flow step id) that produced it, so the
// runner can route to that step's error handler via errors.As rather than
// parsing the message. The Error string is unchanged from the previous
// "<op>: <err>" wrap.
type OpError struct {
	Op  string
	Err error
}

func (e *OpError) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *OpError) Unwrap() error { return e.Err }

// Sampler observes a bounded copy of the records leaving each pipeline
// stage (the source and every operator), for post-run inspection — e.g.
// test-mode data capture. It is called synchronously on the hot path with
// the stage's step name and its output batch, so implementations must be
// cheap and must NOT retain the batch (copy what they keep; batches are
// reused). nil disables sampling at zero cost.
type Sampler interface {
	Sample(step string, b *record.Batch)
}

// Pipeline chains a source through operators into a sink.
type Pipeline struct {
	src     Source
	stats   []*OpStats
	err     error
	sampler Sampler
}

// New starts a pipeline from src; name labels the source in the report.
func New(src Source, name string) *Pipeline {
	p := &Pipeline{}
	st := &OpStats{Name: name}
	p.stats = append(p.stats, st)
	p.src = &measuredSource{up: src, stats: st}
	return p
}

// WithSampler attaches a Sampler that observes every stage's output. Call
// it right after New (before appending operators): it wires the source
// stage immediately and every later Apply picks it up.
func (p *Pipeline) WithSampler(s Sampler) *Pipeline {
	p.sampler = s
	if ms, ok := p.src.(*measuredSource); ok {
		ms.sampler = s
	}
	return p
}

// Transform is a batch-in/batch-out function. It may mutate the batch in
// place (records may be rebuilt using the batch's own builder) and must
// return the batch to pass downstream — usually the same one.
type Transform func(ctx context.Context, b *record.Batch) (*record.Batch, error)

// Apply appends a named transform operator.
func (p *Pipeline) Apply(name string, fn Transform) *Pipeline {
	st := &OpStats{Name: name}
	p.stats = append(p.stats, st)
	p.src = &opSource{up: p.src, fn: fn, stats: st, sampler: p.sampler}
	return p
}

// RenameLastOp relabels the most recently appended operator. The runner
// uses it to stamp the flow step id onto each op so both the telemetry
// (OpStats.Name) and any OpError carry the step id — the convenience
// operator methods otherwise name ops by kind ("project", "coerce", …).
// No-op on an empty pipeline.
func (p *Pipeline) RenameLastOp(id string) *Pipeline {
	if n := len(p.stats); n > 0 {
		p.stats[n-1].Name = id
	}
	return p
}

// fail records a build-time error surfaced at Run.
func (p *Pipeline) fail(err error) *Pipeline {
	if p.err == nil {
		p.err = err
	}
	return p
}

// Run drains the pipeline into sink. It closes the source chain; the sink
// is Closed on success and on error.
func (p *Pipeline) Run(ctx context.Context, sink Sink, sinkName string) (Report, error) {
	if p.err != nil {
		return Report{}, p.err
	}
	sinkStats := &OpStats{Name: sinkName}
	start := time.Now()
	var runErr error
	var out int64
	for {
		b, err := p.src.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			runErr = err
			break
		}
		n := int64(b.Len())
		w := time.Now()
		if err := sink.Write(ctx, b); err != nil {
			runErr = &OpError{Op: sinkName, Err: err}
			break
		}
		sinkStats.Nanos += time.Since(w).Nanoseconds()
		sinkStats.Batches++
		sinkStats.RecordsIn += n
		sinkStats.RecordsOut += n
		out += n
	}
	if err := p.src.Close(); err != nil && runErr == nil {
		runErr = err
	}
	cw := time.Now()
	if err := sink.Close(); err != nil && runErr == nil {
		runErr = fmt.Errorf("%s: close: %w", sinkName, err)
	}
	sinkStats.Nanos += time.Since(cw).Nanoseconds()

	rep := Report{RecordsOut: out, WallNanos: time.Since(start).Nanoseconds()}
	for _, st := range p.stats {
		rep.Ops = append(rep.Ops, *st)
	}
	rep.Ops = append(rep.Ops, *sinkStats)
	return rep, runErr
}

// measuredSource attributes time spent producing batches to the source.
type measuredSource struct {
	up      Source
	stats   *OpStats
	sampler Sampler
}

func (m *measuredSource) Next(ctx context.Context) (*record.Batch, error) {
	start := time.Now()
	b, err := m.up.Next(ctx)
	m.stats.Nanos += time.Since(start).Nanoseconds()
	if err != nil {
		return nil, err
	}
	m.stats.Batches++
	n := int64(b.Len())
	m.stats.RecordsIn += n
	m.stats.RecordsOut += n
	if m.sampler != nil {
		m.sampler.Sample(m.stats.Name, b)
	}
	return b, nil
}

func (m *measuredSource) Close() error { return m.up.Close() }

// opSource applies a transform to each upstream batch, timing only its own
// work.
type opSource struct {
	up      Source
	fn      Transform
	stats   *OpStats
	sampler Sampler
}

func (o *opSource) Next(ctx context.Context) (*record.Batch, error) {
	for {
		b, err := o.up.Next(ctx)
		if err != nil {
			return nil, err // io.EOF included
		}
		in := int64(b.Len())
		start := time.Now()
		nb, err := o.fn(ctx, b)
		o.stats.Nanos += time.Since(start).Nanoseconds()
		if err != nil {
			return nil, &OpError{Op: o.stats.Name, Err: err}
		}
		o.stats.Batches++
		o.stats.RecordsIn += in
		o.stats.RecordsOut += int64(nb.Len())
		if nb.Len() == 0 {
			continue // fully filtered batch; pull the next one
		}
		if o.sampler != nil {
			o.sampler.Sample(o.stats.Name, nb)
		}
		return nb, nil
	}
}

func (o *opSource) Close() error { return o.up.Close() }
