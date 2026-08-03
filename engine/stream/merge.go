package stream

import (
	"context"
	"errors"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// Concat multiplexes several input sources into one stream — the concat mode
// of a v3 merge node (ADR-0029): an unordered, keyless union of every input's
// records. It drains the inputs round-robin (one batch per input per turn),
// yielding each batch as it arrives until all inputs reach EOF.
//
// It is fully streaming and allocation-free: each output Next pulls exactly
// one input and forwards its batch, so memory stays bounded by a single batch
// and no records are copied. The batch-lifetime contract holds unchanged —
// a returned batch is owned by the input that produced it and stays valid
// until Concat pulls that same input again (which happens only after the
// caller has consumed the batch and pulled at least once more).
//
// Order across inputs is unspecified (interleaved). Concat is single-
// goroutine: it inherits pull-based backpressure (it pulls an input only when
// the downstream pulls it), so a slow input throttles the merge without
// buffering — at the cost of head-of-line blocking on whichever input is
// currently being pulled. A non-blocking, ready-input-first multiplex is a
// later optimization (ADR-0029 open questions); correctness does not need it.
func Concat(inputs ...Source) Source {
	return &concatSource{
		inputs: inputs,
		done:   make([]bool, len(inputs)),
		live:   len(inputs),
	}
}

type concatSource struct {
	inputs []Source
	done   []bool // per-input EOF flag
	cursor int    // round-robin position
	live   int    // inputs not yet at EOF
}

func (c *concatSource) Next(ctx context.Context) (*record.Batch, error) {
	for c.live > 0 {
		idx := c.cursor
		c.cursor = (c.cursor + 1) % len(c.inputs)
		if c.done[idx] {
			continue
		}
		b, err := c.inputs[idx].Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.done[idx] = true
				c.live--
				continue
			}
			return nil, err // surface the first hard error; Close tears the rest down
		}
		return b, nil
	}
	return nil, io.EOF
}

// Close closes every input, returning the first error but always attempting
// all — a merge owns its inputs' lifetimes.
func (c *concatSource) Close() error {
	var first error
	for _, s := range c.inputs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
