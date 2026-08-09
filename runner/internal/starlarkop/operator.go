package starlarkop

import (
	"context"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
)

// Apply appends the starlark step to a pipeline.
//
// It attaches through the public Pipeline.Apply, which is what keeps Starlark
// out of `engine` entirely: the engine exposes an operator hook and never
// learns what is on the other side of it (ADR-0052 §2).
func Apply(p *stream.Pipeline, prog *Program, name string) *stream.Pipeline {
	// One scratch batch, reused: results are built here and then copied into
	// the flowing batch, because a script's output cannot be built directly
	// into the batch it is reading without the two interleaving on the
	// builder's stack.
	scratch := record.NewBatch()

	return p.Apply(name, func(ctx context.Context, b *record.Batch) (*record.Batch, error) {
		recs := b.Records()
		kept := recs[:0]
		for _, rec := range recs {
			scratch.Reset()
			out, keep, err := prog.Run(ctx, scratch, rec)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue // the script returned None: drop the record
			}
			// Back into the flowing batch, so downstream operators keep the
			// batch-lifetime contract they already rely on.
			kept = append(kept, record.CopyValue(b, out))
		}
		b.SetRecords(kept)
		return b, nil
	})
}
