package ndjson

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// JSONReader streams a standard JSON document into record batches, reusing the
// ndjson value parser. Unlike Reader (which is newline-delimited and reads one
// value per line), it handles the shapes typical REST APIs return:
//
//   - a top-level array — each element becomes a record (streamed element by
//     element, so a large array is never held whole);
//   - a single object or scalar — one record;
//   - a stream of concatenated / newline-separated values — each a record.
//
// It reads through an encoding/json.Decoder, so pretty-printed input (newlines
// inside a value) parses correctly where the line-based Reader cannot. Only one
// element's raw bytes plus the current batch are resident at a time — the whole
// document is never buffered as a map/slice (doctrine: no whole-payload
// buffering). It implements stream.Source; the batch from Next is valid only
// until the next Next or Close.
type JSONReader struct {
	br    *bufio.Reader
	dec   *json.Decoder
	opts  ReaderOptions
	batch *record.Batch
	p     parser
	// raw is the reused decode buffer for one element. Reader state, not a
	// loop local: see the comment at its use in Next.
	raw     json.RawMessage
	budget  *valueBudget
	started bool
	array   bool
	done    bool
}

// NewJSONReader wraps r. It does not close the underlying reader.
func NewJSONReader(r io.Reader, opts ReaderOptions) *JSONReader {
	opts.defaults()
	// The line Reader bounds a line with MaxLineBytes; here the unit is one
	// top-level VALUE, and until this budget existed nothing bounded it at all.
	// Decode materialises a whole value into json.RawMessage, so a single
	// enormous value is a single enormous allocation — measured at 2,561 MiB of
	// heap from 521,845 wire bytes when the response was gzipped. The per-line
	// bound cannot see it, because there are no lines. TC-019.
	budget := &valueBudget{r: r, limit: int64(opts.MaxLineBytes)}
	br := bufio.NewReader(budget)
	return &JSONReader{br: br, budget: budget, dec: json.NewDecoder(br), opts: opts, batch: record.NewBatch()}
}

// Next returns the next batch, or io.EOF when the document is exhausted.
func (r *JSONReader) Next(ctx context.Context) (*record.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.done {
		return nil, io.EOF
	}
	if !r.started {
		r.started = true
		c, err := peekFirstNonSpace(r.br)
		if err != nil {
			r.done = true
			if errors.Is(err, io.EOF) {
				return nil, io.EOF // empty body
			}
			return nil, fmt.Errorf("json: %w", err)
		}
		if c == '[' { // top-level array: consume the opening bracket, stream elements
			r.array = true
			if _, err := r.dec.Token(); err != nil {
				return nil, fmt.Errorf("json: %w", err)
			}
		}
	}

	r.batch.Reset()
	for r.batch.Len() < r.opts.BatchRecords && r.batch.ArenaBytes() < r.opts.BatchBytes {
		if r.array && !r.dec.More() { // reached the closing ']'
			r.done = true
			_, _ = r.dec.Token() // consume ']' (end of input either way)
			break
		}
		// r.raw is a Reader field, not a local: declared inside the loop it was
		// reallocated for every element instead of reusing its capacity, which
		// cost 2 allocations PER RECORD on the reader the http connector uses
		// for JSON-array APIs. Decode appends into the existing slice once it
		// has room. Measured and caught by TestJSONReaderDoesNotAllocate
		// (TC-006).
		r.raw = r.raw[:0]
		if err := r.dec.Decode(&r.raw); err != nil {
			if errors.Is(err, io.EOF) { // stream of values exhausted
				r.done = true
				break
			}
			return nil, fmt.Errorf("json: %w", err)
		}
		r.budget.reset() // the bound is per value, not for the whole document
		v, err := r.p.parseLine(r.raw, r.batch.Builder(), r.opts.MaxDepth)
		if err != nil {
			return nil, fmt.Errorf("json: %w", err)
		}
		r.batch.Append(v)
	}
	if r.batch.Len() == 0 {
		return nil, io.EOF
	}
	return r.batch, nil
}

// Close releases the reader. It does not close the underlying io.Reader.
func (r *JSONReader) Close() error {
	r.done = true
	return nil
}

// peekFirstNonSpace returns the first non-whitespace byte of br without
// consuming any input (json.Decoder, sharing the same bufio buffer, still sees
// the full stream afterwards). It is used only to distinguish a top-level array
// from other JSON shapes.
func peekFirstNonSpace(br *bufio.Reader) (byte, error) {
	for skip := 0; ; skip++ {
		buf, err := br.Peek(skip + 1)
		if len(buf) > skip {
			switch c := buf[skip]; c {
			case ' ', '\t', '\n', '\r':
				continue
			default:
				return c, nil
			}
		}
		if err != nil {
			return 0, err
		}
	}
}
