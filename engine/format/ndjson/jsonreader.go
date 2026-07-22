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
	br      *bufio.Reader
	dec     *json.Decoder
	opts    ReaderOptions
	batch   *record.Batch
	p       parser
	started bool
	array   bool
	done    bool
}

// NewJSONReader wraps r. It does not close the underlying reader.
func NewJSONReader(r io.Reader, opts ReaderOptions) *JSONReader {
	opts.defaults()
	br := bufio.NewReader(r)
	return &JSONReader{br: br, dec: json.NewDecoder(br), opts: opts, batch: record.NewBatch()}
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
		var raw json.RawMessage
		if err := r.dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) { // stream of values exhausted
				r.done = true
				break
			}
			return nil, fmt.Errorf("json: %w", err)
		}
		v, err := r.p.parseLine(raw, r.batch.Builder(), r.opts.MaxDepth)
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
