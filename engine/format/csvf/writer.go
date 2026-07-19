package csvf

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/aaron-au/shift/engine/record"
)

// Writer streams flat map records out as CSV. Columns are fixed from the
// first record (or Columns option); missing fields write as empty cells,
// container fields are an error (flatten first). Implements stream.Sink.
type Writer struct {
	cw      *csv.Writer
	columns []string
	wrote   bool
	row     []string
	scratch []byte
}

// WriterOptions configure a Writer.
type WriterOptions struct {
	// Columns pins the column set and order. Empty derives it from the
	// first record's fields.
	Columns []string
	// Comma is the field delimiter (',' when zero).
	Comma rune
}

// NewWriter wraps w.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	cw := csv.NewWriter(w)
	if opts.Comma != 0 {
		cw.Comma = opts.Comma
	}
	return &Writer{cw: cw, columns: opts.Columns}
}

// Write encodes each record as one CSV row (header row emitted first).
func (w *Writer) Write(ctx context.Context, b *record.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, rec := range b.Records() {
		if rec.Kind() != record.KindMap {
			return fmt.Errorf("csvf: record is %v, want map", rec.Kind())
		}
		if !w.wrote {
			if w.columns == nil {
				w.columns = make([]string, rec.Len())
				for i := range rec.Len() {
					w.columns[i] = string(rec.KeyAt(i))
				}
			}
			if err := w.cw.Write(w.columns); err != nil {
				return err
			}
			w.row = make([]string, len(w.columns))
			w.wrote = true
		}
		for i, col := range w.columns {
			v, ok := rec.Field(col)
			if !ok {
				w.row[i] = ""
				continue
			}
			cell, err := w.cell(v)
			if err != nil {
				return fmt.Errorf("csvf: column %q: %w", col, err)
			}
			w.row[i] = cell
		}
		if err := w.cw.Write(w.row); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) cell(v record.Value) (string, error) {
	switch v.Kind() {
	case record.KindNull:
		return "", nil
	case record.KindBool:
		if v.Bool() {
			return "true", nil
		}
		return "false", nil
	case record.KindInt:
		w.scratch = strconv.AppendInt(w.scratch[:0], v.Int(), 10)
		return string(w.scratch), nil
	case record.KindFloat:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("unsupported float value %v", f)
		}
		w.scratch = strconv.AppendFloat(w.scratch[:0], f, 'g', -1, 64)
		return string(w.scratch), nil
	case record.KindString, record.KindBytes:
		return v.String(), nil
	default:
		return "", fmt.Errorf("cannot encode %v in CSV; flatten containers first", v.Kind())
	}
}

// Close flushes buffered output; it does not close the underlying writer.
func (w *Writer) Close() error {
	w.cw.Flush()
	return w.cw.Error()
}
