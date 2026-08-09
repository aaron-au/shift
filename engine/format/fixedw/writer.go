package fixedw

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// WriterOptions configure a Writer.
type WriterOptions struct {
	// Columns is the layout. Required.
	Columns []Column
	// Unseparated writes records back to back with no line terminator.
	Unseparated bool
}

// Writer renders records as fixed-width lines. It implements stream.Sink.
type Writer struct {
	w       *bufio.Writer
	opts    WriterOptions
	layout  *resolved
	locs    []*time.Location
	cell    []byte // rendering scratch, reused per column
	started bool
	err     error
}

// NewWriter wraps w. A layout error surfaces on the first Write.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	return &Writer{w: bufio.NewWriter(w), opts: opts}
}

func (w *Writer) start() error {
	w.started = true
	layout, err := resolve(w.opts.Columns)
	if err != nil {
		return err
	}
	w.layout = layout
	w.locs = make([]*time.Location, len(layout.cols))
	for i, c := range layout.cols {
		if c.Location == "" {
			w.locs[i] = time.UTC
			continue
		}
		loc, err := time.LoadLocation(c.Location)
		if err != nil {
			return fmt.Errorf("fixedw: column %q: unknown location %q: %w", c.Name, c.Location, err)
		}
		w.locs[i] = loc
	}
	return nil
}

// Write renders every record in b.
func (w *Writer) Write(ctx context.Context, b *record.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	if !w.started {
		if err := w.start(); err != nil {
			w.err = err
			return err
		}
	}
	for i, rec := range b.Records() {
		if rec.Kind() != record.KindMap {
			return w.fail(fmt.Errorf("fixedw: record %d is %v, want map; flatten containers first", i, rec.Kind()))
		}
		if err := w.record(rec); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

func (w *Writer) fail(err error) error {
	w.err = err
	return err
}

func (w *Writer) record(rec record.Value) error {
	for i, c := range w.layout.cols {
		if c.filler() {
			if err := w.pad(c.Pad, c.Width); err != nil {
				return err
			}
			continue
		}
		v, ok := rec.Field(c.Name)
		if !ok {
			v = record.Null() // a missing field is a blank column, not an error
		}
		text, err := w.render(i, c, v)
		if err != nil {
			return fmt.Errorf("fixedw: column %q: %w", c.Name, err)
		}
		if len(text) > c.Width {
			// The single most important rule in this package. A silently
			// truncated account number is indistinguishable from a real one
			// downstream, and the system that receives it has no way to tell.
			return fmt.Errorf("fixedw: column %q: value %q is %d bytes, column is %d; "+
				"widen the column or shorten the value — truncating would produce a "+
				"plausible wrong value", c.Name, clip(text), len(text), c.Width)
		}
		if err := w.padded(text, c); err != nil {
			return err
		}
	}
	if w.opts.Unseparated {
		return nil
	}
	return w.w.WriteByte('\n')
}

// padded writes text aligned within its column.
func (w *Writer) padded(text []byte, c Column) error {
	fill := c.Width - len(text)
	if c.Align == AlignRight {
		if err := w.pad(c.Pad, fill); err != nil {
			return err
		}
		_, err := w.w.Write(text)
		return err
	}
	if _, err := w.w.Write(text); err != nil {
		return err
	}
	return w.pad(c.Pad, fill)
}

func (w *Writer) pad(b byte, n int) error {
	for range n {
		if err := w.w.WriteByte(b); err != nil {
			return err
		}
	}
	return nil
}

// render produces a column's text into the writer's scratch buffer.
func (w *Writer) render(i int, c Column, v record.Value) ([]byte, error) {
	w.cell = w.cell[:0]
	if v.IsNull() {
		return w.cell, nil // blank: the only NULL a fixed-width file has
	}
	switch c.Type {
	case TypeString:
		switch v.Kind() {
		case record.KindString, record.KindBytes:
			w.cell = append(w.cell, v.Bytes()...)
		default:
			// Everything else renders canonically rather than being refused:
			// a string column receiving an id that happens to be an int is
			// ordinary, and AppendText is the one agreed spelling.
			w.cell = v.AppendText(w.cell)
			if len(w.cell) == 0 {
				return nil, fmt.Errorf("cannot render %v as text", v.Kind())
			}
		}
	case TypeInt:
		if v.Kind() != record.KindInt {
			return nil, fmt.Errorf("value is %v, column is int", v.Kind())
		}
		w.cell = strconv.AppendInt(w.cell, v.Int(), 10)
	case TypeFloat:
		if !v.IsNumeric() {
			return nil, fmt.Errorf("value is %v, column is float", v.Kind())
		}
		w.cell = strconv.AppendFloat(w.cell, v.Float(), 'f', -1, 64)
	case TypeBool:
		if v.Kind() != record.KindBool {
			return nil, fmt.Errorf("value is %v, column is bool", v.Kind())
		}
		if v.Bool() {
			w.cell = append(w.cell, 'Y')
		} else {
			w.cell = append(w.cell, 'N')
		}
	case TypeDecimal:
		coef, scale, err := coefficientAt(v, c.Scale)
		if err != nil {
			return nil, err
		}
		if c.Scale == 0 {
			w.cell = record.AppendDecimal(w.cell, coef, scale)
		} else {
			// An implied scale means the point is not written: the layout
			// already says where it is.
			w.cell = strconv.AppendInt(w.cell, coef, 10)
		}
	case TypeZoned:
		coef, _, err := coefficientAt(v, c.Scale)
		if err != nil {
			return nil, err
		}
		w.cell, err = appendZoned(w.cell, coef)
		if err != nil {
			return nil, err
		}
	case TypeTimestamp, TypeDate, TypeTime:
		if v.AsTime().IsZero() {
			return nil, fmt.Errorf("value is %v, column is %v", v.Kind(), c.Type)
		}
		w.cell = v.AsTime().In(w.locs[i]).AppendFormat(w.cell, layoutFor(c))
	default:
		return nil, fmt.Errorf("unknown column type %d", c.Type)
	}
	return w.cell, nil
}

// coefficientAt returns v's coefficient rescaled to the column's implied scale.
// Rescaling that would drop a digit is an error, for the same reason a too-wide
// value is: a quietly rounded amount is a plausible wrong amount.
func coefficientAt(v record.Value, scale int8) (int64, int8, error) {
	coef, have, ok := v.ExactDecimal()
	if !ok {
		return 0, 0, fmt.Errorf("value is %v, column is an exact decimal; coerce it first", v.Kind())
	}
	if scale == 0 || have == scale {
		return coef, have, nil
	}
	for have < scale { // add implied places
		if coef > (1<<62)/10 || coef < -(1<<62)/10 {
			return 0, 0, fmt.Errorf("value %s does not fit scale %d", v.Text(), scale)
		}
		coef *= 10
		have++
	}
	for have > scale { // drop places, but only zeros
		if coef%10 != 0 {
			return 0, 0, fmt.Errorf("value %s has more decimal places than the column's scale of %d; "+
				"rounding it here would produce a plausible wrong amount", v.Text(), scale)
		}
		coef /= 10
		have--
	}
	return coef, scale, nil
}

// appendZoned renders a signed coefficient with its last digit overpunched.
func appendZoned(dst []byte, coef int64) ([]byte, error) {
	neg := coef < 0
	start := len(dst)
	// AppendDecimal at scale 0 gives the plain digits, with a sign we drop —
	// in a zoned field the sign lives in the final byte, not in front.
	dst = record.AppendDecimal(dst, coef, 0)
	if neg {
		dst = append(dst[:start], dst[start+1:]...)
	}
	last, err := encodeOverpunch(dst[len(dst)-1], neg)
	if err != nil {
		return nil, err
	}
	dst[len(dst)-1] = last
	return dst, nil
}

// Close flushes buffered output. It does not close the underlying writer.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	return w.w.Flush()
}
