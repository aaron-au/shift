package fixedw

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Batch sizing defaults (same rationale as ndjson).
const (
	DefaultBatchRecords = 1024
	DefaultBatchBytes   = 1 << 20
	// DefaultMaxLineBytes bounds one line. 16 MiB matches csvf's record bound
	// and is orders of magnitude past any real fixed-width record, whose width
	// is stated by the layout.
	DefaultMaxLineBytes = 16 << 20
)

// ReaderOptions configure a Reader.
type ReaderOptions struct {
	// Columns is the layout. Required.
	Columns []Column
	// Unseparated reads fixed-length records with no line terminator at all
	// (a mainframe RECFM=F extract), rather than one record per line.
	Unseparated bool
	// SkipLines discards leading lines before the first record — a header or
	// banner some extracts carry. Ignored when Unseparated.
	SkipLines int
	// MaxLineBytes bounds ONE line (DefaultMaxLineBytes when zero).
	//
	// readLine falls back to unbounded accumulation whenever a line exceeds the
	// bufio buffer, and that path serves both the SkipLines preamble and every
	// record read — so a source that never emits a newline grew without limit.
	// Measured before this bound: `go test` reached its 180-second timeout still
	// buffering (TC-019).
	//
	// A fixed-width layout states how wide a record is, which is what makes an
	// endless line a contradiction rather than a judgement call: bytes past the
	// layout's width can never become fields.
	MaxLineBytes int64

	BatchRecords int
	BatchBytes   int64
}

// Reader streams fixed-width records as flat map records. It implements
// stream.Source; the returned batch is valid until the next Next or Close.
type Reader struct {
	br      *bufio.Reader
	opts    ReaderOptions
	layout  *resolved
	batch   *record.Batch
	keys    [][]byte // stable field names, outside the arena
	locs    []*time.Location
	scratch []byte // zoned digit rewriting
	fixed   []byte // whole-record buffer, Unseparated mode
	started bool
	done    bool
	row     int64
}

// NewReader wraps r. A layout error surfaces on the first Next.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	if opts.BatchRecords <= 0 {
		opts.BatchRecords = DefaultBatchRecords
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = DefaultMaxLineBytes
	}
	if opts.BatchBytes <= 0 {
		opts.BatchBytes = DefaultBatchBytes
	}
	return &Reader{br: bufio.NewReader(r), opts: opts, batch: record.NewBatch()}
}

func (r *Reader) start() error {
	r.started = true
	layout, err := resolve(r.opts.Columns)
	if err != nil {
		return err
	}
	r.layout = layout
	// Field names and zone lookups are per-layout, not per-record: resolving
	// them here is what keeps the row loop allocation-free.
	r.keys = make([][]byte, len(layout.cols))
	r.locs = make([]*time.Location, len(layout.cols))
	for i, c := range layout.cols {
		if c.filler() {
			continue
		}
		r.keys[i] = []byte(c.Name)
		if c.Location == "" {
			r.locs[i] = time.UTC
			continue
		}
		loc, err := time.LoadLocation(c.Location)
		if err != nil {
			return fmt.Errorf("fixedw: column %q: unknown location %q: %w", c.Name, c.Location, err)
		}
		r.locs[i] = loc
	}
	if r.opts.Unseparated {
		r.fixed = make([]byte, layout.length)
		return nil
	}
	for range r.opts.SkipLines {
		if _, err := r.readLine(); err != nil {
			if errors.Is(err, io.EOF) {
				r.done = true
				return nil
			}
			return err
		}
	}
	return nil
}

// readLine returns the next line without its terminator. The slice views the
// bufio buffer and is only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			// A line longer than the buffer: fall back to a copy. Rare, and a
			// sign the layout and the file disagree, but not our call to make.
			full := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				if int64(len(full)) > r.opts.MaxLineBytes {
					return nil, fmt.Errorf("fixedw: line %d exceeds max_line_bytes (%d) with no terminator; "+
						"a record cannot be wider than the layout", r.row+1, r.opts.MaxLineBytes)
				}
				line, err = r.br.ReadSlice('\n')
				full = append(full, line...)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
			return bytes.TrimRight(full, "\r\n"), nil
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return bytes.TrimRight(line, "\r\n"), nil // last line, no terminator
		}
		return nil, err
	}
	return bytes.TrimRight(line, "\r\n"), nil
}

// nextRecord returns the next record's bytes, or io.EOF.
func (r *Reader) nextRecord() ([]byte, error) {
	if r.opts.Unseparated {
		if _, err := io.ReadFull(r.br, r.fixed); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("fixedw: trailing partial record: input is not a whole number of %d-byte records", r.layout.length)
			}
			return nil, err
		}
		return r.fixed, nil
	}
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		// A blank line between records is padding, not a record of empty
		// fields; skipping it beats emitting a row of nulls.
		if len(bytes.TrimRight(line, " ")) == 0 {
			continue
		}
		return line, nil
	}
}

// checkLength validates a record against the layout. Positional formats fail
// silently when the layout is wrong — every field still parses, just from the
// wrong bytes — so length is checked rather than assumed.
func (r *Reader) checkLength(rec []byte) ([]byte, error) {
	switch {
	case len(rec) < r.layout.length:
		return nil, fmt.Errorf("fixedw: row %d is %d bytes, layout needs %d; "+
			"a short record means the file and the layout disagree", r.row, len(rec), r.layout.length)
	case len(rec) > r.layout.length:
		// Trailing spaces are ordinary; anything else is unaccounted data, and
		// silently dropping it would hide a layout that has drifted.
		if extra := bytes.TrimRight(rec[r.layout.length:], " "); len(extra) > 0 {
			return nil, fmt.Errorf("fixedw: row %d is %d bytes, layout accounts for %d, "+
				"and the remainder is not padding (%q)", r.row, len(rec), r.layout.length, clip(extra))
		}
		return rec[:r.layout.length], nil
	}
	return rec, nil
}

// clip bounds a byte snippet used in an error message, so a malformed file
// cannot put a whole record into a log line.
func clip(b []byte) []byte {
	const max = 24
	if len(b) > max {
		return b[:max]
	}
	return b
}

// Next returns the next batch, or io.EOF when input is exhausted.
func (r *Reader) Next(ctx context.Context) (*record.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.started {
		if err := r.start(); err != nil {
			return nil, err
		}
	}
	if r.done {
		return nil, io.EOF
	}
	r.batch.Reset()
	bld := r.batch.Builder()
	for r.batch.Len() < r.opts.BatchRecords && r.batch.ArenaBytes() < r.opts.BatchBytes {
		rec, err := r.nextRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.done = true
				break
			}
			return nil, err
		}
		r.row++
		rec, err = r.checkLength(rec)
		if err != nil {
			return nil, err
		}
		bld.BeginMap()
		for i, c := range r.layout.cols {
			if c.filler() {
				continue
			}
			bld.KeyNoCopy(r.keys[i]) // stable for the reader's lifetime
			cell := rec[r.layout.starts[i] : r.layout.starts[i]+c.Width]
			if err := r.cell(bld, i, c, cell); err != nil {
				return nil, fmt.Errorf("fixedw: row %d column %q: %w", r.row, c.Name, err)
			}
		}
		bld.EndMap()
		r.batch.Append(bld.Finish())
	}
	if r.batch.Len() == 0 {
		return nil, io.EOF
	}
	return r.batch, nil
}

// cell parses one column's bytes onto the builder.
func (r *Reader) cell(bld *record.Builder, i int, c Column, raw []byte) error {
	cell := trimCell(raw, c)
	// An all-padding cell is absent, not zero. A fixed-width file has no way
	// to write NULL, so blank is the only thing it can mean.
	if len(cell) == 0 && c.Type != TypeString {
		bld.Null()
		return nil
	}
	switch c.Type {
	case TypeString:
		bld.String(cell)
	case TypeInt:
		n, err := strconv.ParseInt(string(cell), 10, 64)
		if err != nil {
			return fmt.Errorf("not an int: %q", cell)
		}
		bld.Int(n)
	case TypeFloat:
		f, err := strconv.ParseFloat(string(cell), 64)
		if err != nil {
			return fmt.Errorf("not a float: %q", cell)
		}
		bld.Float(f)
	case TypeBool:
		switch string(cell) {
		case "Y", "y", "T", "t", "1", "true", "TRUE", "True":
			bld.Bool(true)
		case "N", "n", "F", "f", "0", "false", "FALSE", "False":
			bld.Bool(false)
		default:
			return fmt.Errorf("not a bool: %q", cell)
		}
	case TypeDecimal:
		v, err := parseDecimalCell(cell, c.Scale)
		if err != nil {
			return err
		}
		bld.Value(v)
	case TypeZoned:
		digits, neg, err := zonedDigits(r.scratch, cell)
		if err != nil {
			return err
		}
		r.scratch = digits[:0] // keep the buffer for the next row
		v, err := parseDecimalCell(digits, c.Scale)
		if err != nil {
			return err
		}
		if neg {
			coef, scale := v.Decimal()
			v = record.Decimal(-coef, scale)
		}
		bld.Value(v)
	case TypeTimestamp, TypeDate, TypeTime:
		return r.temporal(bld, i, c, cell)
	default:
		return fmt.Errorf("unknown column type %d", c.Type)
	}
	return nil
}

// parseDecimalCell reads digits as an exact decimal, applying an implied scale
// when the cell carries no decimal point of its own.
func parseDecimalCell(cell []byte, scale int8) (record.Value, error) {
	v, err := record.ParseDecimal(cell)
	if err != nil {
		return record.Value{}, err
	}
	if scale == 0 || bytes.ContainsRune(cell, '.') {
		// An explicit point in the data wins: the file said where it is, and
		// applying the implied scale on top would shift it again.
		return v, nil
	}
	coef, _ := v.Decimal()
	return record.Decimal(coef, scale), nil
}

func (r *Reader) temporal(bld *record.Builder, i int, c Column, cell []byte) error {
	t, err := time.ParseInLocation(layoutFor(c), string(cell), r.locs[i])
	if err != nil {
		return fmt.Errorf("not a %s in layout %q: %q", c.Type, layoutFor(c), cell)
	}
	switch c.Type {
	case TypeDate:
		bld.Value(record.DateAt(t))
	case TypeTime:
		h, m, s := t.Clock()
		bld.TimeOfDay(int64(h)*int64(time.Hour) + int64(m)*int64(time.Minute) +
			int64(s)*int64(time.Second) + int64(t.Nanosecond()))
	default:
		bld.TimestampAt(t)
	}
	return nil
}

// Close releases the reader; it does not close the underlying io.Reader.
func (r *Reader) Close() error {
	r.done = true
	return nil
}
