// Package csvf streams CSV (and fixed-width-adjacent delimited data) to and
// from record batches. Parsing uses encoding/csv in ReuseRecord mode —
// battle-tested quoting semantics with no per-record allocation — and cell
// values land directly in batch arenas.
package csvf

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/aaron-au/shift/engine/record"
)

// ColumnType directs typed parsing of a column.
type ColumnType uint8

// Column types. Untyped columns pass through as strings.
const (
	TypeString ColumnType = iota
	TypeInt
	TypeFloat
	TypeBool
)

// Batch sizing defaults (same rationale as ndjson).
const (
	DefaultBatchRecords = 1024
	DefaultBatchBytes   = 1 << 20
)

// ReaderOptions configure a Reader.
type ReaderOptions struct {
	// Comma is the field delimiter (',' when zero).
	Comma rune
	// NoHeader treats the first row as data; columns are named col0..colN.
	NoHeader bool
	// Types maps column name -> parse type. Unlisted columns are strings.
	Types map[string]ColumnType
	// LazyQuotes / TrimLeadingSpace mirror encoding/csv.
	LazyQuotes       bool
	TrimLeadingSpace bool
	BatchRecords     int
	BatchBytes       int64
}

// Reader streams CSV rows as flat map records. It implements stream.Source;
// the returned batch is valid until the next Next or Close.
type Reader struct {
	cr      *csv.Reader
	opts    ReaderOptions
	batch   *record.Batch
	header  []string
	types   []ColumnType
	started bool
	done    bool
	row     int64
}

// NewReader wraps r.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	if opts.BatchRecords <= 0 {
		opts.BatchRecords = DefaultBatchRecords
	}
	if opts.BatchBytes <= 0 {
		opts.BatchBytes = DefaultBatchBytes
	}
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	if opts.Comma != 0 {
		cr.Comma = opts.Comma
	}
	cr.LazyQuotes = opts.LazyQuotes
	cr.TrimLeadingSpace = opts.TrimLeadingSpace
	return &Reader{cr: cr, opts: opts, batch: record.NewBatch()}
}

func (r *Reader) start() error {
	r.started = true
	if r.opts.NoHeader {
		return nil // header synthesized from the first data row's width
	}
	rec, err := r.cr.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.done = true
			return nil
		}
		return fmt.Errorf("csvf: header: %w", err)
	}
	r.row++
	r.header = make([]string, len(rec))
	copy(r.header, rec) // rec is reused by encoding/csv; header must own its strings
	r.bindTypes()
	return nil
}

func (r *Reader) synthesizeHeader(width int) {
	r.header = make([]string, width)
	for i := range width {
		r.header[i] = "col" + strconv.Itoa(i)
	}
	r.bindTypes()
}

func (r *Reader) bindTypes() {
	r.types = make([]ColumnType, len(r.header))
	for i, h := range r.header {
		r.types[i] = r.opts.Types[h]
	}
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
		rec, err := r.cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.done = true
				break
			}
			return nil, fmt.Errorf("csvf: %w", err)
		}
		r.row++
		if r.header == nil {
			r.synthesizeHeader(len(rec))
		}
		if len(rec) != len(r.header) {
			return nil, fmt.Errorf("csvf: row %d has %d fields, header has %d", r.row, len(rec), len(r.header))
		}
		bld.BeginMap()
		for i, cell := range rec {
			bld.KeyLiteral(r.header[i])
			if err := r.cell(bld, r.types[i], cell); err != nil {
				return nil, fmt.Errorf("csvf: row %d column %q: %w", r.row, r.header[i], err)
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

func (r *Reader) cell(bld *record.Builder, t ColumnType, cell string) error {
	switch t {
	case TypeString:
		bld.StringLiteral(cell)
		return nil
	case TypeInt:
		if cell == "" {
			bld.Null()
			return nil
		}
		n, err := strconv.ParseInt(cell, 10, 64)
		if err != nil {
			return fmt.Errorf("not an int: %q", cell)
		}
		bld.Int(n)
		return nil
	case TypeFloat:
		if cell == "" {
			bld.Null()
			return nil
		}
		f, err := strconv.ParseFloat(cell, 64)
		if err != nil {
			return fmt.Errorf("not a float: %q", cell)
		}
		bld.Float(f)
		return nil
	case TypeBool:
		switch cell {
		case "":
			bld.Null()
		case "true", "TRUE", "True", "1", "t", "T", "yes", "Y", "y":
			bld.Bool(true)
		case "false", "FALSE", "False", "0", "f", "F", "no", "N", "n":
			bld.Bool(false)
		default:
			return fmt.Errorf("not a bool: %q", cell)
		}
		return nil
	default:
		return fmt.Errorf("unknown column type %d", t)
	}
}

// Close releases the reader; it does not close the underlying io.Reader.
func (r *Reader) Close() error {
	r.done = true
	return nil
}

// Header returns the column names once reading has started ("" before).
func (r *Reader) Header() []string { return r.header }
