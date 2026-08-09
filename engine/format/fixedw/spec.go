// Package fixedw streams fixed-width (column-positional) records to and from
// record batches — the last of ADR-0004's first-class workloads.
//
// A fixed-width file has no delimiters and no self-description: the layout is
// the contract, held somewhere else entirely, and a field is only findable by
// counting bytes. Two consequences shape this package.
//
// First, the layout is declared, so this is where the exact kinds of ADR-0051
// earn their keep. Zoned decimals and implied decimal places are the format's
// native vocabulary — an amount is written "0001010" and means 10.10 — and
// reading that as a float64 would reintroduce, at the very first step, exactly
// the rounding the declaration exists to avoid.
//
// Second, everything is positional, so an off-by-one is silent: the fields
// still parse, they just hold the wrong bytes. This package therefore refuses
// rather than guesses. A short record is an error, a value too wide for its
// column is an error, and unexpected trailing content is an error.
package fixedw

import (
	"errors"
	"fmt"
	"strconv"
)

// Align says which end of a column a value sits against; the rest is padding.
type Align uint8

// Column alignments. The zero value defers to the column's type: numbers are
// right-aligned, everything else left-aligned, which is what the formats in
// the wild do.
const (
	AlignDefault Align = iota
	AlignLeft
	AlignRight
)

// Trim says how much padding to strip from a cell on read.
type Trim uint8

// Trim modes. The zero value picks by pad character, which is not a
// convenience — it is a correctness rule.
//
// Trimming both ends of a ZERO-padded number destroys it: "000100" loses its
// trailing zeros and reads as 1. So a non-space pad byte is stripped only from
// the side the padding is actually on (the side opposite the alignment), while
// a space pad — which is never part of a number and almost never part of a
// name — is stripped from both. TrimNone exists for the columns where a
// leading space really is data.
const (
	TrimDefault Trim = iota
	TrimBoth
	TrimLeft
	TrimRight
	TrimNone
)

// ColumnType directs parsing and rendering of a column.
type ColumnType uint8

// Column types.
const (
	// TypeString takes the cell as text.
	TypeString ColumnType = iota
	TypeInt
	TypeFloat
	TypeBool
	// TypeDecimal is an exact decimal. With Scale set and no decimal point in
	// the cell, the digits are read as a coefficient at that implied scale —
	// "0001010" at scale 2 is 10.10, which is how amounts are almost always
	// written in this format.
	TypeDecimal
	// TypeZoned is a signed decimal whose last byte carries both the final
	// digit and the sign, as an overpunch: "0001010{" is +10100, "0001010}"
	// is -10100. A COBOL signed DISPLAY field, and unreadable without this.
	TypeZoned
	TypeTimestamp
	TypeDate
	TypeTime
)

func (t ColumnType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeBool:
		return "bool"
	case TypeDecimal:
		return "decimal"
	case TypeZoned:
		return "zoned"
	case TypeTimestamp:
		return "timestamp"
	case TypeDate:
		return "date"
	case TypeTime:
		return "time"
	default:
		return "invalid"
	}
}

// isNumeric reports whether the type right-aligns by default.
func (t ColumnType) isNumeric() bool {
	switch t {
	case TypeInt, TypeFloat, TypeDecimal, TypeZoned:
		return true
	default:
		return false
	}
}

// Column is one field of the layout.
//
// There is deliberately no start offset. Columns are contiguous and in order,
// and a gap is expressed as a column with no Name — a filler, exactly as the
// copybooks that produce these files express it. That removes a whole class of
// layout bug: offsets that no longer add up after an edit, silently reading
// the wrong bytes rather than failing.
type Column struct {
	// Name is the record field name. Empty means filler: skipped on read,
	// written as padding.
	Name string
	// Width is the column's size in bytes. Required, and must be positive.
	Width int
	// Type directs parsing (default TypeString).
	Type ColumnType
	// Scale is the implied number of decimal places for TypeDecimal and
	// TypeZoned when the cell carries no decimal point.
	Scale int8
	// Align places the value within the column (default: by type).
	Align Align
	// Pad fills the rest of the column (default: space).
	Pad byte
	// Trim strips padding on read. The default depends on the pad byte — see
	// the Trim constants, where the reason is a correctness one.
	Trim Trim
	// Layout is the time layout for the temporal types. Defaults are
	// "20060102" for a date, "150405" for a time, and "20060102150405" for a
	// timestamp — the packed forms these files use.
	Layout string
	// Location names the zone a timestamp or date is written in ("" = UTC).
	// A fixed-width timestamp carries no offset, so it has to be declared or
	// assumed, and assuming silently is how a date lands a day out.
	Location string
}

// filler reports whether the column is unnamed padding.
func (c Column) filler() bool { return c.Name == "" }

// resolved is a validated layout: defaults applied, offsets computed once.
type resolved struct {
	cols   []Column
	starts []int
	length int
}

// resolve validates the layout and precomputes each column's offset.
func resolve(cols []Column) (*resolved, error) {
	if len(cols) == 0 {
		return nil, errors.New("fixedw: layout has no columns")
	}
	r := &resolved{
		cols:   make([]Column, len(cols)),
		starts: make([]int, len(cols)),
	}
	seen := make(map[string]int, len(cols))
	off := 0
	for i, c := range cols {
		if c.Width <= 0 {
			return nil, fmt.Errorf("fixedw: column %d (%s) needs a positive width", i, describeCol(c))
		}
		if !c.filler() {
			if prev, dup := seen[c.Name]; dup {
				return nil, fmt.Errorf("fixedw: column %q appears at both %d and %d; "+
					"a record cannot have two fields of the same name", c.Name, prev, i)
			}
			seen[c.Name] = i
		}
		if c.Align == AlignDefault {
			if c.Type.isNumeric() {
				c.Align = AlignRight
			} else {
				c.Align = AlignLeft
			}
		}
		if c.Pad == 0 {
			c.Pad = ' '
		}
		if c.Trim == TrimDefault {
			// See the Trim docs: stripping a zero pad from both ends would
			// turn "000100" into "1".
			switch {
			case c.Pad == ' ':
				c.Trim = TrimBoth
			case c.Align == AlignRight:
				c.Trim = TrimLeft
			default:
				c.Trim = TrimRight
			}
		}
		r.cols[i] = c
		r.starts[i] = off
		off += c.Width
	}
	r.length = off
	return r, nil
}

func describeCol(c Column) string {
	if c.filler() {
		return "filler"
	}
	return strconv.Quote(c.Name)
}

// Length returns the byte length of one record for a layout.
func Length(cols []Column) (int, error) {
	r, err := resolve(cols)
	if err != nil {
		return 0, err
	}
	return r.length, nil
}

// layoutFor returns the time layout for a temporal column.
func layoutFor(c Column) string {
	if c.Layout != "" {
		return c.Layout
	}
	switch c.Type {
	case TypeDate:
		return "20060102"
	case TypeTime:
		return "150405"
	default:
		return "20060102150405"
	}
}

// trimCell strips padding according to the column's trim mode, returning a
// sub-slice of cell. It allocates nothing: the result still views the read
// buffer, and only becomes arena-backed when the builder copies it in.
//
// Both the column's pad byte and a plain space are stripped, because a file
// that zero-pads its numbers still space-pads a short one somewhere. resolve
// has already turned TrimDefault into a concrete side.
func trimCell(cell []byte, c Column) []byte {
	if c.Trim == TrimNone {
		return cell
	}
	isPad := func(b byte) bool { return b == c.Pad || b == ' ' }
	lo, hi := 0, len(cell)
	if c.Trim != TrimRight {
		for lo < hi && isPad(cell[lo]) {
			lo++
		}
	}
	if c.Trim != TrimLeft {
		for hi > lo && isPad(cell[hi-1]) {
			hi--
		}
	}
	return cell[lo:hi]
}
