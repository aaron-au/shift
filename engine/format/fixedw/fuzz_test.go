package fixedw

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
)

// FuzzReader fuzzes the LAYOUT alongside the data, because in this format they
// are one input: the file is unlabelled bytes and the layout decides what they
// mean, so the failure mode the package exists to prevent — the two disagreeing
// — is only reachable by varying both. A disagreement must surface as an error,
// never a panic or a silent misread.
//
// spec is decoded into columns (see layoutFrom); data is the extract.
func FuzzReader(f *testing.F) {
	// mode byte, then 3 bytes per column: width, type, flags.
	f.Add([]byte{0, 0, 1, 0, 0, 0, 0}, []byte("0001alice\n0002bob  \n"))
	f.Add([]byte{1, 0, 1, 0, 0, 0, 0}, []byte("0001alice0002bob  ")) // RECFM=F, no terminators
	f.Add([]byte{0, 0, 5, 2, 0, 0, 0}, []byte("0001010{\n"))         // zoned overpunch: the sign is the last byte
	f.Add([]byte{0, 0, 6, 0}, []byte("20260801\n20261332\n"))        // a date, then one that is not
	f.Add([]byte{0, 255, 1, 0}, []byte("1\n"))                       // a column far wider than the record
	f.Add([]byte{1, 255, 0, 0}, []byte("short"))                     // huge declared width, unseparated
	f.Add([]byte{0, 0, 0, 0}, []byte(strings.Repeat("x", 9000)))     // line past bufio's buffer: the copy fallback
	f.Add([]byte{0, 0, 0, 0}, []byte("a\x00b\n\xff\xfe\n"))          // NUL and invalid UTF-8 inside cells
	f.Add([]byte{0, 0, 1, 0, 0, 1, 0}, []byte("\n\n   \n"))          // blank lines are padding, not records
	f.Add(bytes.Repeat([]byte{3}, 40), []byte("aaaa\n"))             // more columns than distinct names
	f.Add([]byte{0}, []byte("x"))                                    // no columns at all
	f.Add([]byte{}, []byte{})

	f.Fuzz(func(t *testing.T, spec, data []byte) {
		if len(spec) > 64 || len(data) > 32<<10 {
			return // bounded work per input; the shapes of interest are all small
		}
		cols, opts := layoutFrom(spec)
		if len(cols) == 0 {
			return
		}
		named := 0
		for _, c := range cols {
			if !c.filler() {
				named++
			}
		}
		opts.Columns = cols
		r := NewReader(bytes.NewReader(data), opts)
		ctx := context.Background()
		for range 10000 {
			b, err := r.Next(ctx)
			if err != nil {
				break
			}
			if b.Len() > opts.BatchRecords {
				t.Fatalf("batch holds %d records, BatchRecords is %d", b.Len(), opts.BatchRecords)
			}
			for _, rec := range b.Records() {
				// A positional reader that emits a different field count than
				// the layout declares has read the wrong bytes for something.
				if rec.Len() != named {
					t.Fatalf("record has %d fields, layout names %d columns", rec.Len(), named)
				}
			}
		}
		_ = r.Close()
	})
}

// layoutFrom decodes fuzz bytes into a layout. Widths are scaled up (to a few
// KiB) rather than left at 0-255 so that over-wide columns and the whole-record
// buffer in Unseparated mode are exercised, and capped because a width is
// operator config: the discovery value is in the parse paths, not in proving
// that make() can allocate.
func layoutFrom(spec []byte) ([]Column, ReaderOptions) {
	if len(spec) == 0 {
		return nil, ReaderOptions{}
	}
	opts := ReaderOptions{
		Unseparated:  spec[0]&1 != 0,
		SkipLines:    int(spec[0]>>1) % 3,
		BatchRecords: 4, // small, so the batch loop runs several times per input
		BatchBytes:   4 << 10,
	}
	pads := [4]byte{' ', '0', '*', 0}
	var cols []Column
	for i := 1; i+2 < len(spec) && len(cols) < 16; i += 3 {
		width, typ, flags := spec[i], spec[i+1], spec[i+2]
		c := Column{
			Width: 1 + int(width)*17,
			// %10 reaches one past the last defined type, so the "unknown column
			// type" arm is a case the fuzzer can hit rather than dead code.
			Type:  ColumnType(typ % 10),
			Align: Align(flags % 3),
			Trim:  Trim((flags >> 2) % 5),
			Pad:   pads[(flags>>5)%4],
			Scale: int8(flags%7) - 3, //nolint:gosec // 0..6 minus 3 is in range
		}
		if flags&1 == 0 {
			// Names repeat past 8 columns, so the duplicate-name refusal is
			// reachable too.
			c.Name = "c" + strconv.Itoa(len(cols)%8)
		}
		cols = append(cols, c)
	}
	return cols, opts
}
