package csvf

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
)

// FuzzReader drives the CSV reader with arbitrary bytes, an arbitrary
// delimiter, and the typed-column paths switched on. The delimiter matters:
// it is per-flow config, so a hostile file and a wrong `comma` produce the same
// class of mess, and encoding/csv's own error surface (bare quotes, ragged
// rows, an invalid delimiter) must reach the caller as an error rather than a
// panic. The typed columns are where the untrusted bytes meet the exact
// decimal/temporal parsers, which is the half a plain CSV fuzzer would miss.
func FuzzReader(f *testing.F) {
	f.Add(byte(','), byte(0), []byte("a,b,c\n1,10.10,2026-08-01\n2,0.5,2026-08-02\n"))
	f.Add(byte(','), byte(1), []byte("1,10.10,2026-08-01T00:00:00Z\n")) // NoHeader: col0..colN
	f.Add(byte(';'), byte(0), []byte("a;b\n\"quoted;field\";\"line\nbreak\"\n"))
	f.Add(byte(','), byte(0), []byte("a,b\n\"unterminated,x\n"))
	f.Add(byte(','), byte(2), []byte("a,b\n\"bare\"quote\",x\n")) // only parses under LazyQuotes
	f.Add(byte(','), byte(0), []byte("a,b\n1\n1,2,3\n"))          // ragged rows
	f.Add(byte(','), byte(0), []byte("a,b\r\n1,2\r\n"))
	f.Add(byte(','), byte(0), []byte("\xef\xbb\xbfa,b\n1,2\n"))                      // BOM ahead of the header
	f.Add(byte(','), byte(0), []byte("a,b\n\x00,\xff\xfe\n"))                        // NUL + invalid UTF-8 cells
	f.Add(byte(','), byte(0), []byte("a\n"+strings.Repeat("9", 9000)+"\n"))          // one very long cell
	f.Add(byte(','), byte(1), []byte(strings.Repeat("x,", 10000)+"x\n"))             // one very wide row
	f.Add(byte('"'), byte(0), []byte("a\"b\n"))                                      // the quote char as delimiter
	f.Add(byte('\n'), byte(0), []byte("a\nb\n"))                                     // a delimiter csv refuses
	f.Add(byte(','), byte(0), []byte("a,b\n99999999999999999999999999,10e999999\n")) // numbers past every range
	f.Add(byte(','), byte(0), []byte{})

	f.Fuzz(func(t *testing.T, comma, mode byte, data []byte) {
		if len(data) > 32<<10 {
			return // bounded work per input
		}
		opts := ReaderOptions{
			Comma:            rune(comma),
			NoHeader:         mode&1 != 0,
			LazyQuotes:       mode&2 != 0,
			TrimLeadingSpace: mode&4 != 0,
			Types:            fuzzTypes,
			BatchRecords:     4, // small, so the batch loop runs several times
			BatchBytes:       4 << 10,
		}
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
				// The header is the record's shape; a row that produced a
				// different field count was accepted when it should not have been.
				if rec.Len() != len(r.Header()) {
					t.Fatalf("record has %d fields, header has %d", rec.Len(), len(r.Header()))
				}
			}
		}
		_ = r.Close()
	})
}

// fuzzTypes types the column names a fuzzer can plausibly produce — the
// synthesized col0..colN and short header names — so the exact parsers are on
// the path for most inputs rather than for the rare one that guesses a name.
var fuzzTypes = func() map[string]ColumnType {
	types := []ColumnType{TypeInt, TypeFloat, TypeBool, TypeDecimal, TypeTimestamp, TypeDate, TypeTime}
	m := make(map[string]ColumnType, 2*len(types))
	for i, t := range types {
		m["col"+strconv.Itoa(i)] = t
		m[string(rune('a'+i))] = t
	}
	return m
}()
