package edi

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// FuzzReader drives the EDI reader with arbitrary bytes. An interchange comes
// from a trading partner nobody here controls, and — uniquely among the format
// readers — its DELIMITERS come from the same bytes: a hostile header is a
// hostile parser configuration, not merely hostile content. The property is
// robustness (error, never panic/hang/unbounded buffering) plus the one bound
// the package exists to hold: MaxSegmentBytes, which is what stops an
// interchange with no terminator in it from buffering the whole file.
func FuzzReader(f *testing.F) {
	// Real interchanges, one per syntax, so the fuzzer starts from something
	// that reaches the segment loop rather than dying in the sniffer.
	f.Add([]byte(x12("*", ">", "~") +
		"GS*PO*SENDER*RECEIVER*20260801*1200*1*X*004010~ST*850*0001~" +
		"BEG*00*SA*3639829**20260801~SE*4*0001~GE*1*1~IEA*1*000000001~"))
	f.Add([]byte("UNA:+.? 'UNB+UNOC:3+SENDER+RECEIVER+260801:1200+1'" +
		"UNH+1+ORDERS:D:96A:UN'BGM+220+123456+9'UNT+3+1'UNZ+1+1'"))
	f.Add([]byte("UNB+UNOC:3+S+R+260801:1200+1'BGM+220+12?'34+9'UNZ+1+1'")) // release escapes a terminator
	f.Add([]byte("UNB+a+b'BGM+1+2?"))                                       // dangling release at EOF

	f.Add([]byte(x12("*", ">", "~")[:60]))          // ISA truncated mid-header
	f.Add([]byte(x12("\x00", ">", "~")))            // NUL declared as the element separator
	f.Add([]byte(x12("*", ">", "*")))               // element and segment terminator collide
	f.Add([]byte(x12("~", "~", "~")))               // every delimiter the same byte
	f.Add([]byte("ISA" + strings.Repeat("*", 200))) // ISA-shaped, no terminator in sight
	f.Add([]byte("UNB+" + strings.Repeat("\xff", 64) + "'"))
	f.Add([]byte("UNB+a'" + strings.Repeat("+", 4096) + "'")) // one segment, thousands of empty elements
	f.Add([]byte("UNB+a'" + strings.Repeat(":", 4096) + "'")) // one element, thousands of components
	f.Add([]byte("\n\r\t   "))                                // whitespace only: the sniffer must not spin
	f.Add([]byte("not an interchange"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32<<10 {
			return // the interesting shapes are structural; size only slows the corpus down
		}
		const (
			maxSeg   = 512
			perBatch = 4
		)
		r := NewReader(bytes.NewReader(data), ReaderOptions{
			BatchRecords:    perBatch,
			MaxSegmentBytes: maxSeg,
		})
		ctx := context.Background()
		// Drained to exhaustion so the whole state machine runs, with a hard
		// iteration cap: a target that can hang poisons the fuzzing job.
		for range 10000 {
			b, err := r.Next(ctx)
			if err != nil {
				break
			}
			if b.Len() > perBatch {
				t.Fatalf("batch holds %d segments, BatchRecords is %d", b.Len(), perBatch)
			}
			for _, rec := range b.Records() {
				if n := segmentTextBytes(rec); n > maxSeg {
					t.Fatalf("segment carried %d bytes of text past a %d-byte cap", n, maxSeg)
				}
			}
		}
		_ = r.Close()
	})
}

// segmentTextBytes totals the text a segment record carries. Splitting and
// unescaping only ever remove bytes, so this can never exceed the segment the
// reader read — unless the cap was not enforced.
func segmentTextBytes(rec record.Value) int {
	tag, _ := rec.Field("tag")
	n := len(tag.Bytes())
	els, _ := rec.Field("elements")
	for i := range els.Len() {
		e := els.Index(i)
		for j := range e.Len() {
			n += len(e.Index(j).Bytes())
		}
	}
	return n
}
