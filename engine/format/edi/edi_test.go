package edi

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// segments drains a reader into a comparable form: "TAG|el:el|el".
func segments(t *testing.T, in string) ([]string, *Reader) {
	t.Helper()
	r := NewReader(strings.NewReader(in), ReaderOptions{})
	var out []string
	for {
		b, err := r.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			tag, _ := rec.Field("tag")
			els, _ := rec.Field("elements")
			var sb strings.Builder
			sb.WriteString(tag.String())
			for i := range els.Len() {
				sb.WriteString("|")
				e := els.Index(i)
				for j := range e.Len() {
					if j > 0 {
						sb.WriteString(":")
					}
					sb.WriteString(e.Index(j).String())
				}
			}
			out = append(out, sb.String())
		}
	}
	return out, r
}

// A real ISA header is fixed-width, and that is what makes the delimiters
// discoverable: the reader must take them from the DATA, since a partner who
// uses different ones sends a file that is still perfectly valid.
func x12(elem, comp, term string) string {
	isa := "ISA" + elem + "00" + elem + "          " + elem + "00" + elem + "          " +
		elem + "ZZ" + elem + "SENDER         " + elem + "ZZ" + elem + "RECEIVER       " +
		elem + "260801" + elem + "1200" + elem + "U" + elem + "00401" + elem + "000000001" +
		elem + "0" + elem + "P" + elem + comp + term
	return isa
}

func TestX12SegmentsAndDiscoveredDelimiters(t *testing.T) {
	doc := x12("*", ">", "~") +
		"GS*PO*SENDER*RECEIVER*20260801*1200*1*X*004010~" +
		"ST*850*0001~" +
		"BEG*00*SA*3639829**20260801~" +
		"SE*4*0001~GE*1*1~IEA*1*000000001~"

	got, r := segments(t, doc)
	if r.Syntax() != X12 {
		t.Fatalf("syntax = %q, want x12", r.Syntax())
	}
	if len(got) != 7 {
		t.Fatalf("segments = %d, want 7: %v", len(got), got)
	}
	if !strings.HasPrefix(got[0], "ISA|") {
		t.Errorf("first segment = %q", got[0])
	}
	if got[3] != "BEG|00|SA|3639829||20260801" {
		t.Errorf("BEG = %q", got[3])
	}
	if got[6] != "IEA|1|000000001" {
		t.Errorf("IEA = %q", got[6])
	}
}

// The same message with a partner's own delimiters must parse identically.
// Nothing is configured; the ISA header is the only source of truth.
func TestX12DelimitersComeFromTheFileNotFromConfig(t *testing.T) {
	a, _ := segments(t, x12("*", ">", "~")+"BEG*00*SA~")
	b, _ := segments(t, x12("|", "^", "\n")+"BEG|00|SA\n")
	if len(a) != len(b) {
		t.Fatalf("different segment counts: %v vs %v", a, b)
	}
	if a[1] != b[1] || a[1] != "BEG|00|SA" {
		t.Errorf("the same message parsed differently under different delimiters: %q vs %q", a[1], b[1])
	}
}

func TestEDIFACTWithDefaultDelimiters(t *testing.T) {
	doc := "UNB+UNOA:1+SENDER+RECEIVER+260801:1200+1'" +
		"UNH+1+ORDERS:D:96A:UN'" +
		"BGM+220+ORDER123+9'" +
		"UNT+3+1'UNZ+1+1'"

	got, r := segments(t, doc)
	if r.Syntax() != EDIFACT {
		t.Fatalf("syntax = %q, want edifact", r.Syntax())
	}
	if len(got) != 5 {
		t.Fatalf("segments = %d, want 5: %v", len(got), got)
	}
	// A composite element keeps its components addressable and in order.
	if got[1] != "UNH|1|ORDERS:D:96A:UN" {
		t.Errorf("UNH = %q", got[1])
	}
	if got[0] != "UNB|UNOA:1|SENDER|RECEIVER|260801:1200|1" {
		t.Errorf("UNB = %q", got[0])
	}
}

// UNA overrides every delimiter. A reader that ignored it would split the
// whole interchange on the wrong bytes and produce confident nonsense.
func TestEDIFACTHonoursTheUNAHeader(t *testing.T) {
	doc := "UNA=*.? " + "|" + "UNB*UNOA=1*SENDER|BGM*220*ORDER123|"
	got, r := segments(t, doc)
	if r.Syntax() != EDIFACT {
		t.Fatalf("syntax = %q", r.Syntax())
	}
	if len(got) != 2 {
		t.Fatalf("segments = %d, want 2: %v", len(got), got)
	}
	// '=' is now the COMPONENT separator, so UNOA=1 is a composite and splits;
	// under the ISO defaults it would not. That difference is the proof the
	// UNA header was applied.
	if got[0] != "UNB|UNOA:1|SENDER" {
		t.Errorf("UNB = %q — the UNA delimiters were not applied", got[0])
	}
}

// The release character escapes the NEXT byte, terminator included. Without
// it, a value containing an apostrophe ends its segment early and every
// segment after it is misaligned.
func TestTheReleaseCharacterProtectsDelimiters(t *testing.T) {
	doc := "UNB+SENDER'" + "NAD+BY+O?'BRIEN LTD+X?+Y'" + "UNZ+1'"
	got, _ := segments(t, doc)
	if len(got) != 3 {
		t.Fatalf("segments = %d, want 3 — an escaped delimiter ended a segment: %v", len(got), got)
	}
	// Escapes are removed on the way out: a flow should see the text, not the
	// wire encoding.
	if got[1] != "NAD|BY|O'BRIEN LTD|X+Y" {
		t.Errorf("NAD = %q, want the unescaped text", got[1])
	}
}

// Partners routinely pretty-print one segment per line. Those breaks are not
// data and must not end up prefixed to the next tag.
func TestLineBreaksBetweenSegmentsAreNotData(t *testing.T) {
	doc := x12("*", ">", "~") + "\r\n" + "GS*PO*A*B~\r\n" + "ST*850*0001~\r\n"
	got, _ := segments(t, doc)
	if len(got) != 3 {
		t.Fatalf("segments = %d, want 3: %v", len(got), got)
	}
	if got[1] != "GS|PO|A|B" {
		t.Errorf("GS = %q — a line break leaked into the tag", got[1])
	}
}

// An element is ALWAYS a list of components, even when it holds one value or
// none. A field whose shape varies breaks every mapping the first time a
// partner sends a composite.
func TestAnElementIsAlwaysAListOfComponents(t *testing.T) {
	r := NewReader(strings.NewReader(x12("*", ">", "~")+"BEG*00**SA>X~"), ReaderOptions{})
	b, err := r.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rec := b.Record(1)
	els, ok := rec.Field("elements")
	if !ok || els.Kind() != record.KindList {
		t.Fatalf("elements is %v, want a list", els.Kind())
	}
	for i := range els.Len() {
		if els.Index(i).Kind() != record.KindList {
			t.Fatalf("element %d is %v, want a list even when single-valued", i, els.Index(i).Kind())
		}
	}
	if n := els.Index(1).Len(); n != 0 {
		t.Errorf("the empty element has %d components, want 0", n)
	}
	if els.Index(2).Len() != 2 {
		t.Errorf("the composite element did not split into components")
	}
}

// A file that is not an interchange must say so, rather than emit garbage
// segments that look like data.
func TestNonInterchangeInputIsRejected(t *testing.T) {
	cases := map[string]string{
		"json":     `{"not":"edi"}`,
		"empty":    "",
		"truncISA": "ISA*00*",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewReader(strings.NewReader(in), ReaderOptions{})
			_, err := r.Next(t.Context())
			if err == nil {
				t.Fatal("accepted non-interchange input")
			}
			if err == io.EOF { //nolint:errorlint // identity is the point: a BARE EOF would mean "clean end of a valid file"
				t.Fatal("reported a clean end for input that never carried an interchange header")
			}
		})
	}
}

// A missing terminator must not buffer the whole file looking for one.
func TestARunawaySegmentIsBounded(t *testing.T) {
	doc := x12("*", ">", "~") + "BEG*" + strings.Repeat("x", 5000)
	r := NewReader(strings.NewReader(doc), ReaderOptions{MaxSegmentBytes: 1024})
	_, err := r.Next(t.Context())
	if err == nil {
		t.Fatal("an unterminated segment was accepted")
	}
	if !strings.Contains(err.Error(), "1024") {
		t.Errorf("error does not mention the bound: %v", err)
	}
}

// A final segment with no terminator is complete data; discarding it would
// silently drop the trailer.
func TestAFinalSegmentWithoutATerminatorIsKept(t *testing.T) {
	got, _ := segments(t, x12("*", ">", "~")+"IEA*1*000000001")
	if len(got) != 2 {
		t.Fatalf("segments = %d, want 2: %v", len(got), got)
	}
	if got[1] != "IEA|1|000000001" {
		t.Errorf("trailer = %q", got[1])
	}
}
