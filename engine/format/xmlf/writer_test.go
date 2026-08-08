package xmlf

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// dump renders a value the same way for two documents, so a round-trip
// comparison does not depend on the writer being correct to describe what the
// reader produced.
func dump(t *testing.T, v record.Value) string {
	t.Helper()
	var sb strings.Builder
	var walk func(record.Value)
	walk = func(v record.Value) {
		switch v.Kind() {
		case record.KindMap:
			sb.WriteString("{")
			for i := range v.Len() {
				sb.Write(v.KeyAt(i))
				sb.WriteString(":")
				walk(v.Index(i))
				sb.WriteString(" ")
			}
			sb.WriteString("}")
		case record.KindList:
			sb.WriteString("[")
			for i := range v.Len() {
				walk(v.Index(i))
				sb.WriteString(" ")
			}
			sb.WriteString("]")
		default:
			sb.WriteString(v.String())
		}
	}
	walk(v)
	return sb.String()
}

func readAll(t *testing.T, doc, elem string) []string {
	t.Helper()
	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: elem})
	defer func() { _ = r.Close() }()
	var out []string
	for {
		b, err := r.Next(t.Context())
		if err != nil {
			break
		}
		for _, rec := range b.Records() {
			out = append(out, dump(t, rec))
		}
	}
	return out
}

// The writer is only worth having if it is the reader's inverse. Anything the
// reader can produce — attributes, mixed content, repeats collapsed to lists,
// nesting — has to come back out with the same shape, or XML is a one-way
// door and every flow that reads XML can only ever write something else.
func TestWhatTheReaderProducesTheWriterReproduces(t *testing.T) {
	const doc = `<data>
	  <row id="1" active="true">
	    <name>ada</name>
	    <price currency="USD">10.5</price>
	    <tag>a</tag>
	    <tag>b</tag>
	    <note>hello<b>x</b>world</note>
	    <nested><deep><leaf>v</leaf></deep></nested>
	  </row>
	  <row id="2"><name>bob</name><empty></empty></row>
	</data>`

	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "row"})
	b, err := r.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	before := make([]string, 0, b.Len())
	for _, rec := range b.Records() {
		before = append(before, dump(t, rec))
	}

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{RecordElement: "row", Indent: "  "})
	if err := w.Write(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()

	after := readAll(t, buf.String(), "row")
	if len(after) != len(before) {
		t.Fatalf("round-tripped %d records, wrote %d\n%s", len(after), len(before), buf.String())
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("record %d did not survive the round trip:\n before %s\n  after %s\n\n%s",
				i, before[i], after[i], buf.String())
		}
	}
}

// Metacharacters must not be able to close an element early. This is the
// injection case: a value containing "</row>" that was written literally would
// let payload restructure the document around it.
func TestTextCannotEscapeItsElement(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("evil")
	bld.StringLiteral(`</row><injected>x</injected><row>`)
	bld.KeyLiteral("amp")
	bld.StringLiteral(`a & b < c > d "q"`)
	bld.EndMap()
	b.Append(bld.Finish())

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "<injected>") {
		t.Fatalf("a value closed its own element and injected markup:\n%s", buf.String())
	}

	got := readAll(t, buf.String(), "row")
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1 — the document was restructured:\n%s", len(got), buf.String())
	}
	if !strings.Contains(got[0], `</row><injected>x</injected><row>`) {
		t.Errorf("the escaped text did not come back verbatim: %s", got[0])
	}
}

// Attributes need escaping the text case does not: a quote ends the value, and
// a raw newline or tab is normalised to a space by every conformant parser, so
// round-tripping them requires character references.
func TestAttributesEscapeQuotesAndWhitespace(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("@quote")
	bld.StringLiteral(`he said "hi"`)
	bld.KeyLiteral("@lines")
	bld.StringLiteral("a\nb\tc")
	bld.EndMap()
	b.Append(bld.Finish())

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAll(t, buf.String(), "row")
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1:\n%s", len(got), buf.String())
	}
	if !strings.Contains(got[0], `he said "hi"`) {
		t.Errorf("quotes did not survive: %s", got[0])
	}
	if !strings.Contains(got[0], "a\nb\tc") {
		t.Errorf("attribute whitespace was normalised away: %q", got[0])
	}
}

// Zero records is a legitimate result. Emitting an empty FILE would make a
// consumer fail to parse where it should read "nothing matched".
func TestNoRecordsStillWritesADocument(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "<"+DefaultRootElement+">") ||
		!strings.Contains(buf.String(), "</"+DefaultRootElement+">") {
		t.Fatalf("empty output is not a parseable document: %q", buf.String())
	}
}

// A fragment writer is for appending to a document somebody else opened, so it
// must emit neither a prolog nor a root.
func TestAFragmentHasNoRootOrProlog(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.StringLiteral("1")
	bld.EndMap()
	b.Append(bld.Finish())

	var buf bytes.Buffer
	w := NewFragmentWriter(&buf, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "<?xml") || strings.Contains(out, DefaultRootElement) {
		t.Fatalf("fragment carried a prolog or root: %q", out)
	}
	if out != "<row><a>1</a></row>" {
		t.Fatalf("fragment = %q", out)
	}
}

// Depth is bounded on the way out as well as in: a record deep enough to
// exhaust the stack must fail, not take the process with it.
func TestNestingIsBounded(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	const depth = 12
	for range depth {
		bld.BeginMap()
		bld.KeyLiteral("n")
	}
	bld.StringLiteral("leaf")
	for range depth {
		bld.EndMap()
	}
	b.Append(bld.Finish())

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{RecordElement: "row", MaxDepth: 4})
	if err := w.Write(t.Context(), b); err == nil {
		t.Fatal("a record nested past MaxDepth was written")
	}
}

// XML carries no types, so every scalar has to render to text deliberately.
// A wrong rendering here is silent: the document is well-formed and the value
// is wrong.
func TestEveryScalarKindRendersAsText(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(-42)
	bld.KeyLiteral("f")
	bld.Float(10.5)
	bld.KeyLiteral("t")
	bld.Bool(true)
	bld.KeyLiteral("z")
	bld.Null()
	bld.EndMap()
	b.Append(bld.Finish())

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"<i>-42</i>", "<f>10.5</f>", "<t>true</t>", "<z></z>"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
	// An empty element closes long-form, so a diff of input against output
	// matches what the reader maps back to "".
	if strings.Contains(out, "<z/>") {
		t.Error("an empty value was written self-closing; the reader maps both to \"\" but the long form round-trips textually")
	}
}

// Infinity and NaN have no XML notation. Writing "NaN" would read back as the
// STRING "NaN", turning a broken number into plausible text.
func TestNonFiniteNumbersAreRefused(t *testing.T) {
	for name, f := range map[string]float64{
		"infinity":     math.Inf(1),
		"negative inf": math.Inf(-1),
		"NaN":          math.NaN(),
	} {
		t.Run(name, func(t *testing.T) {
			b := record.NewBatch()
			bld := b.Builder()
			bld.BeginMap()
			bld.KeyLiteral("v")
			bld.Float(f)
			bld.EndMap()
			b.Append(bld.Finish())

			w := NewWriter(&bytes.Buffer{}, WriterOptions{RecordElement: "row"})
			if err := w.Write(t.Context(), b); err == nil {
				t.Fatalf("%v was written as if it had an XML representation", f)
			}
		})
	}
}

// A container where a scalar belongs — an attribute or #text holding a map —
// cannot be rendered, and guessing would produce a document that parses and
// means something else.
func TestAContainerWhereAScalarBelongsIsRefused(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("@attr")
	bld.BeginList()
	bld.StringLiteral("a")
	bld.EndList()
	bld.EndMap()
	b.Append(bld.Finish())

	w := NewWriter(&bytes.Buffer{}, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err == nil {
		t.Fatal("a list was written as an attribute value")
	}
}

// Indentation is for humans; the reader ignores inter-element whitespace, so
// the records must survive it unchanged.
func TestIndentingDoesNotChangeTheRecords(t *testing.T) {
	const doc = `<data><row id="1"><name>ada</name><tag>a</tag><tag>b</tag></row></data>`
	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "row"})
	b, err := r.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	before := dump(t, b.Record(0))

	var plain, pretty bytes.Buffer
	for _, tc := range []struct {
		buf    *bytes.Buffer
		indent string
	}{{&plain, ""}, {&pretty, "\t"}} {
		w := NewWriter(tc.buf, WriterOptions{RecordElement: "row", Indent: tc.indent})
		if err := w.Write(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	_ = r.Close()

	if !strings.Contains(pretty.String(), "\n\t") {
		t.Error("Indent produced no indentation")
	}
	for name, out := range map[string]string{"plain": plain.String(), "pretty": pretty.String()} {
		got := readAll(t, out, "row")
		if len(got) != 1 || got[0] != before {
			t.Errorf("%s output did not round-trip: %v", name, got)
		}
	}
}

// A failing destination must surface, and must keep failing rather than
// reporting success on Close once the document is already truncated.
func TestAWriteFailureIsStickyNotSwallowed(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.StringLiteral(strings.Repeat("x", 128<<10)) // past bufio's buffer, so it flushes
	bld.EndMap()
	b.Append(bld.Finish())

	w := NewWriter(errWriter{}, WriterOptions{RecordElement: "row"})
	if err := w.Write(t.Context(), b); err == nil {
		t.Fatal("a failing destination reported success")
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close reported success after the document was already truncated")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("device full") }

// failAfter fails once n bytes have been accepted, so a failure can be placed
// at any point in the document.
type failAfter struct {
	n int
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("device full")
	}
	if len(p) > f.n {
		w := f.n
		f.n = 0
		return w, errors.New("device full")
	}
	f.n -= len(p)
	return len(p), nil
}

// A destination that fails PART WAY through must surface the error wherever it
// happens — prolog, attribute, text, or the closing root. The failure mode
// this guards against is a writer that swallows an I/O error and reports a
// clean Close, leaving a truncated file that every later reader treats as
// authoritative.
func TestAFailureAtAnyPointInTheDocumentSurfaces(t *testing.T) {
	build := func() *record.Batch {
		b := record.NewBatch()
		bld := b.Builder()
		bld.BeginMap()
		bld.KeyLiteral("@id")
		bld.StringLiteral("1")
		bld.KeyLiteral("#text")
		bld.StringLiteral("a & b")
		bld.EndMap()
		b.Append(bld.Finish())
		return b
	}

	// Enough points to land inside the prolog, the open tag, the attribute,
	// the escaped text and the trailer.
	for _, n := range []int{0, 1, 8, 20, 40, 48, 56} {
		w := NewWriter(&failAfter{n: n}, WriterOptions{RecordElement: "row", Indent: "  "})
		werr := w.Write(t.Context(), build())
		cerr := w.Close()
		if werr == nil && cerr == nil {
			t.Errorf("failing after %d bytes produced a clean write AND a clean close; the file is truncated and nothing says so", n)
		}
	}
}
