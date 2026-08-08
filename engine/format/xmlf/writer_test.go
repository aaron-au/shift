package xmlf

import (
	"bytes"
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
