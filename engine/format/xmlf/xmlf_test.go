package xmlf

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// field is a small test helper: fetch a map field or fail.
func field(t *testing.T, v record.Value, name string) record.Value {
	t.Helper()
	f, ok := v.Field(name)
	if !ok {
		t.Fatalf("missing field %q in %v", name, v.Kind())
	}
	return f
}

func TestReadRows(t *testing.T) {
	const doc = `<data>
	  <row id="1" active="true">
	    <name>ada</name>
	    <price currency="USD">10.5</price>
	    <tag>a</tag>
	    <tag>b</tag>
	    <note>hello<b>x</b>world</note>
	  </row>
	  <row id="2"><name>bob</name></row>
	</data>`

	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "row"})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 2 {
		t.Fatalf("len = %d, want 2", b.Len())
	}

	row0 := b.Record(0)
	// Attributes under "@name".
	if v := field(t, row0, "@id"); v.String() != "1" {
		t.Errorf("@id = %q, want 1", v.String())
	}
	if v := field(t, row0, "@active"); v.String() != "true" {
		t.Errorf("@active = %q (XML text stays a string, never coerced)", v.String())
	}
	// Leaf child → bare string.
	if v := field(t, row0, "name"); v.Kind() != record.KindString || v.String() != "ada" {
		t.Errorf("name = %v/%q, want string ada", v.Kind(), v.String())
	}
	// Element with an attribute + text → map with @attr and #text.
	price := field(t, row0, "price")
	if price.Kind() != record.KindMap {
		t.Fatalf("price kind = %v, want map", price.Kind())
	}
	if v := field(t, price, "@currency"); v.String() != "USD" {
		t.Errorf("price/@currency = %q", v.String())
	}
	if v := field(t, price, "#text"); v.String() != "10.5" {
		t.Errorf("price/#text = %q, want 10.5", v.String())
	}
	// Repeated child name → list in document order.
	tags := field(t, row0, "tag")
	if tags.Kind() != record.KindList || tags.Len() != 2 {
		t.Fatalf("tag = %v len %d, want list of 2", tags.Kind(), tags.Len())
	}
	if tags.Index(0).String() != "a" || tags.Index(1).String() != "b" {
		t.Errorf("tags = [%q %q], want [a b]", tags.Index(0).String(), tags.Index(1).String())
	}
	// Mixed content: text under #text, child element alongside.
	note := field(t, row0, "note")
	if note.Kind() != record.KindMap {
		t.Fatalf("note kind = %v, want map", note.Kind())
	}
	if v := field(t, note, "#text"); v.String() != "helloworld" {
		t.Errorf("note/#text = %q, want helloworld", v.String())
	}
	if v := field(t, note, "b"); v.String() != "x" {
		t.Errorf("note/b = %q, want x", v.String())
	}

	row1 := b.Record(1)
	if v := field(t, row1, "@id"); v.String() != "2" {
		t.Errorf("row1 @id = %q", v.String())
	}
	if v := field(t, row1, "name"); v.String() != "bob" {
		t.Errorf("row1 name = %q", v.String())
	}

	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
}

func TestNamespacedPretty(t *testing.T) {
	// Pretty-printed + namespaced: prefixes stripped, indentation whitespace
	// ignored, matched by local name.
	const doc = `<?xml version="1.0" encoding="UTF-8"?>
<ns:Orders xmlns:ns="http://example.com/ns">
    <ns:Order ns:id="7">
        <ns:Customer>ada &amp; co</ns:Customer>
    </ns:Order>
    <ns:Order ns:id="8">
        <ns:Customer>bob</ns:Customer>
    </ns:Order>
</ns:Orders>`

	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "Order"})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 2 {
		t.Fatalf("len = %d, want 2", b.Len())
	}
	o0 := b.Record(0)
	if v := field(t, o0, "@id"); v.String() != "7" {
		t.Errorf("@id = %q, want 7 (prefix stripped)", v.String())
	}
	// Entity decoded; prefix stripped on child.
	if v := field(t, o0, "Customer"); v.String() != "ada & co" {
		t.Errorf("Customer = %q, want 'ada & co'", v.String())
	}
	// Indentation whitespace must not become #text.
	if _, ok := o0.Field("#text"); ok {
		t.Error("Order gained a #text field from indentation whitespace")
	}
	// The xmlns declaration must not be mapped as an attribute.
	if _, ok := o0.Field("@xmlns"); ok {
		t.Error("xmlns declaration leaked in as an attribute")
	}
	if v := field(t, b.Record(1), "@id"); v.String() != "8" {
		t.Errorf("record 1 @id = %q", v.String())
	}
}

func TestDefaultRecordElement(t *testing.T) {
	// Empty RecordElement → each direct child of the root is one record.
	const doc = `<items><a>1</a><b>2</b><c>3</c></items>`
	r := NewReader(strings.NewReader(doc), ReaderOptions{})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 3 {
		t.Fatalf("len = %d, want 3", b.Len())
	}
	// Leaf children map to bare strings.
	if got := b.Record(0).String(); got != "1" {
		t.Errorf("record 0 = %q, want 1", got)
	}
	if got := b.Record(2).String(); got != "3" {
		t.Errorf("record 2 = %q, want 3", got)
	}
}

func TestBatchingAcrossBatchRecords(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("<data>")
	for i := 0; i < 5; i++ {
		sb.WriteString("<row><n>x</n></row>")
	}
	sb.WriteString("</data>")

	r := NewReader(strings.NewReader(sb.String()), ReaderOptions{RecordElement: "row", BatchRecords: 2})
	ctx := context.Background()
	var sizes []int
	total := 0
	for {
		b, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, b.Len())
		total += b.Len()
	}
	if total != 5 {
		t.Fatalf("total records = %d, want 5", total)
	}
	// BatchRecords=2 over 5 records → 2,2,1.
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Errorf("batch sizes = %v, want [2 2 1]", sizes)
	}
}

func TestSelfClosingAndEmpty(t *testing.T) {
	const doc = `<data><row id="9"/><row></row></data>`
	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "row"})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 2 {
		t.Fatalf("len = %d, want 2", b.Len())
	}
	// Self-closing with an attribute → map with just the attribute.
	if v := field(t, b.Record(0), "@id"); v.String() != "9" {
		t.Errorf("@id = %q, want 9", v.String())
	}
	// Truly empty element → bare empty string.
	if got := b.Record(1); got.Kind() != record.KindString || got.String() != "" {
		t.Errorf("empty row = %v/%q, want empty string", got.Kind(), got.String())
	}
}

func TestEmptyInput(t *testing.T) {
	r := NewReader(strings.NewReader(""), ReaderOptions{RecordElement: "row"})
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("empty input = %v, want EOF", err)
	}
}

func TestWhitespaceOnly(t *testing.T) {
	r := NewReader(strings.NewReader("   \n\t  "), ReaderOptions{})
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("whitespace input = %v, want EOF", err)
	}
}

func TestNoMatchingRecords(t *testing.T) {
	// Document has content but no element matches RecordElement.
	r := NewReader(strings.NewReader(`<data><other>x</other></data>`), ReaderOptions{RecordElement: "row"})
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("no matches = %v, want EOF", err)
	}
}

func TestMalformed(t *testing.T) {
	// Unquoted attribute value is a syntax error mid-record.
	r := NewReader(strings.NewReader(`<data><row id=nope></row></data>`), ReaderOptions{RecordElement: "row"})
	_, err := r.Next(context.Background())
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("malformed = %v, want a parse error", err)
	}
	if !strings.Contains(err.Error(), "xmlf:") {
		t.Errorf("error not wrapped by package: %v", err)
	}
}

func TestTruncated(t *testing.T) {
	// Record started but the document ends before its close.
	r := NewReader(strings.NewReader(`<data><row><name>ada`), ReaderOptions{RecordElement: "row"})
	_, err := r.Next(context.Background())
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("truncated = %v, want unexpected-EOF error", err)
	}
}

func TestMaxDepth(t *testing.T) {
	const doc = `<data><row><a><b><c>x</c></b></a></row></data>`
	r := NewReader(strings.NewReader(doc), ReaderOptions{RecordElement: "row", MaxDepth: 3})
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("deep nesting = %v, want a max-depth error", err)
	}
}

func TestContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewReader(strings.NewReader(`<data><row>x</row></data>`), ReaderOptions{RecordElement: "row"})
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Next = %v, want context.Canceled", err)
	}
}

func TestCloseThenNext(t *testing.T) {
	r := NewReader(strings.NewReader(`<data><row>x</row></data>`), ReaderOptions{RecordElement: "row"})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v, want EOF", err)
	}
}

func TestDefaultsApplied(t *testing.T) {
	r := NewReader(strings.NewReader(""), ReaderOptions{})
	if r.opts.BatchRecords != DefaultBatchRecords ||
		r.opts.BatchBytes != DefaultBatchBytes ||
		r.opts.MaxDepth != DefaultMaxDepth {
		t.Errorf("defaults not applied: %+v", r.opts)
	}
}
