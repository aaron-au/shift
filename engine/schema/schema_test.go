package schema_test

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/schema"
)

// THE property of ADR-0042 §4c, and the reason this package exists instead of
// a dependency: JSON Schema's own rule is that an unrecognised keyword is an
// annotation and passes silently. That makes a typo a schema which validates
// NOTHING, forever, with no error anywhere — and a 202 that asserts a check
// which never ran is worse than having no schema at all.
func TestATypoInAnAssertionIsRejectedRatherThanIgnored(t *testing.T) {
	_, err := schema.Compile([]byte(`{"type":"object","require":["order_id"]}`))
	if err == nil {
		t.Fatal("compiled a schema with a misspelt `required`; it would have validated nothing")
	}
	if !strings.Contains(err.Error(), "require") {
		t.Errorf("error %q does not name the offending keyword", err)
	}
	// The near-miss hint: the most common real mistake is a typo, and naming
	// the intended keyword turns a puzzle into a fix.
	if !strings.Contains(err.Error(), "required") {
		t.Errorf("error %q does not suggest the intended keyword", err)
	}
}

// Annotations are different in kind from assertions: ignoring `title` cannot
// weaken validation, so rejecting it would only make real schemas unusable.
func TestAnnotationsAreAccepted(t *testing.T) {
	_, err := schema.Compile([]byte(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://example.com/order.json",
		"title": "Order", "description": "an order", "examples": [{"id": 1}],
		"deprecated": false, "readOnly": false, "$comment": "hi",
		"type": "object"
	}`))
	if err != nil {
		t.Fatalf("rejected a schema carrying only annotations alongside type: %v", err)
	}
}

func TestValidateReportsPathAndReason(t *testing.T) {
	s := mustCompile(t, `{
		"type": "object",
		"required": ["order_id", "lines"],
		"properties": {
			"order_id": {"type": "string", "minLength": 3},
			"lines": {"type": "array", "minItems": 1, "items": {
				"type": "object",
				"required": ["sku", "qty"],
				"properties": {
					"sku": {"type": "string"},
					"qty": {"type": "integer", "minimum": 1}
				}
			}}
		}
	}`)

	for _, tc := range []struct {
		name     string
		doc      string
		wantPath string
		wantMsg  string
	}{
		{"a missing required property", `{"lines":[{"sku":"a","qty":1}]}`,
			"/order_id", "required property is missing"},
		{"a wrong type deep in an array", `{"order_id":"abc","lines":[{"sku":"a","qty":"three"}]}`,
			"/lines/0/qty", "expected an integer, got \"three\""},
		{"a violated numeric bound", `{"order_id":"abc","lines":[{"sku":"a","qty":0}]}`,
			"/lines/0/qty", "expected a value >= 1, got 0"},
		{"an empty array against minItems", `{"order_id":"abc","lines":[]}`,
			"/lines", "expected at least 1 items, got 0"},
		{"a short string", `{"order_id":"ab","lines":[{"sku":"a","qty":1}]}`,
			"/order_id", "expected at least 3 characters, got 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := s.Validate(parse(t, tc.doc), nil)
			if len(vs) != 1 {
				t.Fatalf("got %d violations, want 1: %v", len(vs), vs)
			}
			if vs[0].Path != tc.wantPath {
				t.Errorf("path = %q, want %q", vs[0].Path, tc.wantPath)
			}
			if vs[0].Message != tc.wantMsg {
				t.Errorf("message = %q, want %q", vs[0].Message, tc.wantMsg)
			}
		})
	}

	if vs := s.Validate(parse(t, `{"order_id":"abc","lines":[{"sku":"a","qty":2}]}`), nil); len(vs) != 0 {
		t.Errorf("valid document produced %v", vs)
	}
}

// A caller reading an error needs the position in THEIR document, so pointer
// escaping has to be right for the keys people actually use.
func TestJSONPointerEscaping(t *testing.T) {
	s := mustCompile(t, `{"properties": {"a/b": {"type":"integer"}, "c~d": {"type":"integer"}}}`)
	vs := s.Validate(parse(t, `{"a/b":"x","c~d":"y"}`), nil)
	if len(vs) != 2 {
		t.Fatalf("got %d violations, want 2", len(vs))
	}
	got := []string{vs[0].Path, vs[1].Path}
	want := map[string]bool{"/a~1b": true, "/c~0d": true}
	for _, p := range got {
		if !want[p] {
			t.Errorf("path %q is not an escaped form of the offending key", p)
		}
	}
}

// Local $ref is not a nicety: every schema an OpenAPI toolchain produces uses
// it, so a validator without it rejects the schemas customers already have.
func TestLocalRefIsResolvedAtCompileTime(t *testing.T) {
	s := mustCompile(t, `{
		"type": "object",
		"properties": {"ship_to": {"$ref": "#/$defs/address"}, "bill_to": {"$ref": "#/$defs/address"}},
		"$defs": {"address": {
			"type": "object", "required": ["postcode"],
			"properties": {"postcode": {"type": "string", "pattern": "^[0-9]{4}$"}}
		}}
	}`)

	if vs := s.Validate(parse(t, `{"ship_to":{"postcode":"3000"},"bill_to":{"postcode":"2000"}}`), nil); len(vs) != 0 {
		t.Errorf("valid document produced %v", vs)
	}
	vs := s.Validate(parse(t, `{"ship_to":{"postcode":"30"},"bill_to":{}}`), nil)
	if len(vs) != 2 {
		t.Fatalf("got %d violations, want 2: %v", len(vs), vs)
	}
}

// A schema lifted out of an OpenAPI document refers to #/components/schemas/…
// rather than #/$defs/…, and refusing that would refuse most real schemas.
func TestRefResolvesAnyLocalPointer(t *testing.T) {
	s := mustCompile(t, `{
		"$ref": "#/components/schemas/Order",
		"components": {"schemas": {"Order": {"type":"object","required":["id"]}}}
	}`)
	if vs := s.Validate(parse(t, `{}`), nil); len(vs) != 1 {
		t.Fatalf("got %v, want the missing-id violation", vs)
	}
}

func TestRefFailuresAreRejectedAtCompileTime(t *testing.T) {
	for _, tc := range []struct{ name, doc, want string }{
		{"a remote reference", `{"$ref": "https://example.com/a.json"}`, "SSRF"},
		{"a recursive reference",
			`{"$defs":{"n":{"type":"object","properties":{"next":{"$ref":"#/$defs/n"}}}},"$ref":"#/$defs/n"}`,
			"recursive"},
		{"a dangling reference", `{"$ref": "#/$defs/missing"}`, "no such member"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := schema.Compile([]byte(tc.doc))
			if err == nil {
				t.Fatalf("compiled %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// Composition keywords are refused BY NAME rather than ignored, so an author
// finds out in the studio instead of discovering later that half their schema
// was inert.
func TestUnsupportedKeywordsAreNamed(t *testing.T) {
	for _, kw := range []string{"oneOf", "anyOf", "allOf", "if", "unevaluatedProperties", "exclusiveMinimum"} {
		_, err := schema.Compile([]byte(`{"type":"object","` + kw + `":[]}`))
		if err == nil {
			t.Errorf("compiled a schema using %s", kw)
			continue
		}
		if !strings.Contains(err.Error(), kw) {
			t.Errorf("error for %s does not name it: %v", kw, err)
		}
	}
}

// format ASSERTS here rather than annotating (the spec's default), because
// rejecting bad input is the only reason an author writes it.
func TestFormatsAssert(t *testing.T) {
	for _, tc := range []struct {
		format string
		ok     []string
		bad    []string
	}{
		{"date", []string{"2026-08-05", "2024-02-29"}, []string{"2026-8-5", "2026-02-30", "2026-13-01", "not-a-date", ""}},
		{"date-time", []string{"2026-08-05T04:11:07Z", "2026-08-05t04:11:07.25+10:00"},
			[]string{"2026-08-05", "2026-08-05T04:11:07", "2026-08-05T25:00:00Z"}},
		{"uuid", []string{"01234567-89ab-cdef-0123-456789abcdef", "00000000-0000-0000-0000-000000000000"},
			[]string{"0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdeg", ""}},
		{"email", []string{"a@b.co", "aaron.lees+tag@example.com.au"},
			[]string{"a@b", "@b.co", "a@.co", "a b@c.co", "a@b..co", ""}},
	} {
		t.Run(tc.format, func(t *testing.T) {
			s := mustCompile(t, `{"type":"string","format":"`+tc.format+`"}`)
			for _, in := range tc.ok {
				if vs := s.Validate(parse(t, quote(in)), nil); len(vs) != 0 {
					t.Errorf("%q rejected: %v", in, vs)
				}
			}
			for _, in := range tc.bad {
				if vs := s.Validate(parse(t, quote(in)), nil); len(vs) == 0 {
					t.Errorf("%q accepted as a valid %s", in, tc.format)
				}
			}
		})
	}
}

// An unsupported format must not be accepted-and-ignored, for the same reason
// an unsupported keyword must not be: it reads as enforced and is not.
func TestUnsupportedFormatIsRejected(t *testing.T) {
	if _, err := schema.Compile([]byte(`{"type":"string","format":"iso-country-code"}`)); err == nil {
		t.Fatal("accepted a format it cannot check")
	}
}

// 1 and 1.0 are the same number to JSON Schema. A caller whose encoder writes
// trailing zeros must not fail an "integer" field.
func TestIntegerAcceptsAWholeFloat(t *testing.T) {
	s := mustCompile(t, `{"type":"integer"}`)
	if vs := s.Validate(record.Float(3), nil); len(vs) != 0 {
		t.Errorf("3.0 rejected as an integer: %v", vs)
	}
	if vs := s.Validate(record.Float(3.5), nil); len(vs) == 0 {
		t.Error("3.5 accepted as an integer")
	}
}

// minLength/maxLength count CHARACTERS, not bytes: counting bytes would reject
// a legitimate name written in a non-Latin script.
func TestLengthCountsCodePointsNotBytes(t *testing.T) {
	s := mustCompile(t, `{"type":"string","maxLength":3}`)
	if vs := s.Validate(parse(t, `"Ωμέ"`), nil); len(vs) != 0 {
		t.Errorf("a 3-character Greek string was rejected by a byte count: %v", vs)
	}
}

// A document wrong in thousands of places must not produce thousands of
// allocations on a path that is about to return 400.
func TestViolationsAreCapped(t *testing.T) {
	s := mustCompile(t, `{"type":"array","items":{"type":"integer"}}`)
	var b strings.Builder
	b.WriteString(`["a"`)
	for range 5000 {
		b.WriteString(`,"a"`)
	}
	b.WriteString(`]`)
	if vs := s.Validate(parse(t, b.String()), nil); len(vs) != schema.MaxViolations {
		t.Errorf("got %d violations, want the cap of %d", len(vs), schema.MaxViolations)
	}
}

// Valid() stops at the first failure — the accept path only needs to know
// whether to answer 202 or 400.
func TestValidStopsEarly(t *testing.T) {
	s := mustCompile(t, `{"type":"object","required":["a","b","c"]}`)
	if s.Valid(parse(t, `{}`)) {
		t.Error("Valid accepted a document missing every required property")
	}
	if !s.Valid(parse(t, `{"a":1,"b":2,"c":3}`)) {
		t.Error("Valid rejected a satisfying document")
	}
}

// The engine contract: validation is on the request path, so a valid document
// must not allocate. A regression here is invisible in correctness tests and
// shows up as GC pressure at 20k req/s.
func TestValidatingAValidDocumentDoesNotAllocate(t *testing.T) {
	s := mustCompile(t, `{
		"type":"object","required":["order_id","lines"],
		"properties":{
			"order_id":{"type":"string","minLength":3,"maxLength":40},
			"lines":{"type":"array","minItems":1,"items":{
				"type":"object","required":["sku","qty"],
				"properties":{"sku":{"type":"string","pattern":"^[A-Z0-9-]+$"},"qty":{"type":"integer","minimum":1}}}}}
	}`)
	doc := parse(t, `{"order_id":"ORD-1","lines":[{"sku":"AB-1","qty":2},{"sku":"AB-2","qty":9}]}`)
	buf := make([]schema.Violation, 0, 8)

	if n := testing.AllocsPerRun(200, func() {
		if vs := s.Validate(doc, buf[:0]); len(vs) != 0 {
			t.Fatalf("document is not valid: %v", vs)
		}
	}); n != 0 {
		t.Errorf("validating a valid document allocated %.1f times per run, want 0", n)
	}
}

func BenchmarkValidate(b *testing.B) {
	s, err := schema.Compile([]byte(`{
		"type":"object","required":["order_id","lines"],
		"properties":{
			"order_id":{"type":"string","minLength":3},
			"lines":{"type":"array","items":{"type":"object","required":["sku","qty"],
				"properties":{"sku":{"type":"string"},"qty":{"type":"integer","minimum":1}}}}}
	}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := parseB(b, `{"order_id":"ORD-1","lines":[{"sku":"AB-1","qty":2},{"sku":"AB-2","qty":9},{"sku":"AB-3","qty":1}]}`)
	buf := make([]schema.Violation, 0, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if vs := s.Validate(doc, buf[:0]); len(vs) != 0 {
			b.Fatal(vs)
		}
	}
}

// --- helpers ---------------------------------------------------------------

func mustCompile(t *testing.T, src string) *schema.Schema {
	t.Helper()
	s, err := schema.Compile([]byte(src))
	if err != nil {
		t.Fatalf("compiling %s: %v", src, err)
	}
	return s
}

func quote(s string) string {
	var b bytes.Buffer
	b.WriteByte('"')
	b.WriteString(s)
	b.WriteByte('"')
	return b.String()
}

// parse turns a JSON document into a record.Value through the REAL reader, so
// these tests exercise the same values a request would produce rather than a
// hand-built approximation.
func parse(t *testing.T, doc string) record.Value {
	t.Helper()
	v, err := parseJSON(doc)
	if err != nil {
		t.Fatalf("parsing %s: %v", doc, err)
	}
	return v
}

func parseB(b *testing.B, doc string) record.Value {
	b.Helper()
	v, err := parseJSON(doc)
	if err != nil {
		b.Fatal(err)
	}
	return v
}

// parseJSON reads ONE whole document into a record.Value.
//
// It uses the line reader rather than ndjson.JSONReader deliberately: the
// JSON reader STREAMS a top-level array as one record per element, which is
// the right behaviour for a payload stream and the wrong one here — a schema
// with {"type":"array"} must see the array, not its first element. The same
// distinction is what ADR-0042's scope: body vs scope: records selects
// between on the accept path.
func parseJSON(doc string) (record.Value, error) {
	r := ndjson.NewReader(bufio.NewReader(strings.NewReader(doc)), ndjson.ReaderOptions{})
	batch, err := r.Next(context.Background())
	if err != nil {
		return record.Value{}, err
	}
	if batch.Len() == 0 {
		return record.Value{}, errEmpty
	}
	return batch.Record(0), nil
}

var errEmpty = errStr("no record parsed")

type errStr string

func (e errStr) Error() string { return string(e) }
