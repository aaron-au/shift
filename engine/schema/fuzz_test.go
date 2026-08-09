package schema

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// FuzzCompile feeds the compiler arbitrary schema text. A schema is authored
// input rather than payload, but it is still bytes from outside the process,
// and the failure that matters is asymmetric: a panic takes the runner down,
// while a schema that compiles but under-enforces makes every later 202 a lie
// (ADR-0042 §4c). So a compiled schema is also exercised against a spread of
// documents here, not merely compiled.
func FuzzCompile(f *testing.F) {
	f.Add([]byte(`{"type":"object","required":["id"],"properties":{"id":{"type":"string","minLength":1}}}`))
	f.Add([]byte(`{"type":"array","items":{"$ref":"#/$defs/x"},"maxItems":2,"$defs":{"x":{"type":"integer"}}}`))
	f.Add([]byte(`{"type":"string","format":"email","pattern":"^a+$"}`))
	f.Add([]byte(`true`))
	f.Add([]byte(`false`))

	f.Add([]byte(`{"require":["id"]}`))                                      // a misspelt assertion must be refused
	f.Add([]byte(`{"$ref":"#/$defs/a","$defs":{"a":{"$ref":"#/$defs/a"}}}`)) // recursive $ref
	f.Add([]byte(`{"$ref":"https://example.com/s.json"}`))                   // remote $ref: an SSRF primitive
	f.Add([]byte(`{"$ref":"#/$defs/nope"}`))
	f.Add([]byte(`{"pattern":"("}`))                       // uncompilable regexp
	f.Add([]byte(`{"minLength":-1}`))                      //
	f.Add([]byte(`{"minLength":1e400}`))                   // a bound past float64
	f.Add([]byte(`{"type":["string",1]}`))                 //
	f.Add([]byte(`{"additionalProperties":{"type":"x"}}`)) // the schema form is refused, not ignored
	f.Add([]byte(strings.Repeat(`{"items":`, 200) + `true` + strings.Repeat(`}`, 200)))
	f.Add([]byte("{\"const\":\"\x00\xff\xfe\"}")) // NUL + invalid UTF-8 in a constant
	f.Add([]byte(`{`))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4<<10 {
			return // bounded work per input
		}
		s, err := Compile(raw)
		if err != nil {
			return // refusing is the expected outcome for almost everything here
		}
		for _, v := range sampleDocs() {
			assertValidAgreesWithValidate(t, s, v)
		}
	})
}

// FuzzValidate is the other half: a FIXED schema exercising every supported
// keyword, against arbitrary documents. This is the ADR-0042 verifier on its
// real input — bytes a caller posted — and the one thing a caller is told ran.
func FuzzValidate(f *testing.F) {
	f.Add([]byte(`{"id":"a3f","tags":["x"],"n":5,"when":"2026-08-01T00:00:00Z"}` + "\n"))
	f.Add([]byte(`{"id":"","tags":[],"n":-1,"when":"nope","extra":true}` + "\n"))
	f.Add([]byte("{}\n[]\nnull\n7\n\"s\"\ntrue\n")) // every JSON type at the root
	f.Add([]byte(`{"id":` + strings.Repeat(`[`, 200) + "\n"))
	f.Add([]byte(`{"n":1e400}` + "\n"))
	f.Add([]byte(`{"n":10.00,"id":"` + strings.Repeat("é", 200) + `"}` + "\n")) // multi-byte: minLength counts runes
	f.Add([]byte("{\"id\":\"a\x00b\"}\n{\"id\":\"\xff\xfe\"}\n"))
	f.Add([]byte(`{"a/b":1,"c~d":2}` + "\n")) // keys needing JSON Pointer escaping
	f.Add(bytes.Repeat([]byte(`{"nope":1}`+"\n"), 200))
	f.Add([]byte("\n\n"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32<<10 {
			return // bounded work per input
		}
		r := ndjson.NewReader(bytes.NewReader(data), ndjson.ReaderOptions{BatchRecords: 4})
		defer func() { _ = r.Close() }()
		ctx := context.Background()
		for range 10000 {
			b, err := r.Next(ctx)
			if err != nil {
				break
			}
			for _, rec := range b.Records() {
				assertValidAgreesWithValidate(t, fuzzSchema, rec)
			}
		}
	})
}

// assertValidAgreesWithValidate holds the two entry points to one answer. They
// are separate walks — Valid stops at the first failure, Validate collects —
// and callers use them interchangeably, so a document either satisfies the
// schema or it does not, whichever one asked.
func assertValidAgreesWithValidate(t *testing.T, s *Schema, v record.Value) {
	t.Helper()
	got := s.Validate(v, nil)
	if len(got) > MaxViolations {
		t.Fatalf("Validate returned %d violations, MaxViolations is %d", len(got), MaxViolations)
	}
	if s.Valid(v) != (len(got) == 0) {
		t.Fatalf("Valid=%v but Validate returned %d violations: %v", s.Valid(v), len(got), got)
	}
}

// fuzzSchema exercises every keyword in the closed set at once, so an arbitrary
// document meets all of them on one walk.
var fuzzSchema = func() *Schema {
	s, err := Compile([]byte(`{
		"type": "object",
		"required": ["id"],
		"properties": {
			"id":    {"type": "string", "minLength": 1, "maxLength": 8, "pattern": "^[a-f0-9]*$"},
			"tags":  {"type": "array", "minItems": 1, "maxItems": 3, "items": {"$ref": "#/$defs/tag"}},
			"n":     {"type": "number", "minimum": 0, "maximum": 100},
			"i":     {"type": "integer"},
			"when":  {"type": "string", "format": "date-time"},
			"kind":  {"enum": ["a", "b", 1, null]},
			"fixed": {"const": "x"},
			"who":   {"type": "object", "properties": {"mail": {"type": "string", "format": "email"}},
			          "additionalProperties": false}
		},
		"additionalProperties": true,
		"$defs": {"tag": {"type": "string", "maxLength": 4}}
	}`))
	if err != nil {
		panic(err) // a compile-time constant: broken here means broken everywhere
	}
	return s
}()

// sampleDocs is one value of every kind, so a fuzzed schema is applied to
// something of each type rather than only to the one shape it describes.
func sampleDocs() []record.Value {
	bld := record.NewBatch().Builder()
	out := []record.Value{}
	for _, build := range []func(){
		func() { bld.Null() },
		func() { bld.Bool(true) },
		func() { bld.Int(0) },
		func() { bld.Float(1.5) },
		func() { bld.Decimal(1010, 2) },
		func() { bld.StringLiteral("s") },
		func() { bld.BeginList(); bld.Int(1); bld.EndList() },
		func() { bld.BeginMap(); bld.KeyLiteral("id"); bld.StringLiteral("a"); bld.EndMap() },
	} {
		build()
		out = append(out, bld.Finish())
	}
	return out
}
