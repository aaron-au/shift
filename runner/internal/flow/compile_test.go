package flow

import (
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
)

// These tests exercise the compile-only logic of the flow package directly:
// Parse, the per-op lowering in applyOp, filter predicate compilation, and
// the coerce-kind lookup. They spawn no connector subprocess and use no
// clock/randomness, so they are deterministic under SHIFT_COVERAGE.

// dummyPipe returns a pipeline over an empty NDJSON source, enough to hand to
// applyOp when we only care about compilation (not execution).
func dummyPipe(t *testing.T) *stream.Pipeline {
	t.Helper()
	return stream.New(ndjson.NewReader(strings.NewReader(""), ndjson.ReaderOptions{}), "src")
}

// mapRec builds a single-level map record {field: value} in its own batch and
// returns the root Value. The batch is kept alive by the returned closure's
// caller for the lifetime of the assertion.
func mapRec(t *testing.T, field string, v record.Value) record.Value {
	t.Helper()
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral(field)
	bld.Value(v)
	bld.EndMap()
	return bld.Finish()
}

func TestParse(t *testing.T) {
	good := []byte(`{"name":"t","source":{"connector":"x","action":"y"},` +
		`"ops":[{"type":"flatten","sep":"."}],"sink":{"connector":"x","action":"y"}}`)
	d, err := Parse(good)
	if err != nil {
		t.Fatalf("Parse(good) = %v", err)
	}
	if d.Name != "t" {
		t.Fatalf("name = %q, want t", d.Name)
	}

	// Malformed JSON is rejected.
	if _, err := Parse([]byte(`{`)); err == nil {
		t.Fatal("Parse(malformed) = nil error, want failure")
	}
	// Structurally valid JSON that fails document validation is rejected.
	if _, err := Parse([]byte(`{"name":""}`)); err == nil {
		t.Fatal("Parse(invalid doc) = nil error, want failure")
	}
}

func TestKindOf(t *testing.T) {
	cases := []struct {
		name string
		want record.Kind
	}{
		{"int", record.KindInt},
		{"float", record.KindFloat},
		{"bool", record.KindBool},
		{"string", record.KindString},
	}
	for _, c := range cases {
		got, err := kindOf(c.name)
		if err != nil {
			t.Errorf("kindOf(%q) = %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("kindOf(%q) = %v, want %v", c.name, got, c.want)
		}
	}
	if _, err := kindOf("bogus"); err == nil {
		t.Error("kindOf(bogus) = nil error, want failure")
	}
}

func TestApplyOpUnknownType(t *testing.T) {
	if _, err := applyOp(&Op{Type: "nope"}, dummyPipe(t), CompileOptions{}); err == nil {
		t.Fatal("applyOp(unknown) = nil error, want failure")
	}
}

func TestApplyOpCoerceUnknownKind(t *testing.T) {
	o := &Op{Type: "coerce", Rules: []CoerceRule{{Field: "x", To: "bogus"}}}
	if _, err := applyOp(o, dummyPipe(t), CompileOptions{}); err == nil {
		t.Fatal("applyOp(coerce bad kind) = nil error, want failure")
	}
}

func TestApplyOpEachTypeCompiles(t *testing.T) {
	ops := []*Op{
		{Type: "project", Fields: []ProjectField{{Out: "a", Path: "$.a"}}},
		{Type: "coerce", Rules: []CoerceRule{{Field: "a", To: "int"}}},
		{Type: "flatten", Sep: "."},
		{Type: "aggregate", Key: "$.k", Aggs: []Agg{{Op: "count", Out: "n"}}},
		{Type: "aggregate", Key: "$.k", Aggs: []Agg{{Op: "min", Path: "$.a", Out: "lo"}}},
		{Type: "aggregate", Key: "$.k", Aggs: []Agg{{Op: "max", Path: "$.a", Out: "hi"}}},
	}
	for _, o := range ops {
		if _, err := applyOp(o, dummyPipe(t), CompileOptions{}); err != nil {
			t.Errorf("applyOp(%s) = %v", o.Type, err)
		}
	}
}

func TestCompileFilterError(t *testing.T) {
	// A non-scalar filter value cannot be compiled.
	o := &Op{Type: "filter", Path: "$.a", Cmp: "eq", Value: []byte(`{"nested":1}`)}
	if _, err := compileFilter(o); err == nil {
		t.Fatal("compileFilter(non-scalar value) = nil error, want failure")
	}
}

func TestCompileFilterExists(t *testing.T) {
	pred, err := compileFilter(&Op{Path: "$.a", Cmp: "exists"})
	if err != nil {
		t.Fatal(err)
	}
	if !pred(mapRec(t, "a", record.Int(1))) {
		t.Error("exists: present field should pass")
	}
	if pred(mapRec(t, "a", record.Null())) {
		t.Error("exists: null field should not pass")
	}
	if pred(mapRec(t, "b", record.Int(1))) {
		t.Error("exists: absent field should not pass")
	}
}

func TestCompileFilterComparisons(t *testing.T) {
	cases := []struct {
		cmp   string
		value string
		field record.Value
		want  bool
	}{
		{"eq", `5`, record.Int(5), true},
		{"eq", `5`, record.Int(6), false},
		{"ne", `5`, record.Int(6), true},
		{"ne", `5`, record.Int(5), false},
		{"gt", `5`, record.Int(6), true},
		{"gt", `5`, record.Int(5), false},
		{"gte", `5`, record.Int(5), true},
		{"gte", `5`, record.Int(4), false},
		{"lt", `5`, record.Int(4), true},
		{"lt", `5`, record.Int(5), false},
		{"lte", `5`, record.Int(5), true},
		{"lte", `5`, record.Int(6), false},
		{"eq", `"x"`, record.UnsafeString([]byte("x")), true},
		// Ordered comparison against a non-numeric field value is false.
		{"gt", `5`, record.UnsafeString([]byte("x")), false},
		// Ordered comparison against a non-numeric want is false.
		{"gt", `"x"`, record.Int(9), false},
	}
	for _, c := range cases {
		pred, err := compileFilter(&Op{Path: "$.a", Cmp: c.cmp, Value: []byte(c.value)})
		if err != nil {
			t.Fatalf("compileFilter(%s,%s) = %v", c.cmp, c.value, err)
		}
		if got := pred(mapRec(t, "a", c.field)); got != c.want {
			t.Errorf("%s %s vs field: got %v, want %v", c.cmp, c.value, got, c.want)
		}
	}
}

func TestCompileFilterMissingPath(t *testing.T) {
	// A comparison predicate on an absent field is always false.
	for _, cmp := range []string{"eq", "ne", "gt", "gte", "lt", "lte"} {
		pred, err := compileFilter(&Op{Path: "$.a", Cmp: cmp, Value: []byte(`1`)})
		if err != nil {
			t.Fatal(err)
		}
		if pred(mapRec(t, "b", record.Int(1))) {
			t.Errorf("%s: missing field should not pass", cmp)
		}
	}
}

func TestCompileFilterFloatValue(t *testing.T) {
	// A fractional JSON number stays a float scalar and compares numerically.
	pred, err := compileFilter(&Op{Path: "$.a", Cmp: "gt", Value: []byte(`1.5`)})
	if err != nil {
		t.Fatal(err)
	}
	if !pred(mapRec(t, "a", record.Float(2.0))) {
		t.Error("2.0 > 1.5 should pass")
	}
	if pred(mapRec(t, "a", record.Float(1.0))) {
		t.Error("1.0 > 1.5 should not pass")
	}
}

func TestIsNumeric(t *testing.T) {
	if !isNumeric(record.Int(1)) || !isNumeric(record.Float(1)) {
		t.Error("int/float should be numeric")
	}
	if isNumeric(record.UnsafeString([]byte("x"))) || isNumeric(record.Bool(true)) || isNumeric(record.Null()) {
		t.Error("string/bool/null should not be numeric")
	}
}

// TestProjectAndCoerceEndToEnd verifies project selects/renames fields and
// coerce converts each supported kind, through Apply's public path.
func TestProjectAndCoerceEndToEnd(t *testing.T) {
	input := `{"id":"1","n":"42","flag":"true","f":"3.5","drop":"x"}` + "\n"
	ops := `[
	  {"type":"coerce","rules":[
	    {"field":"n","to":"int"},
	    {"field":"flag","to":"bool"},
	    {"field":"f","to":"float"}
	  ]},
	  {"type":"project","fields":[
	    {"out":"id","path":"$.id"},
	    {"out":"n","path":"$.n"},
	    {"out":"flag","path":"$.flag"},
	    {"out":"f","path":"$.f"}
	  ]}
	]`
	got := runDoc(t, ops, input)
	// project dropped "drop"; coerce retyped the scalars.
	if strings.Contains(got, "drop") {
		t.Errorf("project should drop unlisted field: %s", got)
	}
	if !strings.Contains(got, `"n":42`) || !strings.Contains(got, `"flag":true`) || !strings.Contains(got, `"f":3.5`) {
		t.Errorf("coerce output wrong: %s", got)
	}
}

// TestAggregateMinMax exercises the min/max aggregate ops end-to-end.
func TestAggregateMinMax(t *testing.T) {
	input := `{"g":"a","v":3}` + "\n" + `{"g":"a","v":1}` + "\n" +
		`{"g":"a","v":9}` + "\n" + `{"g":"b","v":5}` + "\n"
	ops := `[{"type":"aggregate","key":"$.g","aggs":[
	  {"op":"min","path":"$.v","out":"lo"},
	  {"op":"max","path":"$.v","out":"hi"}
	]}]`
	got := runDoc(t, ops, input)
	if !strings.Contains(got, `"g":"a","lo":1,"hi":9`) {
		t.Errorf("group a min/max wrong: %s", got)
	}
	if !strings.Contains(got, `"g":"b","lo":5,"hi":5`) {
		t.Errorf("group b min/max wrong: %s", got)
	}
}
