package starlarkop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
)

// TestTheScriptVisibleSurface walks what a script can actually see and do with
// each kind, through Starlark rather than through Go — which is the only way to
// exercise the value adapters as an author meets them.
func TestTheScriptVisibleSurface(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    out = {}
    # records: attribute access, subscript, get with a default, keys, len
    out["by_attr"]   = rec.nested.inner
    out["by_index"]  = rec["nested"]["inner"]
    out["missing"]   = rec.get("nope", "fallback")
    out["nkeys"]     = len(rec.keys())
    out["truthy"]    = bool(rec.nested)
    out["repr"]      = str(rec.nested)

    # lists: index, len, iteration, truthiness
    out["first"]     = rec.items[0]
    out["nitems"]    = len(rec.items)
    tot = 0
    for x in rec.items:
        tot = tot + x
    out["sum"]       = tot
    out["empty"]     = bool(rec.empty_list)

    # decimals: parts, text, comparison, deliberate float conversion
    d = rec.amount
    out["coef"]      = d.coefficient
    out["scale"]     = d.scale
    out["dtext"]     = d.text
    out["as_float"]  = d.float
    out["dtruthy"]   = bool(d)
    out["drepr"]     = str(d)
    out["eq"]        = d == decimal("10.10")
    out["lt"]        = d < decimal("99.99")
    out["ge"]        = d >= decimal("10.10")
    out["ne"]        = d != decimal("1.00")
    out["zero"]      = bool(decimal("0.00"))

    # temporal: text, comparison, the numeric accessors
    out["ttext"]     = rec.at.text
    out["tnanos"]    = rec.at.unix_nanos
    out["ddays"]     = rec.on.days
    out["tod"]       = rec.tod.nanos_of_day
    out["tcmp"]      = rec.on == rec.on
    out["trepr"]     = str(rec.at)
    out["ttype"]     = type(rec.on)
    return out
`)
	src := record.NewBatch()
	bld := src.Builder()
	bld.BeginMap()
	bld.KeyLiteral("nested")
	bld.BeginMap()
	bld.KeyLiteral("inner")
	bld.StringLiteral("deep")
	bld.EndMap()
	bld.KeyLiteral("items")
	bld.BeginList()
	bld.Int(1)
	bld.Int(2)
	bld.Int(3)
	bld.EndList()
	bld.KeyLiteral("empty_list")
	bld.BeginList()
	bld.EndList()
	bld.KeyLiteral("amount")
	bld.Decimal(1010, 2)
	bld.KeyLiteral("at")
	bld.TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 0, time.UTC))
	bld.KeyLiteral("on")
	bld.Date(20673)
	bld.KeyLiteral("tod")
	bld.TimeOfDay(int64(90 * time.Second))
	bld.EndMap()
	rec := bld.Finish()

	out, keep, err := prog.Run(context.Background(), record.NewBatch(), rec)
	if err != nil || !keep {
		t.Fatalf("run: err=%v keep=%v", err, keep)
	}
	get := func(name string) record.Value {
		v, ok := out.Field(name)
		if !ok {
			t.Fatalf("field %q missing", name)
		}
		return v
	}
	checks := []struct {
		field string
		want  string
	}{
		{"by_attr", "deep"}, {"by_index", "deep"}, {"missing", "fallback"},
		{"dtext", "10.10"}, {"drepr", "10.10"},
		{"ttext", "2026-08-08T09:30:00Z"}, {"trepr", "2026-08-08T09:30:00Z"},
		{"ttype", "date"},
	}
	for _, c := range checks {
		if got := get(c.field).String(); got != c.want {
			t.Errorf("%s = %q, want %q", c.field, got, c.want)
		}
	}
	ints := map[string]int64{
		"nkeys": 7, "first": 1, "nitems": 3, "sum": 6,
		"coef": 1010, "scale": 2, "ddays": 20673, "tod": int64(90 * time.Second),
	}
	for field, want := range ints {
		if got := get(field).Int(); got != want {
			t.Errorf("%s = %d, want %d", field, got, want)
		}
	}
	bools := map[string]bool{
		"truthy": true, "empty": false, "dtruthy": true, "zero": false,
		"eq": true, "lt": true, "ge": true, "ne": true, "tcmp": true,
	}
	for field, want := range bools {
		if got := get(field).Bool(); got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	if got := get("as_float").Float(); got < 10.09 || got > 10.11 {
		t.Errorf("as_float = %v", got)
	}
}

func TestEveryReturnableTypeConverts(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {
        "none": None, "yes": True, "n": 7, "f": 1.5, "s": "text",
        "b": b"bytes", "list": [1, "two"], "tuple": (1, 2),
        "nested": {"a": {"b": 1}},
    }
`)
	out, keep, err := prog.Run(context.Background(), record.NewBatch(),
		record.Null())
	if err != nil || !keep {
		t.Fatalf("run: err=%v keep=%v", err, keep)
	}
	kinds := map[string]record.Kind{
		"none": record.KindNull, "yes": record.KindBool, "n": record.KindInt,
		"f": record.KindFloat, "s": record.KindString, "b": record.KindBytes,
		"list": record.KindList, "tuple": record.KindList, "nested": record.KindMap,
	}
	for field, want := range kinds {
		v, ok := out.Field(field)
		if !ok {
			t.Errorf("%s missing", field)
			continue
		}
		if v.Kind() != want {
			t.Errorf("%s = %v, want %v", field, v.Kind(), want)
		}
	}
	nested, _ := out.Field("nested")
	inner, _ := nested.Field("a")
	if b, ok := inner.Field("b"); !ok || b.Int() != 1 {
		t.Errorf("nested value lost: %v", inner.Kind())
	}
}

func TestUnrepresentableReturnsAreRefused(t *testing.T) {
	cases := map[string]string{
		"nan":      `def transform(rec): return {"x": float("nan")}`,
		"inf":      `def transform(rec): return {"x": float("inf")}`,
		"function": `def transform(rec): return {"x": transform}`,
		"non-str key": `def transform(rec):
    d = {}
    d[1] = "x"
    return d`,
	}
	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			prog, err := Compile(Options{Script: script, StepID: "s1", Allowed: yes()})
			if err != nil {
				return
			}
			if _, _, err := prog.Run(context.Background(), record.NewBatch(), record.Null()); err == nil {
				t.Errorf("%s was accepted in a returned record", name)
			}
		})
	}
}

func TestDecimalArithmeticEdges(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    d = rec.amount
    return {
        "plus_int":  d + 5,
        "int_plus":  5 + d,
        "minus":     d - decimal("0.10"),
        "rminus":    decimal("20.00") - d,
        "times_int": d * 2,
        "int_times": 2 * d,
        "neg":       decimal("0.00") - d,
    }
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("amount")
		b.Decimal(1010, 2)
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"plus_int": "decimal:15.10", "int_plus": "decimal:15.10",
		"minus": "decimal:10.00", "rminus": "decimal:9.90",
		"times_int": "decimal:20.20", "int_times": "decimal:20.20",
		"neg": "decimal:-10.10",
	}
	for field, w := range want {
		if got[field] != w {
			t.Errorf("%s = %s, want %s", field, got[field], w)
		}
	}
}

func TestDecimalOverflowIsReported(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    big = decimal("9223372036854775807")
    return {"x": big * big}
`)
	if _, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	}); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("error = %v, want an overflow", err)
	}
}

func TestComparingIncompatibleTypesIsAnError(t *testing.T) {
	for _, script := range []string{
		`def transform(rec): return {"x": rec.amount < rec.at}`,
		`def transform(rec): return {"x": rec.at < rec.on}`,
	} {
		prog := compile(t, script)
		_, _, err := runOne(t, prog, func(b *record.Builder) {
			b.KeyLiteral("amount")
			b.Decimal(1, 0)
			b.KeyLiteral("at")
			b.TimestampAt(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
			b.KeyLiteral("on")
			b.Date(1)
		})
		if err == nil {
			t.Errorf("%q compared incomparable types", script)
		}
	}
}

// TestTheOperatorDropsAndRewritesInAPipeline covers Apply itself: filtering,
// rewriting, and the batch handed downstream.
func TestTheOperatorDropsAndRewritesInAPipeline(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    if rec.n % 2 == 0:
        return None
    return {"n": rec.n, "double": rec.n * 2}
`)
	src := ndjson.NewReader(strings.NewReader(
		"{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n"), ndjson.ReaderOptions{})
	p := Apply(stream.New(src, "read"), prog, "code")

	var sink collectSink
	if _, err := p.Run(context.Background(), &sink, "write"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(sink.got) != 2 {
		t.Fatalf("kept %d records, want 2 (evens dropped)", len(sink.got))
	}
	if sink.got[0] != "1/2" || sink.got[1] != "3/6" {
		t.Errorf("records = %v, want [1/2 3/6]", sink.got)
	}
}

type collectSink struct{ got []string }

func (c *collectSink) Write(_ context.Context, b *record.Batch) error {
	for _, rec := range b.Records() {
		n, _ := rec.Field("n")
		d, _ := rec.Field("double")
		c.got = append(c.got, n.Text()+"/"+d.Text())
	}
	return nil
}

func (c *collectSink) Close() error { return nil }

// A script error must surface as a pipeline error, not a dropped record.
func TestAScriptErrorFailsThePipeline(t *testing.T) {
	prog := compile(t, `def transform(rec): fail("nope")`)
	src := ndjson.NewReader(strings.NewReader("{\"n\":1}\n"), ndjson.ReaderOptions{})
	p := Apply(stream.New(src, "read"), prog, "code")
	var sink collectSink
	if _, err := p.Run(context.Background(), &sink, "write"); err == nil {
		t.Fatal("a failing script did not fail the pipeline")
	}
}

func TestDecimalBuiltinRejectsNonDecimalText(t *testing.T) {
	prog := compile(t, `def transform(rec): return {"x": decimal("abc")}`)
	if _, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	}); err == nil {
		t.Fatal("decimal(\"abc\") was accepted")
	}
}

func TestUnknownAttributesAreAbsentNotFatal(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {"has": hasattr(rec.amount, "text"), "hasnt": hasattr(rec.amount, "nope")}
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("amount")
		b.Decimal(1, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["has"] != "bool:true" || got["hasnt"] != "bool:false" {
		t.Errorf("hasattr = %s / %s", got["has"], got["hasnt"])
	}
}
