package stream

import (
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

// coerceOne coerces the "v" field of a single-record input and returns the
// result as "kind:text".
func coerceOne(t *testing.T, jsonValue string, to record.Kind) (string, error) {
	t.Helper()
	src := ndjson.NewReader(strings.NewReader(`{"v":`+jsonValue+"}\n"), ndjson.ReaderOptions{})
	sink := &typedSink{}
	_, err := New(src, "read").
		Coerce(CoerceRule{Field: "v", To: to}).
		Run(context.Background(), sink, "write")
	if err != nil {
		return "", err
	}
	if len(sink.rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(sink.rows))
	}
	return sink.rows[0]["v"], nil
}

func TestCoercingToADecimalIsExactFromText(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`"10.10"`, "decimal:10.10"}, // the written scale survives
		{`"-0.005"`, "decimal:-0.005"},
		{`"1e3"`, "decimal:1000"},
		{`7`, "decimal:7"}, // an int is exact already: scale 0
		{`null`, "null:null"},
	}
	for _, c := range cases {
		got, err := coerceOne(t, c.in, record.KindDecimal)
		if err != nil {
			t.Errorf("coerce %s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("coerce %s to decimal = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestCoercingAFloatToDecimalRecoversTheShortestForm documents the honest limit
// of ADR-0051 §5: coercion cannot restore precision the float never had, it can
// only stop losing more.
func TestCoercingAFloatToDecimalRecoversTheShortestForm(t *testing.T) {
	got, err := coerceOne(t, `10.1`, record.KindDecimal)
	if err != nil {
		t.Fatal(err)
	}
	if got != "decimal:10.1" {
		t.Errorf("coerce 10.1 to decimal = %s, want decimal:10.1", got)
	}
}

func TestCoercingToTemporalKindsParsesText(t *testing.T) {
	cases := []struct {
		in   string
		to   record.Kind
		want string
	}{
		{`"2026-08-08T09:30:00+10:00"`, record.KindTimestamp, "timestamp:2026-08-08T09:30:00+10:00"},
		{`"2026-08-08T09:30:00Z"`, record.KindTimestamp, "timestamp:2026-08-08T09:30:00Z"},
		{`"2026-08-08"`, record.KindDate, "date:2026-08-08"},
		{`"14:30:05"`, record.KindTime, "time:14:30:05"},
		{`null`, record.KindTimestamp, "null:null"},
	}
	for _, c := range cases {
		got, err := coerceOne(t, c.in, c.to)
		if err != nil {
			t.Errorf("coerce %s to %v: %v", c.in, c.to, err)
			continue
		}
		if got != c.want {
			t.Errorf("coerce %s to %v = %s, want %s", c.in, c.to, got, c.want)
		}
	}
}

// TestCoercingAnIntToATimestampIsRefused: an epoch number's unit is not written
// down anywhere, and reading milliseconds as seconds yields a plausible date in
// 1970 rather than an error.
func TestCoercingAnIntToATimestampIsRefused(t *testing.T) {
	_, err := coerceOne(t, `1754607000`, record.KindTimestamp)
	if err == nil {
		t.Fatal("an int should not coerce to a timestamp")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("error = %v, want it to explain the ambiguity", err)
	}
}

func TestCoercingExactKindsToTextUsesOneCanonicalForm(t *testing.T) {
	// Round trip: text in, exact kind, text out. What comes back must be what
	// went in, or the writers and the coercion disagree about rendering.
	for _, in := range []string{"10.10", "-0.005"} {
		src := ndjson.NewReader(strings.NewReader(`{"v":"`+in+"\"}\n"), ndjson.ReaderOptions{})
		sink := &typedSink{}
		_, err := New(src, "read").
			Coerce(CoerceRule{Field: "v", To: record.KindDecimal}).
			Coerce(CoerceRule{Field: "v", To: record.KindString}).
			Run(context.Background(), sink, "write")
		if err != nil {
			t.Fatal(err)
		}
		if got := sink.rows[0]["v"]; got != "string:"+in {
			t.Errorf("round trip of %q = %s", in, got)
		}
	}
}

func TestCoercingADecimalToAnIntTruncates(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(`{"v":"10.99"}`+"\n"), ndjson.ReaderOptions{})
	sink := &typedSink{}
	_, err := New(src, "read").
		Coerce(CoerceRule{Field: "v", To: record.KindDecimal}).
		Coerce(CoerceRule{Field: "v", To: record.KindInt}).
		Run(context.Background(), sink, "write")
	if err != nil {
		t.Fatal(err)
	}
	if got := sink.rows[0]["v"]; got != "int:10" {
		t.Errorf("10.99 to int = %s, want int:10 (truncated toward zero)", got)
	}
}

func TestCoercingRejectsTextThatIsNotADecimal(t *testing.T) {
	for _, in := range []string{`"abc"`, `"1.2.3"`, `""`} {
		if _, err := coerceOne(t, in, record.KindDecimal); err == nil {
			t.Errorf("coerce %s to decimal was accepted", in)
		}
	}
}

// TestADecimalGroupKeyRoundTripsThroughTheKeyCodec: group keys are encoded with
// the spill codec even when nothing spills, so a decimal key must survive it.
func TestADecimalGroupKeyRoundTripsThroughTheKeyCodec(t *testing.T) {
	input := `{"k":"10.10","n":1}
{"k":"10.10","n":2}
{"k":"10.1","n":3}
`
	src := ndjson.NewReader(strings.NewReader(input), ndjson.ReaderOptions{})
	sink := &typedSink{}
	_, err := New(src, "read").
		Coerce(CoerceRule{Field: "k", To: record.KindDecimal}).
		Aggregate(AggregateSpec{
			Key:      record.MustParsePath("$.k"),
			KeyName:  "k",
			SpillDir: t.TempDir(),
			Gov:      mem.New(1 << 20),
			Aggs:     []Agg{{Op: AggCount, Out: "n"}},
		}).
		Run(context.Background(), sink, "write")
	if err != nil {
		t.Fatal(err)
	}
	// 10.10 and 10.1 are numerically equal but written differently, and the key
	// codec preserves the scale — so they group separately. That follows from
	// grouping on the encoded bytes, and is worth pinning: the alternative
	// (numeric key equality) would merge them and change the row count.
	if len(sink.rows) != 2 {
		t.Fatalf("got %d groups, want 2: %v", len(sink.rows), sink.rows)
	}
	seen := map[string]string{}
	for _, r := range sink.rows {
		seen[r["k"]] = r["n"]
	}
	if seen["decimal:10.10"] != "int:2" {
		t.Errorf("group 10.10 count = %s, want int:2", seen["decimal:10.10"])
	}
	if seen["decimal:10.1"] != "int:1" {
		t.Errorf("group 10.1 count = %s, want int:1", seen["decimal:10.1"])
	}
}
