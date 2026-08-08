package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

// typedSink records every output field as "kind:text", so a test can assert the
// kind AND the exact value in one comparison — which matters here, because the
// defects being guarded against (a decimal silently becoming a float, a scale
// being dropped) all still produce a numerically close answer.
type typedSink struct {
	rows []map[string]string
}

func (s *typedSink) Write(_ context.Context, b *record.Batch) error {
	for _, rec := range b.Records() {
		row := make(map[string]string, rec.Len())
		for i := range rec.Len() {
			row[string(rec.KeyAt(i))] = describe(rec.Index(i))
		}
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *typedSink) Close() error { return nil }

// describe renders a value as "kind:text". AppendText deliberately covers only
// the kinds with one format-independent rendering, so the rest are spelled out
// here rather than in the engine.
func describe(v record.Value) string {
	text := v.Text()
	switch v.Kind() {
	case record.KindFloat:
		text = strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case record.KindString, record.KindBytes:
		text = v.String()
	case record.KindBool:
		text = strconv.FormatBool(v.Bool())
	case record.KindNull:
		text = "null"
	}
	return v.Kind().String() + ":" + text
}

// runExactAgg aggregates input, optionally coercing the amount field first, and
// returns one row per group keyed by field name.
func runExactAgg(t *testing.T, input string, to record.Kind, budget int64, aggs []Agg) ([]map[string]string, int64, error) {
	t.Helper()
	gov := mem.New(budget)
	src := ndjson.NewReader(strings.NewReader(input), ndjson.ReaderOptions{})
	p := New(src, "read")
	if to != record.KindNull {
		p = p.Coerce(CoerceRule{Field: "amount", To: to})
	}
	p = p.Aggregate(AggregateSpec{
		Key:      record.MustParsePath("$.group"),
		SpillDir: t.TempDir(),
		Gov:      gov,
		Aggs:     aggs,
	})
	agg := p.src.(*aggSource)
	sink := &typedSink{}
	if _, err := p.Run(context.Background(), sink, "write"); err != nil {
		return nil, 0, err
	}
	if gov.Used() != 0 {
		t.Fatalf("governor leaked %d bytes", gov.Used())
	}
	return sink.rows, agg.SpillBytes(), nil
}

func sumAggs() []Agg {
	return []Agg{
		{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
		{Op: AggMin, From: record.MustParsePath("$.amount"), Out: "lo"},
		{Op: AggMax, From: record.MustParsePath("$.amount"), Out: "hi"},
	}
}

// TestSummingIntegersStaysExactAndStaysAnInt closes issue #4 at the operator
// level: the float64 accumulator could not represent 2^53+1, so a sum of large
// integers came back quietly wrong and typed as a float.
func TestSummingIntegersStaysExactAndStaysAnInt(t *testing.T) {
	const big = 1<<53 + 1
	input := fmt.Sprintf("{\"group\":\"a\",\"amount\":%d}\n{\"group\":\"a\",\"amount\":%d}\n", big, big)

	rows, _, err := runExactAgg(t, input, record.KindNull, 1<<30, sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d groups, want 1", len(rows))
	}
	want := fmt.Sprintf("int:%d", int64(2*big))
	if got := rows[0]["total"]; got != want {
		t.Errorf("total = %s, want %s", got, want)
	}
	if got := rows[0]["lo"]; got != fmt.Sprintf("int:%d", int64(big)) {
		t.Errorf("lo = %s, want the input int unchanged", got)
	}
}

func TestSummingDecimalsKeepsTheFinestScale(t *testing.T) {
	// Amounts arrive as text, the way a CSV or fixed-width column does, and are
	// declared decimal — the opt-in from ADR-0051 §5.
	input := `{"group":"a","amount":"10.10"}
{"group":"a","amount":"0.005"}
{"group":"a","amount":"-0.10"}
{"group":"b","amount":"1"}
`
	rows, _, err := runExactAgg(t, input, record.KindDecimal, 1<<30, sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	byGroup := map[string]map[string]string{}
	for _, r := range rows {
		byGroup[strings.TrimPrefix(r["group"], "string:")] = r
	}
	// 10.10 + 0.005 - 0.10 = 10.005, at the finest scale any input used.
	if got := byGroup["a"]["total"]; got != "decimal:10.005" {
		t.Errorf("total = %s, want decimal:10.005", got)
	}
	// Extremes keep the kind AND the scale of the input they came from.
	if got := byGroup["a"]["lo"]; got != "decimal:-0.10" {
		t.Errorf("lo = %s, want decimal:-0.10", got)
	}
	if got := byGroup["a"]["hi"]; got != "decimal:10.10" {
		t.Errorf("hi = %s, want decimal:10.10", got)
	}
	// A whole-number decimal column stays a decimal at scale 0, which renders
	// as an int would but is still exact.
	if got := byGroup["b"]["total"]; got != "int:1" {
		t.Errorf("total = %s, want int:1 (scale 0 reduces to an int)", got)
	}
}

// TestACurrencyColumnAddsUpToTheCent is the case the kind exists for.
func TestACurrencyColumnAddsUpToTheCent(t *testing.T) {
	var sb strings.Builder
	const rows = 1000
	for range rows {
		sb.WriteString(`{"group":"a","amount":"0.10"}` + "\n")
	}
	got, _, err := runExactAgg(t, sb.String(), record.KindDecimal, 1<<30, sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	if total := got[0]["total"]; total != "decimal:100.00" {
		t.Errorf("total = %s, want decimal:100.00", total)
	}
	// The same column through float64, for the record: 0.10 is not
	// representable, so the error accumulates with every addition.
	var f float64
	for range rows {
		f += 0.10
	}
	if f == 100.0 {
		t.Error("float64 unexpectedly summed 1000×0.10 to exactly 100; the premise of this test needs revisiting")
	}
}

func TestAFloatInTheColumnMakesTheSumInexact(t *testing.T) {
	// Mixing is legal and lossy (ADR-0051 §4): the float drags the column with
	// it rather than failing the record.
	input := `{"group":"a","amount":1}
{"group":"a","amount":2.5}
{"group":"a","amount":3}
`
	rows, _, err := runExactAgg(t, input, record.KindNull, 1<<30, sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	if got := rows[0]["total"]; got != "float:6.5" {
		t.Errorf("total = %s, want float:6.5", got)
	}
}

// TestExactnessSurvivesSpilling is the test that matters most: aggregate state
// that goes to scratch and comes back must be the same state, not state that
// compares equal. A dropped scale or a float64 round trip would pass a
// numeric-only assertion.
func TestExactnessSurvivesSpilling(t *testing.T) {
	// High cardinality with a tiny budget forces many spill rounds, and each
	// group is hit repeatedly so partial totals must merge.
	const groups, perGroup = 400, 5
	var sb strings.Builder
	for i := range groups * perGroup {
		fmt.Fprintf(&sb, "{\"group\":\"g%04d\",\"amount\":\"0.01\"}\n", i%groups)
	}
	input := sb.String()

	spilled, spillBytes, err := runExactAgg(t, input, record.KindDecimal, 20*groupCost(12, 3), sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	if spillBytes == 0 {
		t.Fatal("expected spilling with a tiny budget")
	}
	inMemory, noSpill, err := runExactAgg(t, input, record.KindDecimal, 1<<30, sumAggs())
	if err != nil {
		t.Fatal(err)
	}
	if noSpill != 0 {
		t.Fatalf("unexpected spill of %d bytes on the large budget", noSpill)
	}
	if len(spilled) != groups || len(inMemory) != groups {
		t.Fatalf("group counts: spilled %d, in-memory %d, want %d", len(spilled), len(inMemory), groups)
	}

	index := func(rows []map[string]string) map[string]map[string]string {
		out := map[string]map[string]string{}
		for _, r := range rows {
			out[r["group"]] = r
		}
		return out
	}
	a, b := index(spilled), index(inMemory)
	for g, want := range b {
		got, ok := a[g]
		if !ok {
			t.Errorf("group %s missing from the spilled run", g)
			continue
		}
		for field, wantVal := range want {
			if got[field] != wantVal {
				t.Errorf("group %s field %s: spilled %s, in-memory %s", g, field, got[field], wantVal)
			}
		}
		// And the exact value, not merely agreement between the two runs.
		if want["total"] != "decimal:0.05" {
			t.Errorf("group %s total = %s, want decimal:0.05", g, want["total"])
		}
	}
}

// TestASumThatCannotBeRepresentedIsAnError is ADR-0051 §3: not a wrap, not a
// saturated value, and not a float64 approximation either.
func TestASumThatCannotBeRepresentedIsAnError(t *testing.T) {
	input := fmt.Sprintf("{\"group\":\"a\",\"amount\":%d}\n{\"group\":\"a\",\"amount\":%d}\n",
		int64(9223372036854775807), int64(9223372036854775807))

	_, _, err := runExactAgg(t, input, record.KindNull, 1<<30, []Agg{
		{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
	})
	if err == nil {
		t.Fatal("summing two MaxInt64 values must fail, not wrap")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Errorf("error = %v, want it to name the overflow", err)
	}
}

// TestAnOverflowingColumnWeNeverSumDoesNotFailTheFlow: MIN over huge values is
// perfectly well defined, so the exact accumulator must not be consulted for an
// agg that does not use it.
func TestAnOverflowingColumnWeNeverSumDoesNotFailTheFlow(t *testing.T) {
	input := fmt.Sprintf("{\"group\":\"a\",\"amount\":%d}\n{\"group\":\"a\",\"amount\":%d}\n",
		int64(9223372036854775807), int64(9223372036854775806))

	rows, _, err := runExactAgg(t, input, record.KindNull, 1<<30, []Agg{
		{Op: AggMin, From: record.MustParsePath("$.amount"), Out: "lo"},
		{Op: AggMax, From: record.MustParsePath("$.amount"), Out: "hi"},
		{Op: AggCount, Out: "n"},
	})
	if err != nil {
		t.Fatalf("MIN/MAX over values too large to sum must still work: %v", err)
	}
	if got := rows[0]["lo"]; got != "int:9223372036854775806" {
		t.Errorf("lo = %s", got)
	}
	if got := rows[0]["hi"]; got != "int:9223372036854775807" {
		t.Errorf("hi = %s", got)
	}
	if got := rows[0]["n"]; got != "int:2" {
		t.Errorf("n = %s", got)
	}
}

func TestAnEmptyColumnAggregatesToNullNotZero(t *testing.T) {
	input := `{"group":"a","amount":null}
{"group":"a"}
`
	rows, _, err := runExactAgg(t, input, record.KindNull, 1<<30, append(sumAggs(),
		Agg{Op: AggCount, Out: "n"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"total", "lo", "hi"} {
		if got := rows[0][field]; got != "null:null" {
			t.Errorf("%s = %s, want null (no input is not a zero total)", field, got)
		}
	}
	if got := rows[0]["n"]; got != "int:2" {
		t.Errorf("n = %s, want int:2 (COUNT is null-agnostic)", got)
	}
}
