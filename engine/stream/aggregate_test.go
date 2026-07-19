package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

// aggInput builds n records over k distinct groups with deterministic
// amounts: group g gets records with amount = g*1000 + seq.
func aggInput(n, k int) string {
	var sb strings.Builder
	for i := range n {
		g := i % k
		fmt.Fprintf(&sb, `{"group":"g%04d","amount":%d,"note":"x"}`+"\n", g, g*1000+i/k)
	}
	return sb.String()
}

// runAgg aggregates the input and returns group -> (count, sum, min, max).
func runAgg(t *testing.T, input string, budget int64, spillDir string) (map[string][4]float64, int64) {
	t.Helper()
	gov := mem.New(budget)
	src := ndjson.NewReader(strings.NewReader(input), ndjson.ReaderOptions{})
	p := New(src, "read").Aggregate(AggregateSpec{
		Key:      record.MustParsePath("$.group"),
		SpillDir: spillDir,
		Gov:      gov,
		Aggs: []Agg{
			{Op: AggCount, Out: "n"},
			{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
			{Op: AggMin, From: record.MustParsePath("$.amount"), Out: "lo"},
			{Op: AggMax, From: record.MustParsePath("$.amount"), Out: "hi"},
		},
	})
	agg := p.src.(*aggSource)
	var out bytes.Buffer
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write"); err != nil {
		t.Fatal(err)
	}
	res := map[string][4]float64{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var row struct {
			Group string  `json:"group"`
			N     float64 `json:"n"`
			Total float64 `json:"total"`
			Lo    float64 `json:"lo"`
			Hi    float64 `json:"hi"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("bad output line %q: %v", line, err)
		}
		if _, dup := res[row.Group]; dup {
			t.Fatalf("group %s emitted twice", row.Group)
		}
		res[row.Group] = [4]float64{row.N, row.Total, row.Lo, row.Hi}
	}
	if gov.Used() != 0 {
		t.Fatalf("governor leaked %d bytes", gov.Used())
	}
	return res, agg.SpillBytes()
}

func TestAggregateInMemory(t *testing.T) {
	const n, k = 10000, 40
	res, spilled := runAgg(t, aggInput(n, k), 1<<30, t.TempDir())
	if spilled != 0 {
		t.Fatalf("unexpected spill of %d bytes", spilled)
	}
	verifyAgg(t, res, n, k)
}

func TestAggregateWithForcedSpill(t *testing.T) {
	const n, k = 10000, 2000 // high cardinality
	// Budget fits ~50 groups: forces many spill rounds.
	res, spilled := runAgg(t, aggInput(n, k), 50*groupCost(10, 4), t.TempDir())
	if spilled == 0 {
		t.Fatal("expected spilling with tiny budget")
	}
	verifyAgg(t, res, n, k)

	// Spilled and unspilled must agree exactly.
	res2, _ := runAgg(t, aggInput(n, k), 1<<30, t.TempDir())
	if len(res) != len(res2) {
		t.Fatalf("group counts differ: %d vs %d", len(res), len(res2))
	}
	for g, v := range res {
		if res2[g] != v {
			t.Fatalf("group %s: spilled %v != in-memory %v", g, v, res2[g])
		}
	}
}

func verifyAgg(t *testing.T, res map[string][4]float64, n, k int) {
	t.Helper()
	if len(res) != k {
		t.Fatalf("got %d groups, want %d", len(res), k)
	}
	per := n / k
	var groups []string
	for g := range res {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	for gi, g := range groups {
		v := res[g]
		wantCount := float64(per)
		lo := float64(gi * 1000)
		hi := float64(gi*1000 + per - 1)
		wantSum := (lo + hi) * wantCount / 2
		if v[0] != wantCount || v[1] != wantSum || v[2] != lo || v[3] != hi {
			t.Fatalf("group %s: got n=%v sum=%v lo=%v hi=%v, want n=%v sum=%v lo=%v hi=%v",
				g, v[0], v[1], v[2], v[3], wantCount, wantSum, lo, hi)
		}
	}
}

func TestAggregateNullsAndMissing(t *testing.T) {
	in := `{"group":"a","amount":1}` + "\n" +
		`{"group":"a","amount":null}` + "\n" +
		`{"group":"a"}` + "\n" +
		`{"amount":5}` + "\n" // missing key -> null group
	gov := mem.New(1 << 20)
	src := ndjson.NewReader(strings.NewReader(in), ndjson.ReaderOptions{})
	p := New(src, "read").Aggregate(AggregateSpec{
		Key: record.MustParsePath("$.group"),
		Gov: gov,
		Aggs: []Agg{
			{Op: AggCount, Out: "n"},
			{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
		},
	})
	var out bytes.Buffer
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write"); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 groups, got %v", lines)
	}
	joined := out.String()
	if !strings.Contains(joined, `"group":"a","n":3,"total":1`) {
		t.Errorf("group a wrong: %s", joined)
	}
	if !strings.Contains(joined, `"group":null,"n":1,"total":5`) {
		t.Errorf("null group wrong: %s", joined)
	}
}

func TestAggregateContainerKeyFails(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(`{"k":{"nested":1}}`+"\n"), ndjson.ReaderOptions{})
	p := New(src, "read").Aggregate(AggregateSpec{
		Key:  record.MustParsePath("$.k"),
		Gov:  mem.New(1 << 20),
		Aggs: []Agg{{Op: AggCount, Out: "n"}},
	})
	if _, err := p.Run(context.Background(), ndjson.NewWriter(io.Discard), "write"); err == nil {
		t.Fatal("expected container key error")
	}
}

func TestAggregateNonNumericFails(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(`{"g":"a","v":"str"}`+"\n"), ndjson.ReaderOptions{})
	p := New(src, "read").Aggregate(AggregateSpec{
		Key:  record.MustParsePath("$.g"),
		Gov:  mem.New(1 << 20),
		Aggs: []Agg{{Op: AggSum, From: record.MustParsePath("$.v"), Out: "s"}},
	})
	if _, err := p.Run(context.Background(), ndjson.NewWriter(io.Discard), "write"); err == nil {
		t.Fatal("expected non-numeric error")
	}
}

func TestAggregateSpecValidation(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(""), ndjson.ReaderOptions{})
	p := New(src, "read").Aggregate(AggregateSpec{Key: record.MustParsePath("$.g")}) // no governor
	if _, err := p.Run(context.Background(), ndjson.NewWriter(io.Discard), "write"); err == nil {
		t.Fatal("expected governor-required error")
	}
}
