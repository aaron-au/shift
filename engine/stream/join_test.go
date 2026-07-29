package stream

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

// runJoinSorted runs a join with an explicit watermark + partition count and
// returns its output records sorted (a multiset, so grace-mode reordering does
// not matter).
func runJoinSorted(t *testing.T, probe, build string, spec JoinSpec, wm int64, parts int) []string {
	t.Helper()
	spec.Gov = mem.New(wm)
	spec.Partitions = parts
	spec.SpillDir = t.TempDir()
	ps := ndjson.NewReader(strings.NewReader(probe), ndjson.ReaderOptions{BatchRecords: 3})
	bs := ndjson.NewReader(strings.NewReader(build), ndjson.ReaderOptions{BatchRecords: 3})
	p := New(Join(ps, bs, spec), "join")
	var out bytes.Buffer
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write"); err != nil {
		t.Fatalf("run: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	sort.Strings(lines)
	return lines
}

// TestJoinGraceMatchesInMemory is the correctness net for the spilling join: a
// forced-spill (tiny watermark) grace join must produce exactly the same output
// as the in-memory join, across randomized datasets, for inner and left.
func TestJoinGraceMatchesInMemory(t *testing.T) {
	r := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test data, not security
	for iter := range 50 {
		nb, np := r.Intn(60), r.Intn(60)
		var build, probe strings.Builder
		for i := range nb {
			fmt.Fprintf(&build, `{"id":%d,"b":%d}`+"\n", r.Intn(12), i)
		}
		for i := range np {
			fmt.Fprintf(&probe, `{"cid":%d,"p":%d}`+"\n", r.Intn(12), i)
		}
		for _, jt := range []JoinType{JoinTypeInner, JoinTypeLeft} {
			spec := JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "m", Type: jt}
			inMem := runJoinSorted(t, probe.String(), build.String(), spec, 1<<20, 4) // fits: in-memory path
			grace := runJoinSorted(t, probe.String(), build.String(), spec, 400, 4)   // tiny: forces grace spill
			if !slices.Equal(inMem, grace) {
				t.Fatalf("iter %d jt %d: grace != in-memory\n in-mem (%d): %v\n grace  (%d): %v",
					iter, jt, len(inMem), inMem, len(grace), grace)
			}
		}
	}
}

// Explicit grace-mode checks (forced spill) for the shapes the fuzz test covers
// statistically, so a failure names the case.
func TestJoinGraceExplicit(t *testing.T) {
	build := `{"id":"A","n":"alice"}` + "\n" + `{"id":"A","n":"al2"}` + "\n" + `{"id":"B","n":"bob"}` + "\n"
	probe := `{"cid":"A","o":1}` + "\n" + `{"cid":"B","o":2}` + "\n" + `{"cid":"Z","o":3}` + "\n"
	// inner: A matches 2 build rows, B matches 1, Z drops = 3 rows.
	inner := runJoinSorted(t, probe, build, JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "m", Type: JoinTypeInner}, 400, 4)
	if len(inner) != 3 {
		t.Fatalf("grace inner rows = %d, want 3: %v", len(inner), inner)
	}
	// left: same 3 + Z with null match = 4 rows.
	left := runJoinSorted(t, probe, build, JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "m", Type: JoinTypeLeft}, 400, 4)
	if len(left) != 4 {
		t.Fatalf("grace left rows = %d, want 4: %v", len(left), left)
	}
	joinedZ := false
	for _, l := range left {
		if strings.Contains(l, `"cid":"Z"`) && strings.Contains(l, `"m":null`) {
			joinedZ = true
		}
	}
	if !joinedZ {
		t.Fatalf("grace left join lost the unmatched Z row: %v", left)
	}
}

func runJoin(t *testing.T, probe, build string, spec JoinSpec) string {
	t.Helper()
	if spec.Gov == nil {
		spec.Gov = mem.New(1 << 20)
	}
	ps := ndjson.NewReader(strings.NewReader(probe), ndjson.ReaderOptions{BatchRecords: 2})
	bs := ndjson.NewReader(strings.NewReader(build), ndjson.ReaderOptions{BatchRecords: 2})
	p := New(Join(ps, bs, spec), "join")
	var out bytes.Buffer
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write"); err != nil {
		t.Fatalf("run: %v", err)
	}
	return out.String()
}

func TestJoinInner(t *testing.T) {
	got := runJoin(t,
		`{"oid":1,"cid":"A"}`+"\n"+`{"oid":2,"cid":"B"}`+"\n",
		`{"id":"A","name":"Alice"}`+"\n"+`{"id":"B","name":"Bob"}`+"\n",
		JoinSpec{
			LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"),
			As: "cust", Type: JoinTypeInner,
		})
	want := `{"oid":1,"cid":"A","cust":{"id":"A","name":"Alice"}}` + "\n" +
		`{"oid":2,"cid":"B","cust":{"id":"B","name":"Bob"}}` + "\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestJoinInnerDropsUnmatched(t *testing.T) {
	got := runJoin(t,
		`{"oid":1,"cid":"A"}`+"\n"+`{"oid":9,"cid":"Z"}`+"\n",
		`{"id":"A","name":"Alice"}`+"\n",
		JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "cust", Type: JoinTypeInner})
	want := `{"oid":1,"cid":"A","cust":{"id":"A","name":"Alice"}}` + "\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestJoinLeftKeepsUnmatched(t *testing.T) {
	got := runJoin(t,
		`{"oid":9,"cid":"Z"}`+"\n",
		`{"id":"A","name":"Alice"}`+"\n",
		JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "cust", Type: JoinTypeLeft})
	want := `{"oid":9,"cid":"Z","cust":null}` + "\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestJoinMultipleMatches(t *testing.T) {
	got := runJoin(t,
		`{"oid":1,"cid":"A"}`+"\n",
		`{"id":"A","tag":"x"}`+"\n"+`{"id":"A","tag":"y"}`+"\n",
		JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "m", Type: JoinTypeInner})
	if n := strings.Count(got, "\n"); n != 2 {
		t.Fatalf("emitted %d rows, want 2 (one per build match):\n%s", n, got)
	}
	if !strings.Contains(got, `"tag":"x"`) || !strings.Contains(got, `"tag":"y"`) {
		t.Fatalf("both matches expected:\n%s", got)
	}
}

func TestJoinNullKeysNeverMatch(t *testing.T) {
	// Probe and build both have a null key; must NOT join (SQL null semantics).
	got := runJoin(t,
		`{"oid":1}`+"\n",
		`{"name":"NoKey"}`+"\n",
		JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "m", Type: JoinTypeInner})
	if got != "" {
		t.Fatalf("null keys must not match, got:\n%s", got)
	}
}

func TestJoinWatermarkTooSmallForOneRow(t *testing.T) {
	// A watermark smaller than a single build row can't spill a partial row, so
	// the join fails honestly rather than OOM (larger watermarks spill — see the
	// grace tests).
	ps := ndjson.NewReader(strings.NewReader(`{"cid":"A"}`+"\n"), ndjson.ReaderOptions{})
	bs := ndjson.NewReader(strings.NewReader(`{"id":"A"}`+"\n"+`{"id":"B"}`+"\n"), ndjson.ReaderOptions{BatchRecords: 1})
	p := New(Join(ps, bs, JoinSpec{
		LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"),
		As: "m", Type: JoinTypeInner, Gov: mem.New(10), // 10 bytes: below one row
	}), "join")
	_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "write")
	if err == nil || !strings.Contains(err.Error(), "too small for a single build row") {
		t.Fatalf("err = %v, want single-row watermark error", err)
	}
}

func TestJoinValidation(t *testing.T) {
	cases := []struct {
		name string
		spec JoinSpec
		want string
	}{
		{"no gov", JoinSpec{As: "x"}, "requires a governor"},
		{"no as", JoinSpec{Gov: mem.New(1 << 20)}, "requires an As field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Join(
				ndjson.NewReader(strings.NewReader(""), ndjson.ReaderOptions{}),
				ndjson.NewReader(strings.NewReader(""), ndjson.ReaderOptions{}),
				tc.spec), "join")
			_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "w")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// A large-probe / small-build enrichment stays bounded and correct.
func TestJoinLargeProbeSmallBuild(t *testing.T) {
	var probe strings.Builder
	for i := range 500 {
		probe.WriteString(`{"oid":`)
		probe.WriteByte(byte('0' + i%10))
		probe.WriteString(`,"cid":"A"}` + "\n")
	}
	got := runJoin(t, probe.String(), `{"id":"A","name":"Alice"}`+"\n",
		JoinSpec{LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"), As: "cust", Type: JoinTypeInner})
	if n := strings.Count(got, "\n"); n != 500 {
		t.Fatalf("emitted %d rows, want 500", n)
	}
}
