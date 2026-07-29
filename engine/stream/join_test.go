package stream

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

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

func TestJoinWatermarkExceeded(t *testing.T) {
	// A governor too small for the build side fails honestly, never OOMs.
	ps := ndjson.NewReader(strings.NewReader(`{"cid":"A"}`+"\n"), ndjson.ReaderOptions{})
	bs := ndjson.NewReader(strings.NewReader(`{"id":"A"}`+"\n"+`{"id":"B"}`+"\n"), ndjson.ReaderOptions{BatchRecords: 1})
	p := New(Join(ps, bs, JoinSpec{
		LeftKey: record.MustParsePath("$.cid"), RightKey: record.MustParsePath("$.id"),
		As: "m", Type: JoinTypeInner, Gov: mem.New(10), // 10 bytes: too small
	}), "join")
	_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "write")
	if err == nil || !strings.Contains(err.Error(), "watermark") {
		t.Fatalf("err = %v, want watermark error", err)
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
