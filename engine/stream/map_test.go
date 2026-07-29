package stream

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

func TestMapRestructure(t *testing.T) {
	in := `{"orderId":7,"name":"Al","amount":"12.5","region":"AU"}` + "\n" +
		`{"orderId":8,"name":"Bo","amount":"3"}` + "\n"
	out, _ := runNDJSON(t, in, func(p *Pipeline) *Pipeline {
		return p.Map([]MapField{
			{Out: []string{"id"}, From: record.MustParsePath("$.orderId"), FromSet: true},
			{Out: []string{"customer", "name"}, From: record.MustParsePath("$.name"), FromSet: true},
			{Out: []string{"customer", "tier"}, Const: record.UnsafeString([]byte("gold")), ConstSet: true},
			{Out: []string{"total"}, From: record.MustParsePath("$.amount"), FromSet: true, To: record.KindFloat, ToSet: true},
			{Out: []string{"label"}, Concat: []MapPart{{Lit: "order-"}, {Path: record.MustParsePath("$.orderId"), IsPath: true}}},
			{Out: []string{"region"}, From: record.MustParsePath("$.region"), FromSet: true, Default: record.UnsafeString([]byte("unknown")), DefaultSet: true},
		})
	})
	want1 := `{"id":7,"customer":{"name":"Al","tier":"gold"},"total":12.5,"label":"order-7","region":"AU"}`
	want2 := `{"id":8,"customer":{"name":"Bo","tier":"gold"},"total":3,"label":"order-8","region":"unknown"}`
	if !strings.Contains(out, want1) {
		t.Fatalf("rec1 mismatch:\n got %s\nwant %s", out, want1)
	}
	if !strings.Contains(out, want2) {
		t.Fatalf("rec2 mismatch:\n got %s\nwant %s", out, want2)
	}
}

func TestMapCoerceError(t *testing.T) {
	// A value that cannot coerce surfaces an error (routed to onFailure upstream).
	src := ndjson.NewReader(strings.NewReader(`{"x":"notanumber"}`+"\n"), ndjson.ReaderOptions{})
	p := New(src, "src").Map([]MapField{
		{Out: []string{"n"}, From: record.MustParsePath("$.x"), FromSet: true, To: record.KindInt, ToSet: true},
	})
	_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "sink")
	if err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("err = %v, want a parse error", err)
	}
}

func TestMapPathCollision(t *testing.T) {
	// "a" and "a.b" cannot both be outputs (leaf vs branch).
	src := ndjson.NewReader(strings.NewReader(`{}`+"\n"), ndjson.ReaderOptions{})
	p := New(src, "src").Map([]MapField{
		{Out: []string{"a"}, Const: record.Null(), ConstSet: true},
		{Out: []string{"a", "b"}, Const: record.Null(), ConstSet: true},
	})
	_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "sink")
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("err = %v, want a collision error", err)
	}
}

func TestMapConcatSkipsMissing(t *testing.T) {
	// A missing concat path contributes nothing; null renders empty.
	out, _ := runNDJSON(t, `{"a":"x"}`+"\n", func(p *Pipeline) *Pipeline {
		return p.Map([]MapField{
			{Out: []string{"j"}, Concat: []MapPart{
				{Path: record.MustParsePath("$.a"), IsPath: true},
				{Lit: "-"},
				{Path: record.MustParsePath("$.missing"), IsPath: true},
				{Lit: "-"},
				{Path: record.MustParsePath("$.a"), IsPath: true},
			}},
		})
	})
	if !strings.Contains(out, `{"j":"x--x"}`) {
		t.Fatalf("concat with missing path wrong: %s", out)
	}
}
