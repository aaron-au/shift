package stream

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

func runNDJSON(t *testing.T, input string, build func(*Pipeline) *Pipeline) (string, Report) {
	t.Helper()
	src := ndjson.NewReader(strings.NewReader(input), ndjson.ReaderOptions{BatchRecords: 2})
	p := build(New(src, "read"))
	var out bytes.Buffer
	rep, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write")
	if err != nil {
		t.Fatal(err)
	}
	return out.String(), rep
}

func TestProject(t *testing.T) {
	in := `{"id":1,"name":"a","addr":{"city":"melb"},"junk":true}` + "\n" +
		`{"id":2,"name":"b","addr":{"city":"syd"}}` + "\n"
	out, rep := runNDJSON(t, in, func(p *Pipeline) *Pipeline {
		return p.Project(
			ProjectField{From: record.MustParsePath("$.id")},
			ProjectField{Out: "city", From: record.MustParsePath("$.addr.city")},
			ProjectField{Out: "missing", From: record.MustParsePath("$.nope")},
		)
	})
	want := `{"id":1,"city":"melb","missing":null}` + "\n" +
		`{"id":2,"city":"syd","missing":null}` + "\n"
	if out != want {
		t.Errorf("got:\n%s\nwant:\n%s", out, want)
	}
	if rep.RecordsOut != 2 {
		t.Errorf("records out = %d", rep.RecordsOut)
	}
}

func TestFilterAndFullyFilteredBatches(t *testing.T) {
	var in strings.Builder
	for i := range 10 {
		if i%2 == 0 {
			in.WriteString(`{"keep":true,"i":`)
		} else {
			in.WriteString(`{"keep":false,"i":`)
		}
		in.WriteString(string(rune('0' + i)))
		in.WriteString("}\n")
	}
	out, rep := runNDJSON(t, in.String(), func(p *Pipeline) *Pipeline {
		return p.Filter("keep-only", func(v record.Value) bool {
			k, _ := v.Field("keep")
			return k.Bool()
		})
	})
	if got := strings.Count(out, "\n"); got != 5 {
		t.Errorf("kept %d records, want 5", got)
	}
	var filterStats *OpStats
	for i := range rep.Ops {
		if rep.Ops[i].Name == "keep-only" {
			filterStats = &rep.Ops[i]
		}
	}
	if filterStats == nil || filterStats.RecordsIn != 10 || filterStats.RecordsOut != 5 {
		t.Errorf("filter stats = %+v", filterStats)
	}
}

func TestCoerce(t *testing.T) {
	in := `{"n":"42","f":"1.5","b":"true","s":7,"nul":null}` + "\n"
	out, _ := runNDJSON(t, in, func(p *Pipeline) *Pipeline {
		return p.Coerce(
			CoerceRule{Field: "n", To: record.KindInt},
			CoerceRule{Field: "f", To: record.KindFloat},
			CoerceRule{Field: "b", To: record.KindBool},
			CoerceRule{Field: "s", To: record.KindString},
			CoerceRule{Field: "nul", To: record.KindInt},
		)
	})
	want := `{"n":42,"f":1.5,"b":true,"s":"7","nul":null}` + "\n"
	if out != want {
		t.Errorf("got %s want %s", out, want)
	}
}

func TestCoerceFailure(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(`{"n":"nope"}`+"\n"), ndjson.ReaderOptions{})
	p := New(src, "read").Coerce(CoerceRule{Field: "n", To: record.KindInt})
	_, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "write")
	if err == nil || !strings.Contains(err.Error(), "coerce") {
		t.Fatalf("err = %v", err)
	}
	// The failure is tagged with the operator name (the runner sets this to
	// the flow step id) so it can be routed to that step's error handler.
	var oe *OpError
	if !errors.As(err, &oe) || oe.Op != "coerce" {
		t.Fatalf("OpError not recoverable: %v", err)
	}
}

func TestFlatten(t *testing.T) {
	in := `{"a":1,"b":{"c":2,"d":{"e":3}},"f":[{"g":4}]}` + "\n"
	out, _ := runNDJSON(t, in, func(p *Pipeline) *Pipeline { return p.Flatten(".") })
	want := `{"a":1,"b.c":2,"b.d.e":3,"f":[{"g":4}]}` + "\n"
	if out != want {
		t.Errorf("got %s want %s", out, want)
	}
}

func TestChainedOps(t *testing.T) {
	in := `{"id":"1","amount":"10.5","meta":{"region":"AU"}}` + "\n" +
		`{"id":"2","amount":"3.0","meta":{"region":"NZ"}}` + "\n" +
		`{"id":"3","amount":"99.9","meta":{"region":"AU"}}` + "\n"
	out, rep := runNDJSON(t, in, func(p *Pipeline) *Pipeline {
		return p.
			Coerce(CoerceRule{Field: "amount", To: record.KindFloat}).
			Flatten("_").
			Filter("au-only", func(v record.Value) bool {
				r, _ := v.Field("meta_region")
				return r.String() == "AU"
			}).
			Project(
				ProjectField{From: record.MustParsePath("$.id")},
				ProjectField{From: record.MustParsePath("$.amount")},
			)
	})
	want := `{"id":"1","amount":10.5}` + "\n" + `{"id":"3","amount":99.9}` + "\n"
	if out != want {
		t.Errorf("got:\n%s\nwant:\n%s", out, want)
	}
	if len(rep.Ops) != 6 { // read, coerce, flatten, filter, project, write
		t.Errorf("ops = %d: %+v", len(rep.Ops), rep.Ops)
	}
}

func TestBuildErrorSurfacesAtRun(t *testing.T) {
	src := ndjson.NewReader(strings.NewReader(""), ndjson.ReaderOptions{})
	p := New(src, "read").Project(ProjectField{From: record.MustParsePath("$[0]")}) // no leaf name
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&bytes.Buffer{}), "write"); err == nil {
		t.Fatal("expected build error")
	}
}
