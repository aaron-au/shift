package flow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/stream"
)

// Document parse/validate tests live with the model in pkg/flowdoc; this
// file covers compilation semantics only.

// runDoc executes a flow's ops over NDJSON input in-process.
func runDoc(t *testing.T, opsJSON, input string) string {
	t.Helper()
	var ops []Op
	if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
		t.Fatal(err)
	}
	d := &Document{Name: "t", Source: Endpoint{Connector: "x", Action: "y"},
		Ops: ops, Sink: Endpoint{Connector: "x", Action: "y"}}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	src := ndjson.NewReader(strings.NewReader(input), ndjson.ReaderOptions{})
	p, err := Apply(d, stream.New(src, "src"), CompileOptions{Gov: mem.New(1 << 20), SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	w := ndjson.NewWriter(&out)
	if _, err := p.Run(context.Background(), w, "sink"); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// TestApplyNamesOpsByStepID: a v2 graph compiles its transform steps with
// each operator labeled by its step id (the telemetry + OpError routing
// key), and Apply lowers only the middle steps (endpoints bound by caller).
func TestApplyNamesOpsByStepID(t *testing.T) {
	src := Step{ID: "in", Connector: "x", Action: "y", OnSuccess: "keep"}
	src.Type = "source"
	keep := Step{ID: "keep", OnComplete: "shape"}
	keep.Op = Op{Type: "filter", Path: "$.a", Cmp: "exists"}
	shape := Step{ID: "shape", OnComplete: "out"}
	shape.Op = Op{Type: "flatten", Sep: "."}
	out := Step{ID: "out", Connector: "x", Action: "y"}
	out.Type = "sink"
	d := &Document{Name: "t", Start: "in", Steps: []Step{src, keep, shape, out}}

	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	plan, err := d.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{plan.Main[0].ID, plan.Main[3].ID}; got[0] != "in" || got[1] != "out" {
		t.Fatalf("endpoints = %v", got)
	}

	rd := ndjson.NewReader(strings.NewReader(`{"a":{"b":1}}`+"\n"), ndjson.ReaderOptions{})
	p, err := Apply(d, stream.New(rd, plan.Main[0].ID), CompileOptions{Gov: mem.New(1 << 20), SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var out2 strings.Builder
	rep, err := p.Run(t.Context(), ndjson.NewWriter(&out2), plan.Main[3].ID)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, op := range rep.Ops {
		names = append(names, op.Name)
	}
	want := []string{"in", "keep", "shape", "out"}
	if len(names) != len(want) {
		t.Fatalf("op names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("op names = %v, want %v", names, want)
		}
	}
}

func TestFilterSemantics(t *testing.T) {
	input := `{"a":1,"s":"x"}` + "\n" + `{"a":5,"s":"y"}` + "\n" + `{"a":null}` + "\n" + `{}` + "\n"

	cases := map[string]string{
		`[{"type":"filter","path":"$.a","op":"gte","value":5}]`:  `{"a":5,"s":"y"}` + "\n",
		`[{"type":"filter","path":"$.a","op":"lt","value":2}]`:   `{"a":1,"s":"x"}` + "\n",
		`[{"type":"filter","path":"$.s","op":"eq","value":"y"}]`: `{"a":5,"s":"y"}` + "\n",
		`[{"type":"filter","path":"$.a","op":"exists"}]`:         `{"a":1,"s":"x"}` + "\n" + `{"a":5,"s":"y"}` + "\n",
		`[{"type":"filter","path":"$.a","op":"ne","value":1}]`:   `{"a":5,"s":"y"}` + "\n" + `{"a":null}` + "\n",
	}
	for ops, want := range cases {
		if got := runDoc(t, ops, input); got != want {
			t.Errorf("%s:\n got %q\nwant %q", ops, got, want)
		}
	}
	// ne with a missing field: path miss → filtered out (documented).
	got := runDoc(t, `[{"type":"filter","path":"$.a","op":"ne","value":99}]`, input)
	if strings.Contains(got, "{}") {
		t.Errorf("missing field should not pass ne: %q", got)
	}
}

func TestOpsPipelineEndToEnd(t *testing.T) {
	input := `{"id":"1","amount":"10.5","meta":{"region":"AU"}}` + "\n" +
		`{"id":"2","amount":"3.25","meta":{"region":"AU"}}` + "\n" +
		`{"id":"3","amount":"7.0","meta":{"region":"NZ"}}` + "\n"
	ops := `[
	  {"type":"coerce","rules":[{"field":"amount","to":"float"}]},
	  {"type":"flatten","sep":"_"},
	  {"type":"aggregate","key":"$.meta_region","aggs":[{"op":"count","out":"n"},{"op":"sum","path":"$.amount","out":"total"}]}
	]`
	got := runDoc(t, ops, input)
	if !strings.Contains(got, `"meta_region":"AU","n":2,"total":13.75`) {
		t.Errorf("AU aggregate wrong: %s", got)
	}
	if !strings.Contains(got, `"meta_region":"NZ","n":1,"total":7`) {
		t.Errorf("NZ aggregate wrong: %s", got)
	}
}
