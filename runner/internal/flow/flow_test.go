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

func TestParseAndValidate(t *testing.T) {
	good := `{
	  "name":"orders",
	  "source":{"connector":"http","action":"get","config":{"url":"https://x"}},
	  "ops":[
	    {"type":"filter","path":"$.active","op":"eq","value":true},
	    {"type":"filter","path":"$.amount","op":"gte","value":10.5},
	    {"type":"filter","path":"$.name","op":"exists"},
	    {"type":"coerce","rules":[{"field":"amount","to":"float"}]},
	    {"type":"flatten","sep":"."},
	    {"type":"project","fields":[{"path":"$.id"},{"out":"c","path":"$.addr.city"}]},
	    {"type":"aggregate","key":"$.region","aggs":[{"op":"count","out":"n"},{"op":"sum","path":"$.amount","out":"total"}]}
	  ],
	  "sink":{"connector":"http","action":"post","config":{"url":"https://y"}}
	}`
	if _, err := Parse([]byte(good)); err != nil {
		t.Fatalf("good doc rejected: %v", err)
	}

	bad := []string{
		`{"source":{"connector":"a","action":"b"},"sink":{"connector":"c","action":"d"}}`,                                                                                        // no name
		`{"name":"x","source":{"connector":"a"},"sink":{"connector":"c","action":"d"}}`,                                                                                          // source missing action
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"nope"}],"sink":{"connector":"c","action":"d"}}`,                                                     // unknown op
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"bad","op":"eq","value":1}],"sink":{"connector":"c","action":"d"}}`,                  // bad path
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"$.a","op":"eq"}],"sink":{"connector":"c","action":"d"}}`,                            // eq without value
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"$.a","op":"eq","value":{"o":1}}],"sink":{"connector":"c","action":"d"}}`,            // non-scalar value
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"coerce","rules":[{"field":"f","to":"complex"}]}],"sink":{"connector":"c","action":"d"}}`,            // bad kind
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"flatten"}],"sink":{"connector":"c","action":"d"}}`,                                                  // flatten no sep
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"aggregate","key":"$.k","aggs":[{"op":"median","out":"m"}]}],"sink":{"connector":"c","action":"d"}}`, // bad agg
	}
	for i, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("bad doc %d accepted", i)
		}
	}
}

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
	p, err := d.Apply(stream.New(src, "src"), CompileOptions{Gov: mem.New(1 << 20), SpillDir: t.TempDir()})
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
