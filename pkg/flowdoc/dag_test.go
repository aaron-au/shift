package flowdoc

import (
	"slices"
	"strings"
	"testing"
)

// A valid tee: source → tee → two sinks (one connector, one @discard).
const teeGraph = `{
  "name":"fanout",
  "steps":[
    {"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},
    {"id":"t","type":"tee","branches":["a","b"]},
    {"id":"a","type":"sink","connector":"http","action":"post"},
    {"id":"b","type":"sink","connector":"@discard"}
  ]
}`

func TestDAG_TeeValid(t *testing.T) {
	d, err := Parse([]byte(teeGraph))
	if err != nil {
		t.Fatalf("valid tee rejected: %v", err)
	}
	p, err := d.Plan()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !p.Multi {
		t.Fatal("expected Multi (DAG) plan")
	}
	if p.Main != nil {
		t.Fatalf("Main must be nil for a DAG plan, got %d steps", len(p.Main))
	}
	if !slices.Equal(p.Sources, []string{"in"}) {
		t.Fatalf("sources = %v, want [in]", p.Sources)
	}
	if !slices.Equal(p.Data["t"], []string{"a", "b"}) {
		t.Fatalf("tee data edges = %v, want [a b]", p.Data["t"])
	}
	wantSinks := []string{"a", "b"}
	got := slices.Clone(p.Sinks)
	slices.Sort(got)
	if !slices.Equal(got, wantSinks) {
		t.Fatalf("sinks = %v, want %v", got, wantSinks)
	}
	if _, ok := p.Nodes["t"]; !ok {
		t.Fatal("tee node missing from Nodes")
	}
}

func TestDAG_RouterValid(t *testing.T) {
	const g = `{
      "name":"switch",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","onSuccess":"r"},
        {"id":"r","type":"router",
         "routes":[{"path":"$.kind","op":"eq","value":"vip","to":"hot"}],
         "default":"cold"},
        {"id":"hot","type":"sink","connector":"@discard"},
        {"id":"cold","type":"sink","connector":"@discard"}
      ]
    }`
	d, err := Parse([]byte(g))
	if err != nil {
		t.Fatalf("valid router rejected: %v", err)
	}
	p, _ := d.Plan()
	if !slices.Equal(p.Data["r"], []string{"hot", "cold"}) {
		t.Fatalf("router data edges = %v, want [hot cold]", p.Data["r"])
	}
}

func TestDAG_MergeConcatValid(t *testing.T) {
	const g = `{
      "name":"union",
      "steps":[
        {"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},
        {"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},
        {"id":"m","type":"merge","inputs":["s1","s2"],"mode":"concat","onSuccess":"out"},
        {"id":"out","type":"sink","connector":"@discard"}
      ]
    }`
	d, err := Parse([]byte(g))
	if err != nil {
		t.Fatalf("valid concat rejected: %v", err)
	}
	p, _ := d.Plan()
	src := slices.Clone(p.Sources)
	slices.Sort(src)
	if !slices.Equal(src, []string{"s1", "s2"}) {
		t.Fatalf("sources = %v, want [s1 s2]", src)
	}
	if !slices.Equal(p.Data["m"], []string{"out"}) {
		t.Fatalf("merge successor = %v, want [out]", p.Data["m"])
	}
}

func TestDAG_MergeJoinValid(t *testing.T) {
	const g = `{
      "name":"enrich",
      "steps":[
        {"id":"orders","type":"source","connector":"http","action":"get","onSuccess":"j"},
        {"id":"cust","type":"source","connector":"http","action":"get","onSuccess":"j"},
        {"id":"j","type":"merge","inputs":["orders","cust"],"mode":"join",
         "on":{"left":"$.customer_id","right":"$.id"},"joinType":"left","build":"cust","as":"customer",
         "onSuccess":"out"},
        {"id":"out","type":"sink","connector":"@discard"}
      ]
    }`
	if _, err := Parse([]byte(g)); err != nil {
		t.Fatalf("valid join rejected: %v", err)
	}
}

// A plain transform carrying branches is sugar for an implicit tee.
func TestDAG_BranchesSugar(t *testing.T) {
	const g = `{
      "name":"sugar",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","onSuccess":"f"},
        {"id":"f","type":"filter","path":"$.a","op":"exists","branches":["x","y"]},
        {"id":"x","type":"sink","connector":"@discard"},
        {"id":"y","type":"sink","connector":"@discard"}
      ]
    }`
	d, err := Parse([]byte(g))
	if err != nil {
		t.Fatalf("branches sugar rejected: %v", err)
	}
	p, _ := d.Plan()
	if !p.Multi || !slices.Equal(p.Data["f"], []string{"x", "y"}) {
		t.Fatalf("sugar tee not lowered: multi=%v data=%v", p.Multi, p.Data["f"])
	}
}

// Per-node onFailure handler on a DAG node.
func TestDAG_PerNodeHandler(t *testing.T) {
	const g = `{
      "name":"handled",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},
        {"id":"t","type":"tee","branches":["a","b"],"onFailure":"dead"},
        {"id":"a","type":"sink","connector":"@discard"},
        {"id":"b","type":"sink","connector":"@discard"},
        {"id":"dead","type":"sink","connector":"http","action":"post"}
      ]
    }`
	d, err := Parse([]byte(g))
	if err != nil {
		t.Fatalf("handler graph rejected: %v", err)
	}
	p, _ := d.Plan()
	if h := p.HandlerFor("t"); h == nil || h.ID != "dead" {
		t.Fatalf("handler for tee = %v, want dead", h)
	}
	if _, ok := p.Nodes["dead"]; ok {
		t.Fatal("handler must not be a data node")
	}
}

func TestDAG_Invalid(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"tee too few branches",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a"]},{"id":"a","type":"sink","connector":"@discard"}]}`,
			"tee needs at least 2 branches"},
		{"tee with happy edge",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","b"],"onSuccess":"a"},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"must not also have onSuccess"},
		{"router too few targets",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"r"},{"id":"r","type":"router","routes":[{"path":"$.k","op":"exists","to":"a"}]},{"id":"a","type":"sink","connector":"@discard"}]}`,
			"router needs at least 2 targets"},
		{"router bad predicate",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"r"},{"id":"r","type":"router","routes":[{"path":"$.k","op":"nope","to":"a"}],"default":"b"},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"unknown filter op"},
		{"merge too few inputs",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1"],"mode":"concat","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"merge needs at least 2 inputs"},
		{"join wrong input count",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s3","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2","s3"],"mode":"join","on":{"left":"$.a","right":"$.b"},"joinType":"inner","build":"s2","as":"c","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"exactly 2 inputs"},
		{"join missing on",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"join","joinType":"inner","build":"s2","as":"c","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"needs on.left and on.right"},
		{"join bad type",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"join","on":{"left":"$.a","right":"$.b"},"joinType":"outer","build":"s2","as":"c","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"join type"},
		{"join build not an input",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"join","on":{"left":"$.a","right":"$.b"},"joinType":"inner","build":"zzz","as":"c","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"must name one of its inputs"},
		{"concat with join key",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"concat","on":{"left":"$.a","right":"$.b"},"onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"concat takes no join key"},
		{"edge to unknown step",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","ghost"]},{"id":"a","type":"sink","connector":"@discard"}]}`,
			"edge to unknown step"},
		{"edge into source",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","in2"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"in2","type":"source","connector":"http","action":"get","onSuccess":"a"}]}`,
			"edge into source"},
		{"cycle",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","f"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"f","type":"filter","path":"$.a","op":"exists","onSuccess":"t"}]}`,
			"cycle in data path"},
		{"terminal non-sink",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","f"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"f","type":"filter","path":"$.a","op":"exists"}]}`,
			"only a sink terminates"},
		{"no sink at all",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["f","g"]},{"id":"f","type":"filter","path":"$.a","op":"exists","onSuccess":"g"},{"id":"g","type":"filter","path":"$.b","op":"exists","onSuccess":"f"}]}`,
			"needs at least one sink"},
		{"merge input does not flow",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"out2"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"concat","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"},{"id":"out2","type":"sink","connector":"@discard"}]}`,
			"does not flow to it"},
		{"undeclared producer into merge",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s3","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"concat","onSuccess":"out"},{"id":"out","type":"sink","connector":"@discard"}]}`,
			"is not a declared input"},
		{"sink with happy edge",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","b"]},{"id":"a","type":"sink","connector":"@discard","onSuccess":"b"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"sink must not have a happy-path edge"},
		{"merge with branches",
			`{"name":"x","steps":[{"id":"s1","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"s2","type":"source","connector":"http","action":"get","onSuccess":"m"},{"id":"m","type":"merge","inputs":["s1","s2"],"mode":"concat","branches":["a","b"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"merge fans in via inputs"},
		{"tee with foreign fields",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t"},{"id":"t","type":"tee","branches":["a","b"],"inputs":["in"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"tee takes only branches"},
		{"single branch sugar",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"f"},{"id":"f","type":"filter","path":"$.a","op":"exists","branches":["a"]},{"id":"a","type":"sink","connector":"@discard"}]}`,
			"use onSuccess for a single successor"},
		{"branches and happy together",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"f"},{"id":"f","type":"filter","path":"$.a","op":"exists","onSuccess":"a","branches":["a","b"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"}]}`,
			"both branches and onSuccess"},
		{"handler with outgoing edge",
			`{"name":"x","steps":[{"id":"in","type":"source","connector":"http","action":"get","onSuccess":"t","onFailure":"dead"},{"id":"t","type":"tee","branches":["a","b"]},{"id":"a","type":"sink","connector":"@discard"},{"id":"b","type":"sink","connector":"@discard"},{"id":"dead","type":"sink","connector":"http","action":"post","onSuccess":"a"}]}`,
			"must not have outgoing edges"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatalf("expected rejection, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

// GraphView must render a v3 DAG (branch/route edges) without indexing the
// nil linear Main.
func TestDAG_GraphView(t *testing.T) {
	d, err := Parse([]byte(teeGraph))
	if err != nil {
		t.Fatal(err)
	}
	g, err := d.GraphView()
	if err != nil {
		t.Fatalf("graphview: %v", err)
	}
	var branches int
	for _, e := range g.Edges {
		if e.Kind == "branch" {
			branches++
		}
	}
	if branches != 2 {
		t.Fatalf("branch edges = %d, want 2", branches)
	}

	// Router renders route edges.
	rd, _ := Parse([]byte(`{"name":"r","steps":[
      {"id":"in","type":"source","connector":"http","action":"get","onSuccess":"r"},
      {"id":"r","type":"router","routes":[{"path":"$.k","op":"exists","to":"a"}],"default":"b"},
      {"id":"a","type":"sink","connector":"@discard"},
      {"id":"b","type":"sink","connector":"@discard"}]}`))
	rg, err := rd.GraphView()
	if err != nil {
		t.Fatalf("router graphview: %v", err)
	}
	var routes int
	for _, e := range rg.Edges {
		if e.Kind == "route" {
			routes++
		}
	}
	if routes != 2 {
		t.Fatalf("route edges = %d, want 2 (route + default)", routes)
	}
}
