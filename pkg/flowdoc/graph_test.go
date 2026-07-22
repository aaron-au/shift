package flowdoc

import (
	"encoding/json"
	"slices"
	"testing"
)

// A well-formed graph: source → filter → sink, with a dead-letter handler
// hung off the source's onFailure (a catch-all for the whole flow).
const goodGraph = `{
  "name":"orders",
  "start":"in",
  "steps":[
    {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},
     "onSuccess":"keep","onFailure":"dead"},
    {"id":"keep","type":"filter","path":"$.active","op":"eq","value":true,"onComplete":"out"},
    {"id":"out","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}},
    {"id":"dead","type":"sink","connector":"http","action":"post","config":{"url":"https://dlq"}}
  ]
}`

func TestGraphParseAndPlan(t *testing.T) {
	d, err := Parse([]byte(goodGraph))
	if err != nil {
		t.Fatalf("good graph rejected: %v", err)
	}
	p, err := d.Plan()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	wantMain := []string{"in", "keep", "out"}
	if len(p.Main) != len(wantMain) {
		t.Fatalf("main = %d steps, want %d", len(p.Main), len(wantMain))
	}
	for i, id := range wantMain {
		if p.Main[i].ID != id {
			t.Fatalf("main[%d] = %q, want %q", i, p.Main[i].ID, id)
		}
	}
	// The source's onFailure catches every main step (nearest preceding).
	for _, id := range wantMain {
		h := p.HandlerFor(id)
		if h == nil || h.ID != "dead" {
			t.Fatalf("handler for %q = %v, want dead", id, h)
		}
	}
}

func TestLinearLowersToGraph(t *testing.T) {
	d, err := Parse([]byte(`{
	  "name":"x",
	  "source":{"connector":"http","action":"get"},
	  "ops":[{"type":"filter","path":"$.a","op":"exists"},{"type":"flatten","sep":"."}],
	  "sink":{"connector":"http","action":"post"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	p, err := d.Plan()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"source", "op0", "op1", "sink"}
	if len(p.Main) != len(want) {
		t.Fatalf("main = %d, want %d", len(p.Main), len(want))
	}
	for i, id := range want {
		if p.Main[i].ID != id {
			t.Fatalf("main[%d] = %q, want %q", i, p.Main[i].ID, id)
		}
	}
	if p.Main[0].Type != "source" || p.Main[3].Type != "sink" {
		t.Fatalf("endpoints mislabeled: %q..%q", p.Main[0].Type, p.Main[3].Type)
	}
	if len(p.Catch) != 0 {
		t.Fatalf("linear form has no handlers, got %d", len(p.Catch))
	}
}

func TestGraphValidation(t *testing.T) {
	bad := map[string]string{
		"both forms": `{"name":"x","source":{"connector":"a","action":"b"},
		  "steps":[{"id":"s","type":"source","connector":"a","action":"b","onComplete":"k"},
		           {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"dup id": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"s"},
		  {"id":"s","type":"sink","connector":"c","action":"d"}]}`,
		"dangling edge": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"ghost"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"cycle": `{"name":"x","start":"s","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"f"},
		  {"id":"f","type":"filter","path":"$.a","op":"exists","onComplete":"s"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"two happy edges": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onSuccess":"k","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"handler not sink": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"k","onFailure":"h"},
		  {"id":"k","type":"sink","connector":"c","action":"d"},
		  {"id":"h","type":"filter","path":"$.a","op":"exists"}]}`,
		"reserved type": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"w"},
		  {"id":"w","type":"starlark","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"sink with happy edge": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d","onComplete":"s"}]}`,
		"no source": `{"name":"x","steps":[
		  {"id":"f","type":"filter","path":"$.a","op":"exists","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"start not source": `{"name":"x","start":"k","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
		"orphan step": `{"name":"x","start":"s","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b","onComplete":"k"},
		  {"id":"k","type":"sink","connector":"c","action":"d"},
		  {"id":"lost","type":"filter","path":"$.a","op":"exists","onComplete":"k"}]}`,
		"non-terminal has no edge": `{"name":"x","steps":[
		  {"id":"s","type":"source","connector":"a","action":"b"},
		  {"id":"k","type":"sink","connector":"c","action":"d"}]}`,
	}
	for name, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

func TestDiscardSink(t *testing.T) {
	// Valid: a source-side action terminating at the built-in @discard sink —
	// the single-op flow shape (no action needed on @discard).
	ok := `{"name":"x","start":"op","steps":[
	  {"id":"op","type":"source","connector":"sftp","action":"mkdir","config":{"host":"h","user":"u"},"onSuccess":"end"},
	  {"id":"end","type":"sink","connector":"@discard"}]}`
	if _, err := Parse([]byte(ok)); err != nil {
		t.Fatalf("@discard sink rejected: %v", err)
	}
	// @discard is sink-only, and is not a registry connector (excluded from the
	// resolvable connector set).
	d, err := Parse([]byte(ok))
	if err == nil {
		for _, c := range d.Connectors() {
			if c == DiscardSink {
				t.Fatalf("@discard leaked into resolvable connectors: %v", d.Connectors())
			}
		}
	}
	if _, err := Parse([]byte(`{"name":"x","start":"d","steps":[
	  {"id":"d","type":"source","connector":"@discard","onComplete":"k"},
	  {"id":"k","type":"sink","connector":"c","action":"d"}]}`)); err == nil {
		t.Error("@discard as source accepted, want rejection")
	}
}

func TestGraphView(t *testing.T) {
	d, err := Parse([]byte(goodGraph)) // in→keep→out, source onFailure→dead
	if err != nil {
		t.Fatal(err)
	}
	g, err := d.GraphView()
	if err != nil {
		t.Fatal(err)
	}
	if g.Start != "in" {
		t.Fatalf("start = %q", g.Start)
	}
	wantMain := []string{"in", "keep", "out"}
	if len(g.Main) != 3 || g.Main[0] != "in" || g.Main[2] != "out" {
		t.Fatalf("main = %v, want %v", g.Main, wantMain)
	}
	if len(g.Nodes) != 4 { // in, keep, out, dead
		t.Fatalf("nodes = %d, want 4", len(g.Nodes))
	}
	roles := map[string]string{}
	for _, n := range g.Nodes {
		roles[n.ID] = n.Role
	}
	if roles["dead"] != "handler" || roles["in"] != "main" {
		t.Fatalf("roles = %v", roles)
	}
	var kinds []string
	for _, e := range g.Edges {
		kinds = append(kinds, e.Kind)
	}
	// success (in→keep), complete (keep→out), failure (in→dead)
	if !slices.Contains(kinds, "success") || !slices.Contains(kinds, "complete") || !slices.Contains(kinds, "failure") {
		t.Fatalf("edge kinds = %v", kinds)
	}

	// Linear form: synthesized nodes chained by complete edges.
	lin, _ := Parse([]byte(`{"name":"x","source":{"connector":"gen","action":"gen"},
	  "ops":[{"type":"flatten","sep":"."}],"sink":{"connector":"gen","action":"discard"}}`))
	lg, err := lin.GraphView()
	if err != nil {
		t.Fatal(err)
	}
	if len(lg.Main) != 3 || len(lg.Edges) != 2 {
		t.Fatalf("linear graph main=%v edges=%d", lg.Main, len(lg.Edges))
	}
	for _, e := range lg.Edges {
		if e.Kind != "complete" {
			t.Fatalf("linear edge kind = %q, want complete", e.Kind)
		}
	}
}

func TestWebhookSource(t *testing.T) {
	// Valid: @webhook as a source, real connector sink.
	ok := `{"name":"h","source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"http","action":"post"}}`
	d, err := Parse([]byte(ok))
	if err != nil {
		t.Fatalf("valid @webhook source rejected: %v", err)
	}
	// Built-in is excluded from the registry connector set (policy/resolve skip it).
	if got := d.Connectors(); len(got) != 1 || got[0] != "http" {
		t.Fatalf("connectors = %v, want [http] (no @webhook)", got)
	}

	bad := []string{
		`{"name":"h","source":{"connector":"http","action":"get"},"sink":{"connector":"@webhook","action":"ndjson"}}`, // builtin as sink
		`{"name":"h","source":{"connector":"@other","action":"x"},"sink":{"connector":"http","action":"post"}}`,       // unknown builtin
		`{"name":"h","start":"in","steps":[
		  {"id":"in","type":"sink","connector":"@webhook","action":"ndjson","onComplete":"out"},
		  {"id":"out","type":"sink","connector":"http","action":"post"}]}`, // @webhook on a non-source step
	}
	for i, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("bad @webhook doc %d accepted", i)
		}
	}
}

func TestConnectors(t *testing.T) {
	// Graph form: source, sink, and a handler — deduped + sorted.
	d, err := Parse([]byte(goodGraph))
	if err != nil {
		t.Fatal(err)
	}
	got := d.Connectors()
	if len(got) != 1 || got[0] != "http" { // all three steps use "http"
		t.Fatalf("graph connectors = %v, want [http]", got)
	}

	// Linear form: source + sink.
	d2, err := Parse([]byte(`{"name":"x",
	  "source":{"connector":"sftp","action":"get"},
	  "sink":{"connector":"http","action":"post"}}`))
	if err != nil {
		t.Fatal(err)
	}
	got = d2.Connectors()
	if len(got) != 2 || got[0] != "http" || got[1] != "sftp" {
		t.Fatalf("linear connectors = %v, want [http sftp]", got)
	}
}

func TestGraphSecretRefsAcrossSteps(t *testing.T) {
	d, err := Parse([]byte(`{"name":"x","steps":[
	  {"id":"s","type":"source","connector":"a","action":"b","config":{"token":{"$secret":"src_key"}},"onComplete":"k","onFailure":"h"},
	  {"id":"k","type":"sink","connector":"c","action":"d","config":{"token":{"$secret":"dst_key"}}},
	  {"id":"h","type":"sink","connector":"c","action":"d","config":{"token":{"$secret":"dlq_key"}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := d.SecretRefs()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"dlq_key", "dst_key", "src_key"} // sorted, includes the handler
	if len(refs) != len(want) {
		t.Fatalf("refs = %v, want %v", refs, want)
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Fatalf("refs = %v, want %v", refs, want)
		}
	}
	out, err := d.ResolveSecrets(func(n string) (string, error) { return "V-" + n, nil })
	if err != nil {
		t.Fatal(err)
	}
	// Copy semantics: the receiver's steps are untouched.
	if string(d.Steps[0].Config) == string(out.Steps[0].Config) {
		t.Fatal("ResolveSecrets mutated the original document")
	}
}

// TestStepIDCharset pins the identifier charset for step ids (M6 review
// hardening): a tight charset keeps ids safe as edge targets and in the
// studio DOM. Valid ids parse+plan; ids with quotes/pipes/angle-brackets/
// spaces are rejected.
func TestStepIDCharset(t *testing.T) {
	docFor := func(id string) string {
		b, _ := json.Marshal(id)
		return `{"name":"f","start":` + string(b) + `,"steps":[` +
			`{"id":` + string(b) + `,"type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onComplete":"snk"},` +
			`{"id":"snk","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}}]}`
	}
	for _, id := range []string{"in", "step-1", "step_2", "a.b.c", "S", "x9"} {
		d, err := Parse([]byte(docFor(id)))
		if err != nil {
			t.Errorf("valid id %q: parse: %v", id, err)
			continue
		}
		if _, err := d.Plan(); err != nil {
			t.Errorf("valid id %q rejected: %v", id, err)
		}
	}
	for _, id := range []string{"a'b", "a|b", "a<b", "a b", "a\"b", "-lead", ".lead", "a`b"} {
		d, err := Parse([]byte(docFor(id)))
		if err != nil {
			continue // rejected at parse is also fine
		}
		if _, err := d.Plan(); err == nil {
			t.Errorf("invalid id %q accepted", id)
		}
	}
}
