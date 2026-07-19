package flowdoc

import "testing"

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
		  {"id":"w","type":"wasm","onComplete":"k"},
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
