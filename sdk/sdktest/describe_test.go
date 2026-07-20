package sdktest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/sdktest"
)

func TestDescribe(t *testing.T) {
	schema := []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`)
	c := sdk.Connector{
		Name:    "demo",
		Version: "9.9.9",
		Sources: map[string]func() sdk.SourceAction{
			"read": func() sdk.SourceAction { return &countSource{n: 1} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"write": func() sdk.SinkAction { return &collectSink{} },
			"log":   func() sdk.SinkAction { return &collectSink{} },
		},
		Schemas: map[string][]byte{"read": schema},
	}
	p := sdktest.Serve(t, c)

	d, err := p.Describe(context.Background())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if d.Name != "demo" || d.Version != "9.9.9" {
		t.Fatalf("identity = %q/%q", d.Name, d.Version)
	}
	// Actions come back canonical-ready; check via CanonicalDescriptor.
	if len(d.Actions) != 3 {
		t.Fatalf("actions = %d, want 3", len(d.Actions))
	}
	got := map[string]sdk.ActionDescriptor{}
	for _, a := range d.Actions {
		got[a.Direction+"/"+a.Action] = a
	}
	if a, ok := got["source/read"]; !ok || string(a.ConfigSchema) != string(schema) {
		t.Errorf("source/read schema = %q, want %q (present=%v)", a.ConfigSchema, schema, ok)
	}
	if a, ok := got["sink/write"]; !ok || len(a.ConfigSchema) != 0 {
		t.Errorf("sink/write should have no schema, got %q (present=%v)", a.ConfigSchema, ok)
	}
	if _, ok := got["sink/log"]; !ok {
		t.Errorf("sink/log missing")
	}

	// CanonicalDescriptor is deterministic and sorted by (direction, action).
	b1, err := sdk.CanonicalDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := sdk.CanonicalDescriptor(d)
	if string(b1) != string(b2) {
		t.Errorf("CanonicalDescriptor not deterministic")
	}
	var parsed sdk.Descriptor
	if err := json.Unmarshal(b1, &parsed); err != nil {
		t.Fatalf("canonical bytes not valid JSON: %v", err)
	}
	order := []string{"log", "write", "read"} // by (direction, action): sinks sorted, then source
	for i, a := range parsed.Actions {
		if a.Action != order[i] {
			t.Errorf("action[%d] = %q, want %q (sort order)", i, a.Action, order[i])
		}
	}
}
