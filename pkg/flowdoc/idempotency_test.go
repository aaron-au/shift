package flowdoc

import (
	"encoding/json"
	"testing"
)

// TestWithSinkConfigGraph pins that the injected idempotency key reaches the
// SINK STEP config of a v2 graph document (the canonical form), not just the
// linear d.Sink — the at-least-once contract depends on it (ADR-0002/0013).
func TestWithSinkConfigGraph(t *testing.T) {
	src := `{
      "name":"g","start":"in",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onSuccess":"out","onFailure":"dead"},
        {"id":"out","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}},
        {"id":"dead","type":"sink","connector":"http","action":"post","config":{"url":"https://dlq"}}
      ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := d.WithSinkConfig(map[string]any{"idempotency_key": "K123"})
	if err != nil {
		t.Fatalf("WithSinkConfig: %v", err)
	}
	// Every sink step must carry the key; source steps must not.
	sinks := 0
	for _, s := range out.Steps {
		var cfg map[string]any
		if len(s.Config) > 0 {
			_ = json.Unmarshal(s.Config, &cfg)
		}
		if s.Type == "sink" {
			sinks++
			if cfg["idempotency_key"] != "K123" {
				t.Errorf("sink %q missing idempotency_key, got %v", s.ID, cfg)
			}
			if cfg["url"] == nil {
				t.Errorf("sink %q lost its original config", s.ID)
			}
		} else if cfg["idempotency_key"] != nil {
			t.Errorf("non-sink step %q got an idempotency_key", s.ID)
		}
	}
	if sinks != 2 {
		t.Fatalf("expected 2 sink steps, saw %d", sinks)
	}
	// Original document must be untouched (copy-on-write).
	for _, s := range d.Steps {
		if s.Type == "sink" {
			var cfg map[string]any
			_ = json.Unmarshal(s.Config, &cfg)
			if cfg["idempotency_key"] != nil {
				t.Errorf("original doc mutated on step %q", s.ID)
			}
		}
	}
}

// TestCountAggMalformedPathRejected pins that a count aggregate with a
// malformed path is rejected at validation — the compiler would otherwise
// reach the panicking MustParsePath and crash the runner.
func TestCountAggMalformedPathRejected(t *testing.T) {
	src := `{"name":"g","start":"in","steps":[
      {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onComplete":"agg"},
      {"id":"agg","type":"aggregate","key":"$.k","aggs":[{"op":"count","out":"n","path":"$.["}],"onComplete":"out"},
      {"id":"out","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}}
    ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		return // rejected at parse is also acceptable
	}
	if _, err := d.Plan(); err == nil {
		t.Fatal("count agg with malformed path was accepted; expected rejection")
	}
}
