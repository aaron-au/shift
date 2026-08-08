package flowdoc

import (
	"encoding/json"
	"testing"
)

// TestWithSinkConfigGraph pins that the injected idempotency key reaches the
// SINK STEP config of a v2 graph document (the canonical form), not just the
// linear d.Sink — the at-least-once contract depends on it (ADR-0002/0013).
//
// This flow has TWO side-effecting sinks (happy path + dead letter), so each
// gets its own DERIVED key (ADR-0029 §5). It previously asserted the bare key
// on both, which is the bug in issue #61: two sinks writing the same target
// under one key means the second write dedupes away and is silently lost.
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
			if want := "K123:" + s.ID; cfg["idempotency_key"] != want {
				t.Errorf("sink %q key = %v, want %q", s.ID, cfg["idempotency_key"], want)
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

// TestDeliveryPolicy pins the at-least-once/at-most-once flow intent (issue
// #11): validation of the field, the MaxAttempts() mapping, and the
// no-full-parse DeliveryFromDoc extractor.
func TestDeliveryPolicy(t *testing.T) {
	base := `{"name":"f","start":"in","steps":[` +
		`{"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onComplete":"out"},` +
		`{"id":"out","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}}]`
	valid := map[string]int{
		`,"delivery":"at_least_once"}`: DefaultMaxAttempts,
		`,"delivery":"at_most_once"}`:  1,
		`}`:                            DefaultMaxAttempts, // absent → default
	}
	for tail, wantMax := range valid {
		d, err := Parse([]byte(base + tail))
		if err != nil {
			t.Errorf("tail %q: parse: %v", tail, err)
			continue
		}
		if got := d.MaxAttempts(); got != wantMax {
			t.Errorf("tail %q: MaxAttempts = %d, want %d", tail, got, wantMax)
		}
		if got := DeliveryFromDoc([]byte(base + tail)); got != d.Delivery {
			t.Errorf("tail %q: DeliveryFromDoc = %q, want %q", tail, got, d.Delivery)
		}
	}
	if _, err := Parse([]byte(base + `,"delivery":"whenever"}`)); err == nil {
		t.Error("invalid delivery accepted")
	}
}

// Issue #61 / ADR-0029 §5. The hub injects ONE key per task, which is right
// for one sink and wrong the moment two side-effecting sinks write the same
// target: the second write dedupes against the first and is silently lost.
// Nothing fails and nothing is logged — the record is simply not there.
//
// Distinct keys are the whole fix, and they must be DERIVED from the step id
// rather than generated, because at-least-once depends on the same attempt
// producing the same key.
func TestFanOutSinksGetDistinctStableKeys(t *testing.T) {
	src := `{
      "name":"tee","start":"in",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onSuccess":"fan"},
        {"id":"fan","type":"tee","branches":["a","b"]},
        {"id":"a","type":"sink","connector":"http","action":"post","config":{"url":"https://same"}},
        {"id":"b","type":"sink","connector":"http","action":"post","config":{"url":"https://same"}}
      ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	keys := sinkKeys(t, d, "TASK")
	if keys["a"] == keys["b"] {
		t.Fatalf("both branches got %q: two sinks writing one target under one key lose a write", keys["a"])
	}
	if keys["a"] != "TASK:a" || keys["b"] != "TASK:b" {
		t.Fatalf("keys = %v, want <task>:<stepID> (ADR-0029 §5)", keys)
	}

	// Stability is the property at-least-once rests on: a re-dispatched
	// attempt derives the same key from the same step id, so an idempotent
	// receiver still dedupes the retry.
	again := sinkKeys(t, d, "TASK")
	for id, k := range keys {
		if again[id] != k {
			t.Fatalf("step %q derived %q then %q; a redelivery must reuse the key", id, k, again[id])
		}
	}
}

// A single-sink flow keeps the bare task key. That is what ADR-0029 §5
// specifies, and changing the key a sink sees is itself a hazard: a task in
// flight across an upgrade would retry under a different key than its first
// attempt and double-write against a receiver that had already deduped it.
func TestASingleSinkKeepsTheBareTaskKey(t *testing.T) {
	src := `{
      "name":"linear","start":"in",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onSuccess":"out"},
        {"id":"out","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}}
      ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := sinkKeys(t, d, "TASK")["out"]; got != "TASK" {
		t.Fatalf("single sink key = %q, want the bare task key", got)
	}
}

// Built-in terminals have no side effect and no store to dedupe against, so
// they must not tip a one-real-sink flow into derivation. Letting a
// side-effect-free terminal change a real sink's key would be a behaviour
// change for no safety gain.
func TestABuiltinTerminalDoesNotChangeARealSinksKey(t *testing.T) {
	src := `{
      "name":"tee","start":"in",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onSuccess":"fan"},
        {"id":"fan","type":"tee","branches":["real","drop"]},
        {"id":"real","type":"sink","connector":"http","action":"post","config":{"url":"https://y"}},
        {"id":"drop","type":"sink","connector":"@discard","action":""}
      ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := sinkKeys(t, d, "TASK")["real"]; got != "TASK" {
		t.Fatalf("key = %q; a @discard branch must not change a real sink's key", got)
	}
}

// The linear (sugar) form has exactly one sink by construction, so it can
// never fan out — and must keep behaving as it always did.
func TestTheLinearFormIsUnaffected(t *testing.T) {
	src := `{"name":"l",
	  "source":{"connector":"http","action":"get","config":{"url":"https://x"}},
	  "sink":{"connector":"http","action":"post","config":{"url":"https://y"}}}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := d.WithSinkConfig(map[string]any{IdempotencyKeyField: "TASK"})
	if err != nil {
		t.Fatalf("WithSinkConfig: %v", err)
	}
	var cfg map[string]any
	_ = json.Unmarshal(out.Sink.Config, &cfg)
	if cfg[IdempotencyKeyField] != "TASK" {
		t.Fatalf("linear sink key = %v, want the bare task key", cfg[IdempotencyKeyField])
	}
}

// Injection carries fields other than the key; those must reach every sink
// unchanged, and the caller's map must not be mutated by the derivation.
func TestDerivationOnlyRewritesTheKey(t *testing.T) {
	src := `{
      "name":"tee","start":"in",
      "steps":[
        {"id":"in","type":"source","connector":"http","action":"get","config":{"url":"https://x"},"onSuccess":"fan"},
        {"id":"fan","type":"tee","branches":["a","b"]},
        {"id":"a","type":"sink","connector":"http","action":"post","config":{"url":"https://p"}},
        {"id":"b","type":"sink","connector":"http","action":"post","config":{"url":"https://q"}}
      ]}`
	d, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	extra := map[string]any{IdempotencyKeyField: "TASK", "trace": "abc"}
	out, err := d.WithSinkConfig(extra)
	if err != nil {
		t.Fatalf("WithSinkConfig: %v", err)
	}
	for _, s := range out.Steps {
		if s.Type != "sink" {
			continue
		}
		var cfg map[string]any
		_ = json.Unmarshal(s.Config, &cfg)
		if cfg["trace"] != "abc" {
			t.Errorf("sink %q lost a non-key injected field: %v", s.ID, cfg)
		}
		if cfg["url"] == nil {
			t.Errorf("sink %q lost its own config: %v", s.ID, cfg)
		}
	}
	if extra[IdempotencyKeyField] != "TASK" {
		t.Fatalf("the caller's map was mutated: %v", extra)
	}
}

// sinkKeys runs the injection and reports each sink step's resulting key.
func sinkKeys(t *testing.T, d *Document, task string) map[string]string {
	t.Helper()
	out, err := d.WithSinkConfig(map[string]any{IdempotencyKeyField: task})
	if err != nil {
		t.Fatalf("WithSinkConfig: %v", err)
	}
	keys := map[string]string{}
	for _, s := range out.Steps {
		if s.Type != "sink" {
			continue
		}
		var cfg map[string]any
		_ = json.Unmarshal(s.Config, &cfg)
		k, _ := cfg[IdempotencyKeyField].(string)
		keys[s.ID] = k
	}
	return keys
}
