package flowdoc_test

import (
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// An input schema is part of the flow document, so an unenforceable one must
// fail at AUTHORING — a 422 in the studio while somebody is looking — rather
// than at request time, when the only person who finds out is a caller reading
// a 500.
func TestInputSchemaIsCompiledAtValidation(t *testing.T) {
	doc := `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "body",
			"schema": {"type": "object", "require": ["order_id"]}
		}},
		"sink": {"connector": "@discard"}
	}`
	_, err := flowdoc.Parse([]byte(doc))
	if err == nil {
		t.Fatal("accepted a flow whose input schema has a misspelt keyword; it would have verified nothing")
	}
	if !strings.Contains(err.Error(), "require") {
		t.Errorf("error %q does not name the offending keyword", err)
	}
}

func TestValidInputIsAccepted(t *testing.T) {
	doc := `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "records",
			"schema": {"type": "object", "required": ["order_id"],
				"properties": {"order_id": {"type": "string"}}}
		}},
		"sink": {"connector": "@discard"}
	}`
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	in, ok := d.InputSpec()
	if !ok {
		t.Fatal("InputSpec did not find the declared input")
	}
	if in.EffectiveScope() != flowdoc.ScopeRecords {
		t.Errorf("scope = %q, want %q", in.EffectiveScope(), flowdoc.ScopeRecords)
	}
	if in.Limit() != flowdoc.DefaultMaxValidateBytes {
		t.Errorf("limit = %d, want the default %d", in.Limit(), flowdoc.DefaultMaxValidateBytes)
	}
	s, err := in.Compile()
	if err != nil || s == nil {
		t.Fatalf("Compile() = %v, %v; want a usable validator", s, err)
	}
}

// Verification belongs where a REQUEST exists to verify. On a pull source
// there is no caller to reject, so an input block there would be a promise
// that silently never runs.
func TestInputIsRejectedWhereThereIsNoRequest(t *testing.T) {
	doc := `{
		"name": "orders",
		"source": {"connector": "http", "action": "get", "input": {
			"schema": {"type": "object"}
		}},
		"sink": {"connector": "@discard"}
	}`
	_, err := flowdoc.Parse([]byte(doc))
	if err == nil {
		t.Fatal("accepted input verification on a pull source")
	}
	if !strings.Contains(err.Error(), "@webhook") {
		t.Errorf("error %q does not explain where input belongs", err)
	}
}

func TestInputOnASinkIsRejected(t *testing.T) {
	doc := `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard", "input": {"schema": {"type": "object"}}}
	}`
	if _, err := flowdoc.Parse([]byte(doc)); err == nil {
		t.Fatal("accepted input verification on a sink")
	}
}

// An input block with no schema looks like verification and performs none —
// the same trap as a silently-ignored keyword, one level up.
func TestInputWithoutASchemaIsRejected(t *testing.T) {
	doc := `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {"scope": "body"}},
		"sink": {"connector": "@discard"}
	}`
	_, err := flowdoc.Parse([]byte(doc))
	if err == nil {
		t.Fatal("accepted an input block with no schema")
	}
	if !strings.Contains(err.Error(), "verifies nothing") {
		t.Errorf("error %q does not say why it is refused", err)
	}
}

func TestInputScopeAndLimitsAreChecked(t *testing.T) {
	for _, tc := range []struct{ name, block, want string }{
		{"an unknown scope", `{"scope":"first-page","schema":{"type":"object"}}`, "scope"},
		{"a negative limit", `{"schema":{"type":"object"},"maxBytes":-1}`, "negative"},
		{"a limit past the ceiling", `{"schema":{"type":"object"},"maxBytes":1073741824}`, "ceiling"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := `{"name":"orders",
				"source":{"connector":"@webhook","action":"ndjson","input":` + tc.block + `},
				"sink":{"connector":"@discard"}}`
			_, err := flowdoc.Parse([]byte(doc))
			if err == nil {
				t.Fatalf("accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// A DAG may have several @webhook sources reading the same inbound body
// (ADR-0029 fan-in). Two input blocks would be two answers to "is this request
// acceptable", and the accept path has exactly one response to give.
func TestTwoInputBlocksAreRejected(t *testing.T) {
	doc := `{
		"name": "orders",
		"start": "in-a",
		"steps": [
			{"id":"in-a","type":"source","connector":"@webhook","action":"ndjson",
			 "input":{"schema":{"type":"object"}},"onSuccess":"merge"},
			{"id":"in-b","type":"source","connector":"@webhook","action":"ndjson",
			 "input":{"schema":{"type":"object"}},"onSuccess":"merge"},
			{"id":"merge","type":"merge","mode":"concat","inputs":["in-a","in-b"],"onSuccess":"out"},
			{"id":"out","type":"sink","connector":"@discard"}
		]
	}`
	_, err := flowdoc.Parse([]byte(doc))
	if err == nil {
		t.Fatal("accepted two input blocks in one flow")
	}
	if !strings.Contains(err.Error(), "one accept decision") {
		t.Errorf("error %q does not explain the conflict", err)
	}
}

// The graph form must reach the same place as the linear form: one model, one
// set of rules (ADR-0013).
func TestInputOnAGraphStep(t *testing.T) {
	doc := `{
		"name": "orders",
		"start": "in",
		"steps": [
			{"id":"in","type":"source","connector":"@webhook","action":"ndjson",
			 "input":{"scope":"body","schema":{"type":"object","required":["id"]}},
			 "onSuccess":"out"},
			{"id":"out","type":"sink","connector":"@discard"}
		]
	}`
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.InputSpec(); !ok {
		t.Fatal("InputSpec did not find the input declared on a graph step")
	}
	// The runner binds a graph step through Endpoint(); losing Input there
	// would drop verification silently on exactly the documents that use it.
	if d.Steps[0].Endpoint().Input == nil {
		t.Error("Step.Endpoint() dropped the input block")
	}
}

// A document with no input block is the norm and must stay unaffected.
func TestNoInputIsFine(t *testing.T) {
	doc := `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.InputSpec(); ok {
		t.Error("InputSpec reported an input on a document that declares none")
	}
	var none *flowdoc.Input
	if s, err := none.Compile(); s != nil || err != nil {
		t.Errorf("nil.Compile() = %v, %v; want no validator and no error", s, err)
	}
	if none.EffectiveScope() != flowdoc.ScopeBody {
		t.Errorf("nil scope = %q, want the %q default", none.EffectiveScope(), flowdoc.ScopeBody)
	}
}
