package gwclient

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// The point of ADR-0042 §4, in one test: a caller who sends the wrong shape
// learns WHICH FIELD is wrong, synchronously, instead of getting a cheerful
// acknowledgement and a dead letter somebody reads tomorrow.
func TestABadRequestIsRejectedWithTheOffendingField(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "body",
			"schema": {"type":"object","required":["order_id","qty"],
				"properties":{"order_id":{"type":"string"},"qty":{"type":"integer","minimum":1}}}
		}},
		"sink": {"connector": "@discard"}
	}`)

	res := l.verify(doc, []byte(`{"qty":"three"}`))
	if res.ok() {
		t.Fatal("accepted a request that violates the schema")
	}
	paths := map[string]string{}
	for _, v := range res.Violations {
		paths[v.Path] = v.Message
	}
	if _, ok := paths["/order_id"]; !ok {
		t.Errorf("no violation for the missing order_id: %v", res.Violations)
	}
	if msg, ok := paths["/qty"]; !ok {
		t.Errorf("no violation for the wrong qty type: %v", res.Violations)
	} else if !strings.Contains(msg, "integer") {
		t.Errorf("qty message %q does not say what was expected", msg)
	}
}

func TestAValidRequestPasses(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"schema": {"type":"object","required":["order_id"],
				"properties":{"order_id":{"type":"string"}}}
		}},
		"sink": {"connector": "@discard"}
	}`)
	if res := l.verify(doc, []byte(`{"order_id":"ORD-1"}`)); !res.ok() {
		t.Fatalf("rejected a valid request: %+v", res)
	}
}

// A flow with no input block must be untouched by any of this — the vast
// majority of flows, and the path that must not gain cost or behaviour.
func TestAFlowWithoutAnInputBlockIsUnaffected(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`)
	if res := l.verify(doc, []byte(`{"anything":true}`)); !res.ok() {
		t.Fatalf("a flow with no input block rejected a request: %+v", res)
	}
	if res := l.verify(doc, []byte(`not even json`)); !res.ok() {
		t.Fatalf("a flow with no input block rejected malformed input it never agreed to check: %+v", res)
	}
}

// scope: body must see the WHOLE document. A top-level array validated as its
// first element would silently accept a batch whose shape the author rejected.
func TestScopeBodySeesTheWholeDocument(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "batch",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "body",
			"schema": {"type":"array","minItems":2,"items":{"type":"object","required":["sku"]}}
		}},
		"sink": {"connector": "@discard"}
	}`)

	if res := l.verify(doc, []byte(`[{"sku":"A"},{"sku":"B"}]`)); !res.ok() {
		t.Fatalf("rejected a valid array body: %+v", res)
	}
	res := l.verify(doc, []byte(`[{"sku":"A"}]`))
	if res.ok() {
		t.Fatal("accepted an array shorter than minItems — the array was not seen as an array")
	}
	// Pretty-printed input is normal from a hand-written client and must parse.
	if res := l.verify(doc, []byte("[\n  {\"sku\": \"A\"},\n  {\"sku\": \"B\"}\n]\n")); !res.ok() {
		t.Fatalf("rejected a pretty-printed body: %+v", res)
	}
}

// scope: records validates the FIRST record and lets the rest stream — the
// weaker guarantee ADR-0042 §4b describes, and the one that suits a stream.
func TestScopeRecordsChecksTheFirstRecord(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "stream",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "records",
			"schema": {"type":"object","required":["sku"],"properties":{"sku":{"type":"string"}}}
		}},
		"sink": {"connector": "@discard"}
	}`)

	if res := l.verify(doc, []byte("{\"sku\":\"A\"}\n{\"sku\":\"B\"}\n")); !res.ok() {
		t.Fatalf("rejected a valid NDJSON stream: %+v", res)
	}
	if res := l.verify(doc, []byte("{\"nope\":1}\n{\"sku\":\"B\"}\n")); res.ok() {
		t.Fatal("accepted a stream whose first record violates the schema")
	}
	// The documented limit, asserted rather than left implied: a bad record
	// LATER in the stream is not caught here, and becomes the flow's error path.
	if res := l.verify(doc, []byte("{\"sku\":\"A\"}\n{\"nope\":1}\n")); !res.ok() {
		t.Fatalf("scope: records rejected a later bad record; it verifies the first only: %+v", res)
	}
	// An empty stream has no record to be wrong.
	if res := l.verify(doc, []byte("")); !res.ok() {
		t.Fatalf("rejected an empty stream: %+v", res)
	}
}

// A body past the limit is 413, not 400: the request may well be valid, we
// simply will not buffer it to find out.
func TestAnOversizeBodyIsRefusedRatherThanBuffered(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"schema": {"type":"object"}, "maxBytes": 64
		}},
		"sink": {"connector": "@discard"}
	}`)
	res := l.verify(doc, []byte(`{"pad":"`+strings.Repeat("x", 200)+`"}`))
	if !res.TooLarge {
		t.Fatalf("a 200-byte body passed a 64-byte verification limit: %+v", res)
	}
	if res.Limit != 64 {
		t.Errorf("limit reported as %d, want 64", res.Limit)
	}
}

// Malformed JSON is a 400 with no per-field detail, because there are no
// fields to point at.
func TestMalformedJSONIsReportedWithoutDetails(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {"schema": {"type":"object"}}},
		"sink": {"connector": "@discard"}
	}`)
	res := l.verify(doc, []byte(`{"order_id": `))
	if res.Err == nil {
		t.Fatal("accepted a truncated document")
	}
	if len(res.Violations) != 0 {
		t.Errorf("reported field violations for input that could not be parsed: %v", res.Violations)
	}
}

// The compiled schema is cached, because compiling per request would parse
// JSON and rebuild a tree on the hot path — the cost this design exists to
// avoid.
func TestCompiledSchemasAreCached(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {"schema": {"type":"object"}}},
		"sink": {"connector": "@discard"}
	}`)
	in, _ := doc.InputSpec()

	first, err := l.schemas.get(in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := l.schemas.get(in)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Error("the schema was recompiled on the second request")
	}
}

// The error envelope is what the caller actually reads, so its shape is part
// of the contract (ADR-0023).
func TestProblemEnvelopeShape(t *testing.T) {
	l := &Loop{log: discardLog()}
	doc := mustParse(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"schema": {"type":"object","required":["order_id"]}}},
		"sink": {"connector": "@discard"}
	}`)
	res := l.verify(doc, []byte(`{}`))
	raw := problem(400, "input_invalid", "the request does not satisfy this flow's input schema", res.Violations)

	var got struct {
		Error struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
			Details []struct {
				Path    string `json:"path"`
				Message string `json:"message"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("envelope is not valid JSON: %v (%s)", err, raw)
	}
	if got.Error.Status != 400 || got.Error.Code != "input_invalid" {
		t.Errorf("envelope = %+v, want status 400 and code input_invalid", got.Error)
	}
	if len(got.Error.Details) != 1 || got.Error.Details[0].Path != "/order_id" {
		t.Errorf("details = %+v, want one entry naming /order_id", got.Error.Details)
	}
}

// discardLog silences the loop: these tests deliberately exercise failure
// paths, and each one logs.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func mustParse(t *testing.T, src string) *flowdoc.Document {
	t.Helper()
	d, err := flowdoc.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parsing the flow: %v", err)
	}
	return d
}
