package flowdoc_test

import (
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

func parseDoc(tb testing.TB, raw string) *flowdoc.Document {
	tb.Helper()
	d, err := flowdoc.Parse([]byte(raw))
	if err != nil {
		tb.Fatalf("fixture does not validate: %v", err)
	}
	return d
}

func codes(ns []flowdoc.Notice) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Code
	}
	return out
}

func find(ns []flowdoc.Notice, code string) (flowdoc.Notice, bool) {
	for _, n := range ns {
		if n.Code == code {
			return n, true
		}
	}
	return flowdoc.Notice{}, false
}

// The notice the whole thing exists for: nothing in a document SAYS "this is
// asynchronous" — it is the absence of a node, which is exactly the kind of
// fact a canvas hides.
func TestAWebhookFlowWithNoResponseIsReportedAsAsynchronous(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard"}
	}`)
	ns := flowdoc.Review(d)
	n, ok := find(ns, "async-response.asynchronous")
	if !ok {
		t.Fatalf("no asynchronous notice; got %v", codes(ns))
	}
	if !strings.Contains(n.Detail, "202") {
		t.Errorf("detail %q does not tell the author what the caller receives", n.Detail)
	}
	if n.Step != flowdoc.WebhookSource {
		t.Errorf("step = %q; the notice must name a node so the canvas can badge it", n.Step)
	}
}

func TestAWebhookFlowEndingAtResponseIsReportedAsSynchronous(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@response"}
	}`)
	if _, ok := find(flowdoc.Review(d), "async-response.synchronous"); !ok {
		t.Errorf("no synchronous notice; got %v", codes(flowdoc.Review(d)))
	}
}

// A scheduled flow has no caller, so neither notice is a fact about it. A
// review that fires on every flow is one nobody reads.
func TestAFlowWithNoCallerGetsNoRequestNotices(t *testing.T) {
	d := parseDoc(t, `{
		"name": "nightly",
		"source": {"connector": "gen", "action": "records"},
		"sink": {"connector": "@discard"}
	}`)
	for _, c := range codes(flowdoc.Review(d)) {
		if strings.HasPrefix(c, "async-response") || strings.HasPrefix(c, "unverified-input") {
			t.Errorf("notice %q fired on a flow with no caller", c)
		}
	}
}

func TestAnUnverifiedWebhookIsAWarning(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard"}
	}`)
	n, ok := find(flowdoc.Review(d), "unverified-input.no-schema")
	if !ok {
		t.Fatal("an endpoint that accepts anything and answers 202 produced no notice")
	}
	if n.Severity != flowdoc.SeverityWarn {
		t.Errorf("severity = %q, want warn: the caller gets no feedback at all in this shape", n.Severity)
	}
}

// The same missing schema on a SYNCHRONOUS flow is a smaller problem — the
// failure still reaches the caller — and the severity has to reflect that or
// the warning stops meaning anything.
func TestAnUnverifiedSynchronousWebhookIsOnlyInformational(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@response"}
	}`)
	n, ok := find(flowdoc.Review(d), "unverified-input.no-schema")
	if !ok {
		t.Fatal("no notice")
	}
	if n.Severity != flowdoc.SeverityInfo {
		t.Errorf("severity = %q, want info", n.Severity)
	}
}

func TestAVerifiedWebhookGetsNoInputWarning(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "body",
			"schema": {"type": "object", "required": ["id"]}
		}},
		"sink": {"connector": "@discard"}
	}`)
	if _, ok := find(flowdoc.Review(d), "unverified-input.no-schema"); ok {
		t.Error("a flow that does verify its input was told it does not")
	}
}

// "records" scope reads as "verified" on a canvas. It is a weaker promise, and
// the review is where that gets said.
func TestRecordsScopeNamesItsWeakerGuarantee(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "records",
			"schema": {"type": "object", "required": ["id"]}
		}},
		"sink": {"connector": "@discard"}
	}`)
	if _, ok := find(flowdoc.Review(d), "input-scope.records"); !ok {
		t.Errorf("no scope notice; got %v", codes(flowdoc.Review(d)))
	}
}

func TestBodyScopeGetsNoScopeNotice(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "body",
			"schema": {"type": "object"}
		}},
		"sink": {"connector": "@discard"}
	}`)
	if _, ok := find(flowdoc.Review(d), "input-scope.records"); ok {
		t.Error("full-body verification was reported as partial")
	}
}

// A synchronous flow with an aggregate cannot answer until it has read
// everything — the case where a sync route meets the delivery timeout.
func TestASynchronousAggregateWarnsAboutHoldingTheCaller(t *testing.T) {
	d := parseDoc(t, `{
		"name": "totals",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"ops": [{"type": "aggregate", "key": "$.region", "aggs": [{"op": "count", "out": "n"}]}],
		"sink": {"connector": "@response"}
	}`)
	n, ok := find(flowdoc.Review(d), "sync-blocking.aggregate")
	if !ok {
		t.Fatalf("no blocking notice; got %v", codes(flowdoc.Review(d)))
	}
	if n.Severity != flowdoc.SeverityWarn {
		t.Errorf("severity = %q, want warn", n.Severity)
	}
}

// The same aggregate asynchronously is unremarkable — nobody is waiting.
func TestAnAsynchronousAggregateIsNotABlockingWarning(t *testing.T) {
	d := parseDoc(t, `{
		"name": "totals",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"ops": [{"type": "aggregate", "key": "$.region", "aggs": [{"op": "count", "out": "n"}]}],
		"sink": {"connector": "@discard"}
	}`)
	for _, c := range codes(flowdoc.Review(d)) {
		if strings.HasPrefix(c, "sync-blocking") {
			t.Errorf("notice %q fired on a flow with nobody waiting", c)
		}
	}
}

// Warnings first, then codes: decided once, here, so the studio and any other
// client render the same order.
func TestNoticesAreOrderedWarningsFirst(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard"}
	}`)
	ns := flowdoc.Review(d)
	if len(ns) < 2 {
		t.Fatalf("expected several notices, got %v", codes(ns))
	}
	seenInfo := false
	for _, n := range ns {
		if n.Severity == flowdoc.SeverityInfo {
			seenInfo = true
		} else if seenInfo {
			t.Fatalf("warning %q came after an info notice: %v", n.Code, codes(ns))
		}
	}
}

// Review is advisory. If it ever grows a way to refuse a document, the
// distinction between validation and advice has collapsed and this test is
// where that shows up.
func TestReviewNeverRefusesAValidDocument(t *testing.T) {
	raw := `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@discard"}
	}`
	d := parseDoc(t, raw)
	if len(flowdoc.Review(d)) == 0 {
		t.Fatal("fixture produced no notices, so this asserts nothing")
	}
	if _, err := flowdoc.Parse([]byte(raw)); err != nil {
		t.Fatalf("a document that reviews with warnings must still deploy: %v", err)
	}
}

func TestReviewOfNilIsEmpty(t *testing.T) {
	if ns := flowdoc.Review(nil); ns != nil {
		t.Errorf("Review(nil) = %v; an advisory pass must not be what takes the hub down", ns)
	}
}

func TestReviewRawIgnoresAnUnparseableDocument(t *testing.T) {
	if ns := flowdoc.ReviewRaw([]byte(`{"name":`)); ns != nil {
		t.Errorf("ReviewRaw reported %v on a broken document; validation reports that, not review", codes(ns))
	}
}

// Every registered check must be findable and described, because the studio
// shows a notice's provenance and there is no compiler between the two.
func TestEveryCheckIsDescribedAndOrdered(t *testing.T) {
	cs := flowdoc.Checks()
	if len(cs) < 4 {
		t.Fatalf("expected the built-in checks, got %d", len(cs))
	}
	for i, c := range cs {
		if c.Code == "" || c.Summary == "" || c.Fn == nil {
			t.Errorf("check %d is incomplete: %+v", i, c)
		}
		if i > 0 && cs[i-1].Code >= c.Code {
			t.Errorf("checks are not ordered by code: %q then %q", cs[i-1].Code, c.Code)
		}
	}
}

// A notice's code must trace back to the check that produced it, or codes are
// decoration. Re-rooting is how that stays true even for a check that forgets.
func TestEveryNoticeStaysInItsChecksNamespace(t *testing.T) {
	d := parseDoc(t, `{
		"name": "orders",
		"source": {"connector": "@webhook", "action": "ndjson", "input": {
			"scope": "records", "schema": {"type": "object"}
		}},
		"ops": [{"type": "aggregate", "key": "$.region", "aggs": [{"op": "count", "out": "n"}]}],
		"sink": {"connector": "@response"}
	}`)
	cs := flowdoc.Checks()
	for _, n := range flowdoc.Review(d) {
		owned := false
		for _, c := range cs {
			if n.Code == c.Code || strings.HasPrefix(n.Code, c.Code+".") {
				owned = true
				break
			}
		}
		if !owned {
			t.Errorf("notice %q belongs to no registered check", n.Code)
		}
		if n.Severity != flowdoc.SeverityInfo && n.Severity != flowdoc.SeverityWarn {
			t.Errorf("notice %q has severity %q; advisory severities are info and warn", n.Code, n.Severity)
		}
		if n.Title == "" {
			t.Errorf("notice %q has no title", n.Code)
		}
	}
}

// Fire-and-forget is a third shape, and the review has to say which of the
// three was deployed — "asynchronous" would be true but would promise a status
// URL that does not exist (ADR-0042 §3d).
func TestFireAndForgetIsReportedAsItsOwnShape(t *testing.T) {
	d := parseDoc(t, `{
		"name": "events",
		"source": {"connector": "@webhook", "action": "ndjson", "ack": "none",
			"input": {"scope": "body", "schema": {"type": "object"}}},
		"sink": {"connector": "@discard"}
	}`)
	ns := flowdoc.Review(d)
	n, ok := find(ns, "async-response.fire-and-forget")
	if !ok {
		t.Fatalf("no fire-and-forget notice; got %v", codes(ns))
	}
	if !strings.Contains(n.Detail, "no status URL") {
		t.Errorf("detail %q does not say what the caller will not get", n.Detail)
	}
	if _, wrong := find(ns, "async-response.asynchronous"); wrong {
		t.Error("a fire-and-forget flow was also promised a status URL")
	}
}

// Unverified AND untrackable is the one shape with no feedback path at all.
// The warning has to name that, not repeat the generic dead-letter line.
func TestUnverifiedFireAndForgetNamesTheSilence(t *testing.T) {
	d := parseDoc(t, `{
		"name": "events",
		"source": {"connector": "@webhook", "action": "ndjson", "ack": "none"},
		"sink": {"connector": "@discard"}
	}`)
	n, ok := find(flowdoc.Review(d), "unverified-input.no-schema")
	if !ok {
		t.Fatal("no notice on an endpoint that checks nothing and reports nothing")
	}
	if n.Severity != flowdoc.SeverityWarn {
		t.Errorf("severity = %q, want warn", n.Severity)
	}
	if !strings.Contains(n.Detail, "fire-and-forget") {
		t.Errorf("detail %q does not name the combination that makes this silent", n.Detail)
	}
}
