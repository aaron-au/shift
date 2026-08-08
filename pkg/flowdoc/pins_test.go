package flowdoc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// A draft means "newest" and a published flow means "the build I was published
// against" (ADR-0047 §1). Pinning is what turns the first into the second.
func TestPinningFillsInOnlyWhatIsUnpinned(t *testing.T) {
	raw := `{
	  "name": "orders",
	  "steps": [
	    {"id":"in","type":"source","connector":"sftp","action":"get","onSuccess":"out"},
	    {"id":"out","type":"sink","connector":"http","action":"post","version":"0.9.0"}
	  ],
	  "start": "in"
	}`
	doc, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	asked := map[string]int{}
	if err := doc.PinConnectors(func(name string) (string, error) {
		asked[name]++
		return "1.2.3", nil
	}); err != nil {
		t.Fatalf("pin: %v", err)
	}

	// The already-pinned step is not re-resolved. Republishing an unchanged
	// document must be a no-op, not a silent upgrade — and a rollback has to
	// keep the builds the older version was published against.
	if asked["http"] != 0 {
		t.Fatal("an already-pinned step was resolved again; republishing would silently upgrade it")
	}
	if asked["sftp"] != 1 {
		t.Fatalf("sftp resolved %d times, want 1", asked["sftp"])
	}

	pins := map[string]string{}
	for _, p := range doc.ConnectorPins() {
		pins[p.Connector] = p.Version
	}
	if pins["sftp"] != "1.2.3" || pins["http"] != "0.9.0" {
		t.Fatalf("pins = %v", pins)
	}

	// The pin has to survive the round trip through storage, or the published
	// document goes back to meaning "newest" the moment it is read.
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Parse(out)
	if err != nil {
		t.Fatalf("reparse a pinned document: %v", err)
	}
	for _, p := range back.ConnectorPins() {
		if p.Version != pins[p.Connector] {
			t.Fatalf("pin for %s did not round-trip: %q", p.Connector, p.Version)
		}
	}
}

// The linear form is sugar over the same model, so it pins the same way.
func TestPinningTheLinearForm(t *testing.T) {
	doc, err := Parse([]byte(`{
	  "name": "f",
	  "source": {"connector":"sftp","action":"get"},
	  "sink": {"connector":"@discard","action":""}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.PinConnectors(func(string) (string, error) { return "2.0.0", nil }); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if doc.Source.Version != "2.0.0" {
		t.Fatalf("source version = %q", doc.Source.Version)
	}
	// A built-in is compiled into the runner: there is no artifact to pin, so
	// it must not acquire a version that would then be resolved against a
	// registry that has never heard of it.
	if doc.Sink.Version != "" {
		t.Fatalf("built-in sink was pinned to %q", doc.Sink.Version)
	}
	pins := doc.ConnectorPins()
	if len(pins) != 1 || pins[0].Connector != "sftp" {
		t.Fatalf("pins = %+v, want the registry connector only", pins)
	}
}

// A resolver failure aborts the whole document. A half-pinned flow would run a
// mix of recorded and newest builds, which is the ambiguity pinning exists to
// remove.
func TestAFailedResolveLeavesNothingPinned(t *testing.T) {
	doc, err := Parse([]byte(`{
	  "name": "f",
	  "source": {"connector":"sftp","action":"get"},
	  "sink": {"connector":"http","action":"post"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("no such connector")
	if err := doc.PinConnectors(func(string) (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the resolver's error", err)
	}
	if doc.Source.Version != "" || doc.Sink.Version != "" {
		t.Fatal("a failed resolve left part of the document pinned")
	}
}

// A resolver that answers with something unusable is caught here rather than
// at the runner, where it would be a task failure against live data.
func TestAnUnusableResolvedVersionIsRejected(t *testing.T) {
	doc, err := Parse([]byte(`{
	  "name": "f",
	  "source": {"connector":"sftp","action":"get"},
	  "sink": {"connector":"@discard","action":""}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"../../etc/passwd", "1.0 0", strings.Repeat("9", 100)} {
		if err := doc.PinConnectors(func(string) (string, error) { return bad, nil }); err == nil {
			t.Fatalf("resolved version %q was accepted", bad)
		}
	}

	// An EMPTY answer is different: it means the registry has nothing to pin,
	// which is legitimate in a deployment that provisions connector binaries
	// locally. The step stays unpinned and the connector-pin check reports it,
	// rather than the hub deciding a registry is mandatory.
	if err := doc.PinConnectors(func(string) (string, error) { return "", nil }); err != nil {
		t.Fatalf("an empty resolve should leave the step unpinned, not fail: %v", err)
	}
	if doc.Source.Version != "" {
		t.Fatalf("source pinned to %q from an empty resolve", doc.Source.Version)
	}
	if notices := Review(doc); !func() bool {
		for _, n := range notices {
			if n.Code == "connector-pin.unpinned" {
				return true
			}
		}
		return false
	}() {
		t.Fatalf("an unpinned connector step raised no notice: %+v", notices)
	}
}

// A version is only meaningful on a registry connector, and only in a shape
// safe to put in a path and a query string.
func TestVersionValidation(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		ok   bool
	}{
		{"pinned graph step", `{"name":"f","steps":[{"id":"a","type":"source","connector":"sftp","action":"get","version":"1.0.0","onSuccess":"b"},{"id":"b","type":"sink","connector":"@discard"}],"start":"a"}`, true},
		{"pinned linear endpoint", `{"name":"f","source":{"connector":"sftp","action":"get","version":"1.0.0"},"sink":{"connector":"@discard","action":""}}`, true},
		{"version on a built-in step", `{"name":"f","steps":[{"id":"a","type":"source","connector":"@webhook","action":"ndjson","version":"1.0.0","onSuccess":"b"},{"id":"b","type":"sink","connector":"@discard"}],"start":"a"}`, false},
		{"version on a built-in endpoint", `{"name":"f","source":{"connector":"@webhook","action":"ndjson","version":"1"},"sink":{"connector":"@discard","action":""}}`, false},
		{"path traversal", `{"name":"f","source":{"connector":"sftp","action":"get","version":"../evil"},"sink":{"connector":"@discard","action":""}}`, false},
		{"leading dot", `{"name":"f","source":{"connector":"sftp","action":"get","version":".hidden"},"sink":{"connector":"@discard","action":""}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if tc.ok && err != nil {
				t.Fatalf("rejected: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// RepinConnector is the mechanical half of a bulk upgrade (ADR-0047 §9), and
// its whole job is the thing PinConnectors deliberately will not do:
// overwrite an existing pin. What it must NOT do is touch anything else.
func TestRepinMovesOneConnectorAndLeavesTheRestAlone(t *testing.T) {
	both := `{
	  "name": "sync",
	  "steps": [
	    {"id":"in","type":"source","connector":"sftp","action":"get","version":"1.0.0","onSuccess":"out"},
	    {"id":"out","type":"sink","connector":"sftp","action":"put","version":"1.0.0"}
	  ],
	  "start": "in"
	}`
	doc, err := Parse([]byte(both))
	if err != nil {
		t.Fatal(err)
	}
	moved := doc.RepinConnector("sftp", "2.0.0")
	if len(moved) != 2 {
		t.Fatalf("moved %v, want BOTH sftp steps — a flow half-upgraded runs two builds at once", moved)
	}
	for _, p := range doc.ConnectorPins() {
		if p.Version != "2.0.0" {
			t.Fatalf("step %q left at %q", p.StepID, p.Version)
		}
	}

	mixed := `{
	  "name": "orders",
	  "steps": [
	    {"id":"in","type":"source","connector":"sftp","action":"get","version":"1.0.0","onSuccess":"out"},
	    {"id":"out","type":"sink","connector":"http","action":"post","version":"0.9.0"}
	  ],
	  "start": "in"
	}`
	doc, err = Parse([]byte(mixed))
	if err != nil {
		t.Fatal(err)
	}
	if moved := doc.RepinConnector("sftp", "2.0.0"); len(moved) != 1 || moved[0] != "in" {
		t.Fatalf("moved %v, want only the sftp step", moved)
	}
	pins := map[string]string{}
	for _, p := range doc.ConnectorPins() {
		pins[p.StepID] = p.Version
	}
	// A bulk upgrade of one connector that also moved a different connector
	// would be an unannounced change inside an action somebody approved for
	// something else — the exact failure ADR-0047 exists to remove.
	if pins["out"] != "0.9.0" {
		t.Fatalf("the http step moved to %q; a batch must touch only its own connector", pins["out"])
	}
}

// Reporting "nothing moved" is what lets the caller refuse the whole batch
// rather than stage a flow that was never going to change.
func TestRepinReportsWhenThereIsNothingToMove(t *testing.T) {
	raw := `{
	  "name": "orders",
	  "source": {"connector":"sftp","action":"get","version":"2.0.0"},
	  "sink": {"connector":"@discard","action":""}
	}`
	doc, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if moved := doc.RepinConnector("sftp", "2.0.0"); len(moved) != 0 {
		t.Fatalf("moved %v: a step already at the target has not moved", moved)
	}
	if moved := doc.RepinConnector("http", "2.0.0"); len(moved) != 0 {
		t.Fatalf("moved %v: no step uses that connector", moved)
	}
	// The linear form is the same model, so it repins the same way.
	if moved := doc.RepinConnector("sftp", "3.0.0"); len(moved) != 1 || moved[0] != "source" {
		t.Fatalf("moved %v, want the linear source", moved)
	}
	if doc.Source.Version != "3.0.0" {
		t.Fatalf("linear source pin = %q", doc.Source.Version)
	}
}

// A version the caller never validated must not reach a stored document: an
// unusable pin resolves to nothing at dispatch, which is a flow that fails at
// its first task. Built-ins have no artifact to pin at all.
func TestRepinRefusesWhatCannotBeAPin(t *testing.T) {
	raw := `{
	  "name": "orders",
	  "source": {"connector":"sftp","action":"get","version":"1.0.0"},
	  "sink": {"connector":"@discard","action":""}
	}`
	doc, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "not a version", "../../etc/passwd"} {
		if moved := doc.RepinConnector("sftp", bad); len(moved) != 0 {
			t.Fatalf("repin to %q moved %v", bad, moved)
		}
	}
	if doc.Source.Version != "1.0.0" {
		t.Fatalf("a refused repin still rewrote the pin: %q", doc.Source.Version)
	}
	if moved := doc.RepinConnector("@discard", "1.0.0"); len(moved) != 0 {
		t.Fatalf("repinned a built-in: %v", moved)
	}
}
