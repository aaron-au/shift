package flowdoc

import (
	"encoding/json"
	"fmt"
)

// Test-only behaviour (ADR-0048 §5, revised 2026-08-07).
//
// ADR-0014 capture tells you what a flow DID, but a test run still drives real
// sources and — more alarmingly — still writes to real sinks. Sampling an SFTP
// `put` does not stop the file landing on the customer's server. A test mode
// that faithfully performs every side effect is not a test mode.
//
// One invariant governs the answer:
//
//	Test-only behaviour is ADDITIVE in test and STRICTLY INERT in production.
//	It may never remove, replace or alter a production step.
//
// So mocking is a PROPERTY OF THE REAL STEP, not a node that stands in for it.
// The first design used substitute nodes (@mock in place of a sink, @inject in
// place of a source) and that violated the invariant it was written to protect:
// the author deleted the connector, and the published document stopped saying
// where data went. It needed a publish-blocking 422 to be safe — a patch over a
// model error.
//
// As step options the connector, its config and its version pin stay in the
// document always. Production is complete BY CONSTRUCTION, so nothing needs
// refusing at publish, and — the practical win — a mock never has to be
// removed before shipping. The flow that runs in production is the same
// document that was tested, with the diversion simply not taken.
//
//	Mock       option on a connector SINK step   — records what would have
//	                                               been written
//	TestInput  option on a connector SOURCE step — emits configured records
//	@probe     a node of its own                 — taps the stream
//
// @probe stays a node because it is the only one that genuinely ADDS: it taps a
// point where nothing was and replaces no production step.
//
// An enabled mock renders a SHIFT-DECISION on the canvas — "test → mock,
// otherwise → connector" — drawn like any other branch so what runs in each
// mode is visible rather than implied. It is not authorable: derived from the
// option, with no editable predicate and no third arm. `running_mode` is
// deliberately NOT developer-routable, because the moment it is, somebody
// writes "test hits the sandbox, production hits the real one" and production
// takes a path nobody ever ran.

// ProbeType is the step TYPE that taps a stream mid-flow. On a v3 DAG this is
// the difference between "the output is wrong" and "the output is wrong after
// the join" — a superset of what uniform capture gives, placed deliberately by
// the author rather than sampled everywhere.
//
// A type rather than a connector because a connector step is a source or a
// sink by construction, and a probe is neither: it sits between two steps and
// passes everything through.
const ProbeType = "probe"

// ProbeConfig is a probe's presentation: what to call the sample it collects.
type ProbeConfig struct {
	// Label names the tap in the capture. Optional — the step id is the
	// fallback — but on a DAG with several probes it is what makes the output
	// readable.
	Label string `json:"label,omitempty"`
}

// Mock is a connector SINK step's test-only diversion (ADR-0048 §5).
//
// Enabled, a TEST execution records what would have been written instead of
// writing it. A deployed execution ignores it entirely and the connector runs.
// Unchecking it is how a developer says "actually drive the real system in
// this test", which is sometimes the point of the test.
type Mock struct {
	Enabled bool `json:"enabled"`
	// Label names the recording in the capture; the step id is the fallback.
	Label string `json:"label,omitempty"`
}

// TestInput is a connector SOURCE step's test-only diversion (ADR-0048 §5).
//
// Enabled, a TEST execution emits Records instead of calling the connector —
// so a flow can be exercised without arranging for a file to appear on the
// customer's SFTP server. A deployed execution ignores it and the connector
// reads as normal.
type TestInput struct {
	Enabled bool `json:"enabled"`
	// Records are emitted verbatim, in order. None is valid and emits nothing:
	// an author who has ticked the box but not filled it in yet is halfway
	// through building a canvas, not shipping.
	Records []json.RawMessage `json:"records,omitempty"`
}

// IsTestOnlyType reports whether a step TYPE is test-only (the probe).
func IsTestOnlyType(stepType string) bool { return stepType == ProbeType }

// TestDiversion is one step whose behaviour differs in a test execution.
type TestDiversion struct {
	StepID string `json:"step"`
	// Kind is "mock", "test-input" or "probe".
	Kind string `json:"kind"`
}

// Test diversion kinds.
const (
	DiversionMock      = "mock"
	DiversionTestInput = "test-input"
	DiversionProbe     = "probe"
)

// TestDiversions lists everything in the document that behaves differently in a
// test execution, in document order.
//
// It is what the studio badges and what an operator reads to answer "what will
// this flow do differently when I run it as a test?" — a question that has to
// be answerable from the document alone, since the answer decides whether a
// run is safe against live systems.
func (d *Document) TestDiversions() []TestDiversion {
	var out []TestDiversion
	add := func(id string, ep Endpoint, stepType string) {
		switch {
		case stepType == ProbeType:
			out = append(out, TestDiversion{StepID: id, Kind: DiversionProbe})
		case ep.Mock != nil && ep.Mock.Enabled:
			out = append(out, TestDiversion{StepID: id, Kind: DiversionMock})
		case ep.TestInput != nil && ep.TestInput.Enabled:
			out = append(out, TestDiversion{StepID: id, Kind: DiversionTestInput})
		}
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			st := &d.Steps[i]
			add(st.ID, st.Endpoint(), st.Type)
		}
		return out
	}
	add("source", d.Source, "source")
	add("sink", d.Sink, "sink")
	return out
}

// validateTestOnly checks a test-only option sits where it can mean something.
//
// Position is the whole contract: a mock stands in for a WRITE and test input
// stands in for a READ. One on the wrong end would be inert in test as well as
// production, which is the single outcome that teaches an author nothing.
//
// Neither is allowed on a built-in. A built-in sink writes nowhere already, and
// mocking @discard is a checkbox that changes nothing.
func validateTestOnly(id string, ep Endpoint, stepType string) error {
	if ep.Mock != nil && ep.Mock.Enabled {
		if stepType != "sink" {
			return fmt.Errorf("step %q: mock stands in for a write, so it belongs on a sink, not a %s", id, stepType)
		}
		if IsBuiltinConnector(ep.Connector) {
			return fmt.Errorf("step %q: %s writes nowhere already, so mocking it changes nothing", id, ep.Connector)
		}
	}
	if ep.TestInput != nil && ep.TestInput.Enabled {
		if stepType != "source" {
			return fmt.Errorf("step %q: test input stands in for a read, so it belongs on a source, not a %s", id, stepType)
		}
		if IsBuiltinConnector(ep.Connector) {
			return fmt.Errorf("step %q: %s already supplies its own records, so test input would be ignored", id, ep.Connector)
		}
		for i, r := range ep.TestInput.Records {
			if !json.Valid(r) {
				return fmt.Errorf("step %q: test input record %d is not valid JSON", id, i)
			}
		}
	}
	return nil
}
