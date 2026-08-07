package flowdoc

import "testing"

// A test-only option is valid only where it can mean something (ADR-0048 §5).
// One on the wrong end of a flow would be inert in test as well as production,
// which is the single outcome that teaches an author nothing.
func TestTestOnlyOptionPositions(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		ok   bool
	}{
		{"mock on a real sink", `{"name":"f","source":{"connector":"@webhook","action":"ndjson"},"sink":{"connector":"sftp","action":"put","mock":{"enabled":true}}}`, true},
		{"test input on a real source", `{"name":"f","source":{"connector":"sftp","action":"get","testInput":{"enabled":true,"records":[{"a":1}]}},"sink":{"connector":"@discard","action":""}}`, true},
		{"probe between steps", `{"name":"f","steps":[{"id":"a","type":"source","connector":"@webhook","action":"ndjson","onSuccess":"p"},{"id":"p","type":"probe","onSuccess":"b"},{"id":"b","type":"sink","connector":"@discard"}],"start":"a"}`, true},
		{"mock on a source", `{"name":"f","source":{"connector":"sftp","action":"get","mock":{"enabled":true}},"sink":{"connector":"@discard","action":""}}`, false},
		{"test input on a sink", `{"name":"f","source":{"connector":"@webhook","action":"ndjson"},"sink":{"connector":"sftp","action":"put","testInput":{"enabled":true}}}`, false},
		// Mocking something that writes nowhere is a checkbox that changes
		// nothing — an author ticking it has misunderstood, and silence would
		// leave them believing they were protected.
		{"mock on @discard", `{"name":"f","source":{"connector":"@webhook","action":"ndjson"},"sink":{"connector":"@discard","action":"","mock":{"enabled":true}}}`, false},
		{"malformed test record", `{"name":"f","source":{"connector":"sftp","action":"get","testInput":{"enabled":true,"records":[123]}},"sink":{"connector":"@discard","action":""}}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if tc.ok && err != nil {
				t.Fatalf("rejected: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("accepted an option where it can do nothing")
			}
		})
	}
}

// The production step SURVIVES the option — that is the whole revision. A
// document carrying a mock still says which connector, which action and which
// build it runs, so publishing it needs no gate.
func TestAMockedStepKeepsItsProductionConfiguration(t *testing.T) {
	doc, err := Parse([]byte(`{"name":"f",
	  "source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"sftp","action":"put","version":"1.2.3","connection":"erp",
	          "config":{"path":"/out"},"mock":{"enabled":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Sink.Connector != "sftp" || doc.Sink.Action != "put" {
		t.Fatalf("the production connector was lost: %+v", doc.Sink)
	}
	if doc.Sink.Version != "1.2.3" || doc.Sink.Connection != "erp" {
		t.Fatalf("the production pin or connection was lost: %+v", doc.Sink)
	}
	// And it is still a pinned connector for retention purposes, because it
	// really does run in production.
	pins := doc.ConnectorPins()
	if len(pins) != 1 || pins[0].Version != "1.2.3" {
		t.Fatalf("a mocked step stopped counting as a connector reference: %+v", pins)
	}
}

// TestDiversions answers "what will this flow do differently as a test run?"
// from the document alone — the question that decides whether a run is safe
// against live systems.
func TestDiversionsAreReportedFromTheDocument(t *testing.T) {
	doc, err := Parse([]byte(`{"name":"f","steps":[
	  {"id":"in","type":"source","connector":"sftp","action":"get","testInput":{"enabled":true,"records":[{"a":1}]},"onSuccess":"p"},
	  {"id":"p","type":"probe","onSuccess":"out"},
	  {"id":"out","type":"sink","connector":"http","action":"post","mock":{"enabled":true}}],"start":"in"}`))
	if err != nil {
		t.Fatal(err)
	}
	got := doc.TestDiversions()
	if len(got) != 3 {
		t.Fatalf("diversions = %+v, want three", got)
	}
	want := []string{DiversionTestInput, DiversionProbe, DiversionMock}
	for i, k := range want {
		if got[i].Kind != k {
			t.Fatalf("diversion %d = %q, want %q (%+v)", i, got[i].Kind, k, got)
		}
	}

	// A mock switched OFF is not a diversion: unchecking it is how an author
	// says "drive the real system in this test".
	off, err := Parse([]byte(`{"name":"f","source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"http","action":"post","mock":{"enabled":false}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := off.TestDiversions(); len(got) != 0 {
		t.Fatalf("a disabled mock reported as a diversion: %+v", got)
	}
}
