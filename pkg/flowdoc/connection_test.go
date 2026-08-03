package flowdoc

import (
	"strings"
	"testing"
)

// TestConnectionIsOptionalAndAdditive is the compatibility guarantee that makes
// ADR-0034 safe to land: flows are stored, versioned and published, so a
// document written before connections existed must still validate and plan
// exactly as it did.
func TestConnectionIsOptionalAndAdditive(t *testing.T) {
	const noConnection = `{"name":"legacy","source":{"connector":"sftp","action":"get",
	  "config":{"host":"h","path":"/in"}},"sink":{"connector":"http","action":"post"}}`

	d, err := Parse([]byte(noConnection))
	if err != nil {
		t.Fatalf("a pre-connections document must stay valid: %v", err)
	}
	if got := d.Connections(); len(got) != 0 {
		t.Errorf("Connections() = %v, want none", got)
	}
	if _, err := d.Plan(); err != nil {
		t.Errorf("plan: %v", err)
	}
}

// TestConnectionReferenceCollected: the hub needs the set of referenced
// connections to validate a deploy and to know which flows depend on a
// connection before it is edited or deleted.
func TestConnectionReferenceCollected(t *testing.T) {
	linear := `{"name":"f","source":{"connector":"sftp","action":"get","connection":"prod-sftp",
	  "config":{"path":"/in"}},"sink":{"connector":"sftp","action":"put","connection":"prod-sftp",
	  "config":{"path":"/out"}}}`
	d, err := Parse([]byte(linear))
	if err != nil {
		t.Fatal(err)
	}
	// Deduplicated: two nodes, one connection.
	if got := d.Connections(); len(got) != 1 || got[0] != "prod-sftp" {
		t.Errorf("Connections() = %v, want [prod-sftp]", got)
	}

	graph := `{"name":"g","start":"in","steps":[
	  {"id":"in","type":"source","connector":"sftp","action":"get","connection":"b-conn"},
	  {"id":"out","type":"sink","connector":"http","action":"post","connection":"a-conn"}],
	  "steps_note":"in->out"}`
	graph = strings.Replace(graph,
		`{"id":"in","type":"source","connector":"sftp","action":"get","connection":"b-conn"}`,
		`{"id":"in","type":"source","connector":"sftp","action":"get","connection":"b-conn","onSuccess":"out"}`, 1)
	gd, err := Parse([]byte(graph))
	if err != nil {
		t.Fatal(err)
	}
	// Sorted, so a report or a diff is stable.
	if got := gd.Connections(); len(got) != 2 || got[0] != "a-conn" || got[1] != "b-conn" {
		t.Errorf("Connections() = %v, want [a-conn b-conn] sorted", got)
	}
}

// TestConnectionRejectedOnBuiltins: @webhook, @discard and @response talk to no
// external system, so a connection on one is meaningless. Rejecting beats
// ignoring — an ignored setting is silence where the author expected
// configuration.
func TestConnectionRejectedOnBuiltins(t *testing.T) {
	for _, doc := range []string{
		`{"name":"f","source":{"connector":"@webhook","action":"ndjson","connection":"c"},
		  "sink":{"connector":"http","action":"post"}}`,
		`{"name":"f","source":{"connector":"http","action":"get"},
		  "sink":{"connector":"@discard","connection":"c"}}`,
	} {
		_, err := Parse([]byte(doc))
		if err == nil {
			t.Errorf("built-in accepted a connection: %s", doc)
			continue
		}
		if !strings.Contains(err.Error(), "takes no connection") {
			t.Errorf("error = %v, want it to say built-ins take no connection", err)
		}
	}
}

// TestConnectionNameCharset: the name is addressed in the hub's control API and
// rendered on studio nodes, so it takes the same tight charset as a secret.
func TestConnectionNameCharset(t *testing.T) {
	doc := func(conn string) []byte {
		return []byte(`{"name":"f","source":{"connector":"sftp","action":"get","connection":"` +
			conn + `"},"sink":{"connector":"@discard"}}`)
	}
	for _, ok := range []string{"prod-sftp", "a", "a.b_c-1", "A1"} {
		if _, err := Parse(doc(ok)); err != nil {
			t.Errorf("valid connection name %q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"has space", "-leading", "a/b", `a"b`, "<script>", strings.Repeat("a", 129)} {
		if _, err := Parse(doc(bad)); err == nil {
			t.Errorf("connection name %q accepted, want rejection", bad)
		}
	}
}

// TestConnectionSurvivesStepToEndpoint: the runner binds a graph step through
// Endpoint(), so a connection dropped there would be silently ignored at
// execution — the node would connect to nothing, or to a default.
func TestConnectionSurvivesStepToEndpoint(t *testing.T) {
	s := &Step{ID: "in", Connector: "sftp", Action: "get", Connection: "prod-sftp"}
	s.Type = "source"
	if got := s.Endpoint().Connection; got != "prod-sftp" {
		t.Fatalf("Endpoint().Connection = %q, want it carried through", got)
	}
}
