package flow_test

import (
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
)

func planOf(t *testing.T, doc string) *flowdoc.Plan {
	t.Helper()
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	p, err := d.Plan()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return p
}

// A streaming plan can record a position: every confirmed sink write means
// everything up to it was delivered.
func TestStreamingPlanIsResumable(t *testing.T) {
	p := planOf(t, `{"name":"f",
	  "source":{"connector":"fs","action":"get","config":{"root":"/r","path":"a"}},
	  "ops":[{"type":"filter","path":"$.ok","op":"eq","value":true},
	         {"type":"project","fields":[{"path":"$.id"}]}],
	  "sink":{"connector":"gen","action":"discard"}}`)
	if !flow.Resumable(p) {
		t.Fatal("a filter/project pipeline should be resume-eligible")
	}
}

// An aggregate makes resume WRONG, not slow. It drains its whole input before
// emitting, so the first confirmed write already reports end-of-input; and
// rebuilding it from a suffix loses the state for everything before the
// cursor, so the output would be quietly incorrect.
func TestAggregatePlanIsNotResumable(t *testing.T) {
	p := planOf(t, `{"name":"f",
	  "source":{"connector":"fs","action":"get","config":{"root":"/r","path":"a"}},
	  "ops":[{"type":"aggregate","key":"$.region","aggs":[{"op":"count","out":"n"}]}],
	  "sink":{"connector":"gen","action":"discard"}}`)
	if flow.Resumable(p) {
		t.Fatal("an aggregate pipeline was marked resume-eligible; resuming it would produce wrong results")
	}
}

// Multi-path plans have several sources with independent positions and no
// single confirm point, so there is no one cursor to record.
func TestMultiPathPlanIsNotResumable(t *testing.T) {
	p := planOf(t, `{"name":"g","start":"in","steps":[
	  {"id":"in","type":"source","connector":"fs","action":"get","config":{"root":"/r","path":"a"},"onSuccess":"t"},
	  {"id":"t","type":"tee","branches":["a","b"]},
	  {"id":"a","type":"sink","connector":"@discard"},
	  {"id":"b","type":"sink","connector":"@discard"}]}`)
	if flow.Resumable(p) {
		t.Fatal("a fan-out plan was marked resume-eligible; the confirm point is per-branch")
	}
}

func TestNilPlanIsNotResumable(t *testing.T) {
	if flow.Resumable(nil) {
		t.Fatal("nil plan reported resumable")
	}
}
