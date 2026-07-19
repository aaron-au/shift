package flowdoc

import (
	"encoding/json"
	"testing"
)

func TestParseAndValidate(t *testing.T) {
	good := `{
	  "name":"orders",
	  "source":{"connector":"http","action":"get","config":{"url":"https://x"}},
	  "ops":[
	    {"type":"filter","path":"$.active","op":"eq","value":true},
	    {"type":"filter","path":"$.amount","op":"gte","value":10.5},
	    {"type":"filter","path":"$.name","op":"exists"},
	    {"type":"coerce","rules":[{"field":"amount","to":"float"}]},
	    {"type":"flatten","sep":"."},
	    {"type":"project","fields":[{"path":"$.id"},{"out":"c","path":"$.addr.city"}]},
	    {"type":"aggregate","key":"$.region","aggs":[{"op":"count","out":"n"},{"op":"sum","path":"$.amount","out":"total"}]}
	  ],
	  "sink":{"connector":"http","action":"post","config":{"url":"https://y"}}
	}`
	if _, err := Parse([]byte(good)); err != nil {
		t.Fatalf("good doc rejected: %v", err)
	}

	bad := []string{
		`{"source":{"connector":"a","action":"b"},"sink":{"connector":"c","action":"d"}}`,                                                                                        // no name
		`{"name":"x","source":{"connector":"a"},"sink":{"connector":"c","action":"d"}}`,                                                                                          // source missing action
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"nope"}],"sink":{"connector":"c","action":"d"}}`,                                                     // unknown op
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"bad","op":"eq","value":1}],"sink":{"connector":"c","action":"d"}}`,                  // bad path
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"$.a","op":"eq"}],"sink":{"connector":"c","action":"d"}}`,                            // eq without value
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"filter","path":"$.a","op":"eq","value":{"o":1}}],"sink":{"connector":"c","action":"d"}}`,            // non-scalar value
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"coerce","rules":[{"field":"f","to":"complex"}]}],"sink":{"connector":"c","action":"d"}}`,            // bad kind
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"flatten"}],"sink":{"connector":"c","action":"d"}}`,                                                  // flatten no sep
		`{"name":"x","source":{"connector":"a","action":"b"},"ops":[{"type":"aggregate","key":"$.k","aggs":[{"op":"median","out":"m"}]}],"sink":{"connector":"c","action":"d"}}`, // bad agg
	}
	for i, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("bad doc %d accepted", i)
		}
	}
}

func TestWithSinkConfig(t *testing.T) {
	d, err := Parse([]byte(`{"name":"x",
	  "source":{"connector":"a","action":"b"},
	  "sink":{"connector":"c","action":"d","config":{"url":"https://y","keep":1}}}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := d.WithSinkConfig(map[string]any{"idempotency_key": "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Sink.Config, &got); err != nil {
		t.Fatal(err)
	}
	if got["url"] != "https://y" || got["keep"] != float64(1) || got["idempotency_key"] != "task-1" {
		t.Fatalf("merged config = %v", got)
	}
	// Original untouched.
	var orig map[string]any
	_ = json.Unmarshal(d.Sink.Config, &orig)
	if _, leaked := orig["idempotency_key"]; leaked {
		t.Fatal("WithSinkConfig mutated the original document")
	}
}
