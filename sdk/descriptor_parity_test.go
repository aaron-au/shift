package sdk_test

import (
	"testing"

	"github.com/aaron-au/shift/sdk"
)

// TestDescriptorParityWithoutConnectionSchema: ADR-0018 requires that adding a
// descriptor field cannot invalidate an existing signature. A connector that
// declares no connection schema must therefore canonicalize to exactly the
// bytes it did before the field existed.
func TestDescriptorParityWithoutConnectionSchema(t *testing.T) {
	c := sdk.Connector{
		Name: "x", Version: "1.0.0",
		Sources: map[string]func() sdk.SourceAction{"get": nil},
		Schemas: map[string][]byte{"get": []byte(`{"type":"object"}`)},
	}
	got, err := sdk.CanonicalDescriptor(sdk.BuildDescriptor(c))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"name":"x","version":"1.0.0","actions":[{"action":"get","direction":"source","configSchema":{"type":"object"}}]}`
	if string(got) != want {
		t.Fatalf("canonical bytes changed:\n got %s\nwant %s", got, want)
	}

	// Declaring one must add exactly one field and nothing else.
	c.ConnectionSchema = []byte(`{"type":"object","properties":{"host":{"type":"string"}}}`)
	withConn, err := sdk.CanonicalDescriptor(sdk.BuildDescriptor(c))
	if err != nil {
		t.Fatal(err)
	}
	if string(withConn) == string(got) {
		t.Fatal("connection schema did not reach the descriptor")
	}
	if !contains(string(withConn), `"connectionSchema":{"type":"object","properties":{"host":{"type":"string"}}}`) {
		t.Fatalf("connection schema not carried verbatim: %s", withConn)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
