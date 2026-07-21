package sdk

import (
	"bytes"
	"strings"
	"testing"
)

// TestCanonicalDescriptorNoMetaStable pins ADR-0018 parity: a connector without
// discovery metadata must produce descriptor bytes with no "meta" key, so its
// signature stays byte-identical to the pre-M6e form.
func TestCanonicalDescriptorNoMetaStable(t *testing.T) {
	c := Connector{
		Name: "gen", Version: "0.1.0",
		Sources: map[string]func() SourceAction{"gen": nil},
		Sinks:   map[string]func() SinkAction{"discard": nil},
	}
	b, err := CanonicalDescriptor(BuildDescriptor(c))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "meta") {
		t.Fatalf("no-meta descriptor should omit meta, got %s", b)
	}
}

// TestCanonicalDescriptorMetaDeterministic: metadata rides in the descriptor,
// and tag order must not change the canonical bytes (re-hash must match
// regardless of the publisher's declared tag order).
func TestCanonicalDescriptorMetaDeterministic(t *testing.T) {
	mk := func(tags []string) []byte {
		c := Connector{
			Name: "http", Version: "0.1.0",
			Sources: map[string]func() SourceAction{"get": nil},
			Meta:    &ConnectorMeta{Description: "HTTP", Category: "protocol", Icon: "🌐", Tags: tags},
		}
		b, err := CanonicalDescriptor(BuildDescriptor(c))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a := mk([]string{"rest", "api", "http"})
	b := mk([]string{"http", "api", "rest"})
	if !bytes.Equal(a, b) {
		t.Fatalf("tag order changed canonical bytes:\n a=%s\n b=%s", a, b)
	}
	if !strings.Contains(string(a), `"category":"protocol"`) {
		t.Fatalf("meta not carried into descriptor: %s", a)
	}
}
