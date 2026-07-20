package connectors_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/host"
)

// TestDescribeCLIMode covers the publisher-side self-describe path
// (ADR-0018): `<connector> describe` prints canonical descriptor bytes
// that parse into a sdk.Descriptor carrying the action config schemas the
// studio builder renders. This is exactly what shift-bootstrap shells out
// to at publish time.
func TestDescribeCLIMode(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a binary")
	}
	bin := filepath.Join(t.TempDir(), "shift-connector-http")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "./cmd/shift-connector-http") //nolint:gosec // G204: test compiles our own package to a temp path
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build http connector: %v\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "describe").Output() //nolint:gosec // G204: our own freshly built binary
	if err != nil {
		t.Fatalf("describe: %v", err)
	}

	var d sdk.Descriptor
	if err := json.Unmarshal(out, &d); err != nil {
		t.Fatalf("descriptor not valid JSON: %v\n%s", err, out)
	}
	if d.Name != "http" {
		t.Fatalf("name = %q, want http", d.Name)
	}
	byKey := map[string]sdk.ActionDescriptor{}
	for _, a := range d.Actions {
		byKey[a.Direction+"/"+a.Action] = a
	}
	get, ok := byKey["source/get"]
	if !ok {
		t.Fatalf("missing source/get; actions = %+v", d.Actions)
	}
	if _, ok := byKey["sink/post"]; !ok {
		t.Fatalf("missing sink/post; actions = %+v", d.Actions)
	}
	// The config schema must be present and describe the url property so the
	// builder can render a form.
	var schema struct {
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(get.ConfigSchema, &schema); err != nil {
		t.Fatalf("get config schema not valid JSON schema: %v", err)
	}
	if _, ok := schema.Properties["url"]; !ok {
		t.Errorf("get config schema missing url property: %s", get.ConfigSchema)
	}

	// The same bytes must round-trip through host.ExtractDescriptor (the
	// gRPC path publisher tooling can also use) to identical actions.
	canon, viaRPC, err := host.ExtractDescriptor(ctx, bin)
	if err != nil {
		t.Fatalf("ExtractDescriptor: %v", err)
	}
	if len(viaRPC.Actions) != len(d.Actions) {
		t.Errorf("RPC actions = %d, CLI actions = %d", len(viaRPC.Actions), len(d.Actions))
	}
	if string(canon) != string(out[:len(out)-1]) { // out has a trailing newline
		t.Errorf("CLI and RPC canonical bytes differ:\ncli %s\nrpc %s", out, canon)
	}
}
