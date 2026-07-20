// Package sdk is the connector SDK: implement SourceAction and/or
// SinkAction, register them on a Connector, and call Serve from main.
// The runner spawns the binary and speaks the gRPC protocol in
// proto/connector/v1 over a unix socket (ADR-0001/0007).
//
// Interfaces mirror the engine's stream contract, including the batch
// lifetime rule: a batch returned by SourceAction.Next is valid only until
// the next Next/Close call, and SinkAction.Write must not retain the batch.
package sdk

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"

	"github.com/aaron-au/shift/engine/record"
)

// SourceAction produces record batches (e.g. an HTTP GET, a file read).
// One instance serves one Pull stream.
type SourceAction interface {
	// Open receives the action configuration (a JSON document).
	Open(ctx context.Context, config []byte) error
	// Next returns the next batch, or io.EOF when exhausted. The batch is
	// valid until the next call (reuse encouraged).
	Next(ctx context.Context) (*record.Batch, error)
	Close() error
}

// SinkAction consumes record batches. One instance serves one Push stream.
type SinkAction interface {
	Open(ctx context.Context, config []byte) error
	// Write consumes a batch; it must not retain it.
	Write(ctx context.Context, b *record.Batch) error
	// Close flushes; called once after the final Write.
	Close() error
}

// Connector declares a connector binary's identity and actions.
type Connector struct {
	Name    string
	Version string
	// Sources/Sinks map action names to factories; a fresh instance is
	// created per stream.
	Sources map[string]func() SourceAction
	Sinks   map[string]func() SinkAction
	// Schemas maps an action name to a JSON Schema (draft-07 subset)
	// describing that action's config document (ADR-0018). Optional: an
	// action without a schema still serves; the studio builder falls back
	// to a raw JSON editor for it. Descriptive only — Open remains the
	// config authority. Keyed by action name (a name shared by a source
	// and a sink shares one schema).
	Schemas map[string][]byte
}

// ActionDescriptor is one action's public shape within a Descriptor.
type ActionDescriptor struct {
	Action       string          `json:"action"`
	Direction    string          `json:"direction"` // "source" | "sink"
	ConfigSchema json.RawMessage `json:"configSchema,omitempty"`
}

// Descriptor is a connector's action catalog with config schemas
// (ADR-0018). The publisher tooling extracts it via the Describe RPC,
// signs its canonical bytes into the artifact manifest, and uploads it;
// the hub stores and serves the opaque bytes for the studio builder to
// render config forms. The hub never parses it.
type Descriptor struct {
	Name    string             `json:"name"`
	Version string             `json:"version"`
	Actions []ActionDescriptor `json:"actions"`
}

// BuildDescriptor assembles a connector's Descriptor from its declared
// actions and schemas, actions sorted by (direction, action). Shared by
// the Describe RPC and the `describe` CLI mode so both report identically.
func BuildDescriptor(c Connector) Descriptor {
	d := Descriptor{Name: c.Name, Version: c.Version}
	for name := range c.Sources {
		d.Actions = append(d.Actions, ActionDescriptor{Action: name, Direction: "source", ConfigSchema: schemaOrNil(c.Schemas[name])})
	}
	for name := range c.Sinks {
		d.Actions = append(d.Actions, ActionDescriptor{Action: name, Direction: "sink", ConfigSchema: schemaOrNil(c.Schemas[name])})
	}
	slices.SortFunc(d.Actions, func(a, b ActionDescriptor) int {
		return cmp.Or(cmp.Compare(a.Direction, b.Direction), cmp.Compare(a.Action, b.Action))
	})
	return d
}

func schemaOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

// CanonicalDescriptor renders a Descriptor to deterministic JSON bytes:
// actions sorted by (direction, action). The same bytes are hashed for
// the signature and uploaded verbatim, so the hub can re-hash the stored
// blob and verify without re-marshaling.
func CanonicalDescriptor(d Descriptor) ([]byte, error) {
	slices.SortFunc(d.Actions, func(a, b ActionDescriptor) int {
		return cmp.Or(cmp.Compare(a.Direction, b.Direction), cmp.Compare(a.Action, b.Action))
	})
	return json.Marshal(d)
}

// Env var names forming the spawn contract with the host (ADR-0007).
const (
	EnvSocket = "SHIFT_CONNECTOR_SOCKET"
	EnvToken  = "SHIFT_CONNECTOR_TOKEN"
)
