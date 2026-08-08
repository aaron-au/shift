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

// ResumableSource is an OPTIONAL capability on a SourceAction: the ability to
// restart a stream from a position the action previously emitted (ADR-0037).
// A source that does not implement it is not broken — it replays from the
// beginning after a lost runner, which is today's behaviour and remains
// correct under at-least-once dispatch (ADR-0002).
//
// The cursor is opaque. The runner and the hub move these bytes around and
// store them as control metadata; only the connector that produced a cursor
// can interpret it. That opacity is what keeps resumption on the control
// plane: a page token, a byte offset, a CDC LSN and a keyset high-water mark
// are all a few bytes, and none of them is payload.
//
// Implement this on paginated, seekable or ordered sources — an object read,
// a keyset-paginated query, a change feed. Do not implement it on a source
// whose position is not meaningful (a one-shot request body); returning a
// cursor there would promise a resumption that cannot be honoured.
type ResumableSource interface {
	SourceAction
	// Resume positions the stream at cur, and is called after Open and
	// before the first Next. A nil or empty cursor means "from the
	// beginning" and MUST behave identically to not resuming at all.
	//
	// Return an error if cur cannot be honoured — a cursor from an
	// incompatible version, a range that no longer exists. Failing here is
	// correct and safe: the runner falls back to a full replay, which is
	// slower but never wrong. Silently starting from the beginning while
	// reporting success would let the runner record progress that did not
	// happen.
	Resume(ctx context.Context, cur []byte) error

	// Checkpoint returns a position that is safe to resume from ON THE
	// ASSUMPTION that every batch returned so far has been fully processed
	// downstream. It is called after each Next and must be cheap. Return nil
	// when no safe position exists yet (mid-page, mid-transaction).
	//
	// The runner never persists a checkpoint until the terminal sink has
	// confirmed the records it covers, so an implementation may report
	// progress freely without risking data loss.
	Checkpoint() []byte
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
	// Compat is this version's compatibility class relative to the one
	// before it: "compatible", "behaviour-change" or "breaking"
	// (ADR-0047 §6). It is the publisher's own statement, and the
	// compatibility gate (ADR-0047 §8, `sdk/compat` + `sdktest.CheckSurface`)
	// refuses a build that declares something weaker than the recorded
	// surface diff can support. Declaring something STRONGER is always
	// allowed — a publisher who knows a "compatible" change will surprise
	// people can say breaking, and nothing argues.
	//
	// It is deliberately NOT part of the Descriptor: the descriptor's
	// canonical bytes are hashed into the signed manifest (ADR-0018), and a
	// field that changes every release would churn that digest for something
	// the hub already stores per version.
	Compat string
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
	// ConnectionSchema describes the connection-level config for this
	// connector — the settings that identify the SYSTEM being talked to
	// (host, port, credentials, TLS) rather than the verb (ADR-0034).
	//
	// One schema per connector, not per action, because a host is a host
	// whether you are reading or deleting. Fields described here are
	// authored once as a named Connection and referenced by many nodes;
	// they are rejected on the node itself, so an author cannot silently
	// point one node at a different system than its siblings.
	//
	// Optional: a connector without one keeps every field on the node,
	// exactly as before, and its descriptor stays byte-identical.
	ConnectionSchema []byte
	// Meta is optional marketplace discovery metadata (M6e). It travels in
	// the signed descriptor (tamper-evident) and is rendered by the studio;
	// the hub never parses it. Absent Meta keeps the descriptor byte-identical
	// to a metadata-free one (ADR-0018 parity).
	Meta *ConnectorMeta
}

// ConnectorMeta is marketplace discovery metadata for a connector: human
// description, a category, an icon (emoji/short glyph), free-form tags, and a
// docs URL. All fields optional; it is descriptive only. Because it rides in
// the descriptor whose digest is bound into the signed manifest (ADR-0018), it
// cannot be altered without invalidating the signature.
type ConnectorMeta struct {
	Description string   `json:"description,omitempty"`
	Category    string   `json:"category,omitempty"`
	Icon        string   `json:"icon,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	DocsURL     string   `json:"docsURL,omitempty"`
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
	// ConnectionSchema is the connector's connection-level config schema
	// (ADR-0034). Omitted when absent, so a connector that declares none
	// produces byte-identical canonical bytes to before this field existed
	// — existing signatures stay valid (ADR-0018 parity).
	ConnectionSchema json.RawMessage `json:"connectionSchema,omitempty"`
	// Meta is optional discovery metadata (M6e); omitted (nil) keeps the
	// canonical bytes identical to a metadata-free descriptor.
	Meta *ConnectorMeta `json:"meta,omitempty"`
}

// BuildDescriptor assembles a connector's Descriptor from its declared
// actions and schemas, actions sorted by (direction, action). Shared by
// the Describe RPC and the `describe` CLI mode so both report identically.
func BuildDescriptor(c Connector) Descriptor {
	d := Descriptor{
		Name: c.Name, Version: c.Version, Meta: c.Meta,
		ConnectionSchema: schemaOrNil(c.ConnectionSchema),
	}
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
	// Sort tags so re-hash is independent of the publisher's declared order.
	if d.Meta != nil && len(d.Meta.Tags) > 0 {
		tags := slices.Clone(d.Meta.Tags)
		slices.Sort(tags)
		metaCopy := *d.Meta
		metaCopy.Tags = tags
		d.Meta = &metaCopy
	}
	return json.Marshal(d)
}

// Env var names forming the spawn contract with the host (ADR-0007).
const (
	EnvSocket = "SHIFT_CONNECTOR_SOCKET"
	EnvToken  = "SHIFT_CONNECTOR_TOKEN"
)
