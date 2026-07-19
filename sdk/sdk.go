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
	"context"

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
}

// Env var names forming the spawn contract with the host (ADR-0007).
const (
	EnvSocket = "SHIFT_CONNECTOR_SOCKET"
	EnvToken  = "SHIFT_CONNECTOR_TOKEN"
)
