package flowdoc

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aaron-au/shift/engine/schema"
)

// Input verification on the flow's entry step (ADR-0042 §4).
//
// Without it, an accepted request means only "the bytes arrived". With it, the
// runner validates BEFORE answering, so a malformed payload becomes a
// synchronous 400 with field-level detail while the caller is still holding the
// data and still in the code path that produced it — rather than a dead letter
// somebody reads tomorrow.
type Input struct {
	// Scope decides how much can be verified before the response, which is
	// bounded by how much can be read before the response. See ScopeBody /
	// ScopeRecords.
	Scope string `json:"scope,omitempty"`

	// Schema is a JSON Schema document (the subset in engine/schema). It is
	// compiled at validation time, so an unenforceable schema is a 422 at
	// authoring rather than a surprise at request time.
	Schema json.RawMessage `json:"schema,omitempty"`

	// MaxBytes caps how much of the request is read for verification. Zero
	// means DefaultMaxValidateBytes. It exists because "verify the whole body"
	// and "do not buffer the payload" are in tension, and the resolution has to
	// be an explicit number rather than an accident of available memory.
	MaxBytes int64 `json:"maxBytes,omitempty"`
}

// Input scopes.
const (
	// ScopeBody validates the ENTIRE request before accepting it. Right for an
	// API-shaped call, where the request is one document.
	ScopeBody = "body"

	// ScopeRecords validates the FIRST record and lets the rest stream. A
	// weaker guarantee, and deliberately so: you cannot verify what you have
	// not read, and reading a 1 GB request in order to verify it is exactly the
	// whole-payload buffering this platform exists to avoid. It catches the
	// overwhelmingly common failure — wrong field names or wrong types, which
	// are wrong from record one — without buying that at the cost of the
	// stream. A bad record 40,000 remains a dead letter, because there is no
	// version of this system where it could be anything else.
	ScopeRecords = "records"
)

// DefaultMaxValidateBytes bounds a scope: body verification. A body larger than
// this is refused with 413 rather than buffered: an unbounded read on the
// accept path is a memory-exhaustion primitive reachable by any caller.
const DefaultMaxValidateBytes = 1 << 20 // 1 MiB

// MaxValidateBytesCeiling is the largest value an author may set. The bound
// exists so that raising it is a decision with a limit, not an open door.
const MaxValidateBytesCeiling = 64 << 20 // 64 MiB

// Limit returns the effective byte cap for verification.
func (in *Input) Limit() int64 {
	if in == nil || in.MaxBytes <= 0 {
		return DefaultMaxValidateBytes
	}
	return in.MaxBytes
}

// EffectiveScope returns the scope, defaulted.
func (in *Input) EffectiveScope() string {
	if in == nil || in.Scope == "" {
		return ScopeBody
	}
	return in.Scope
}

// Compile builds the validator. The runner calls this once at plan build; the
// document has already been validated, so a failure here is a bug rather than
// user input.
func (in *Input) Compile() (*schema.Schema, error) {
	if in == nil || len(in.Schema) == 0 {
		return nil, nil
	}
	return schema.Compile(in.Schema)
}

// validate checks one input block in isolation.
func (in *Input) validate(where string) error {
	if in == nil {
		return nil
	}
	switch in.Scope {
	case "", ScopeBody, ScopeRecords:
	default:
		return fmt.Errorf("flow: %s: input scope %q must be %q or %q", where, in.Scope, ScopeBody, ScopeRecords)
	}
	if len(in.Schema) == 0 {
		// An input block with no schema verifies nothing while looking like it
		// does, which is the same trap as a silently-ignored keyword.
		return fmt.Errorf("flow: %s: input needs a schema (an input block with no schema verifies nothing)", where)
	}
	if in.MaxBytes < 0 {
		return fmt.Errorf("flow: %s: input maxBytes must not be negative", where)
	}
	if in.MaxBytes > MaxValidateBytesCeiling {
		return fmt.Errorf("flow: %s: input maxBytes %d exceeds the %d-byte ceiling",
			where, in.MaxBytes, MaxValidateBytesCeiling)
	}
	if _, err := schema.Compile(in.Schema); err != nil {
		// The compile error names the offending keyword and where it is, which
		// is what the studio surfaces. Wrapping keeps that intact.
		return fmt.Errorf("flow: %s: input schema: %w", where, err)
	}
	return nil
}

// validateInputs enforces where an input block may appear.
//
// It is confined to a @webhook source because that is the only place a REQUEST
// exists to verify: everything else is a pull, where "reject before accepting"
// has no caller to reject. Widening this later is additive; allowing it
// everywhere now would mean an input block that silently never runs.
func (d *Document) validateInputs() error {
	if in := d.Source.Input; in != nil {
		if d.Source.Connector != WebhookSource {
			return fmt.Errorf("flow: source: input verification applies to a %s source "+
				"(there is no request to verify on a %q source)", WebhookSource, d.Source.Connector)
		}
		if err := in.validate("source"); err != nil {
			return err
		}
	}
	if d.Sink.Input != nil {
		return errors.New("flow: sink: input verification applies to the entry step, not the sink")
	}

	seen := ""
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.Input == nil {
			continue
		}
		if s.Type != "source" || s.Connector != WebhookSource {
			return fmt.Errorf("flow: step %q: input verification applies to a %s source, not a %s step %q",
				s.ID, WebhookSource, s.Type, s.Connector)
		}
		if seen != "" {
			// A DAG may have several @webhook sources reading the same inbound
			// body (ADR-0029 fan-in). Two input blocks would mean two answers
			// to "is this request acceptable", and the accept path has one
			// response to give.
			return fmt.Errorf("flow: step %q: input verification is already declared on step %q; "+
				"a request has one accept decision", s.ID, seen)
		}
		seen = s.ID
		if err := s.Input.validate("step " + s.ID); err != nil {
			return err
		}
	}
	return nil
}

// InputSpec returns the flow's input verification, in either document form.
func (d *Document) InputSpec() (*Input, bool) {
	if d.Source.Input != nil {
		return d.Source.Input, true
	}
	for i := range d.Steps {
		if d.Steps[i].Input != nil {
			return d.Steps[i].Input, true
		}
	}
	return nil, false
}

// TerminatesAtResponse reports whether the flow returns its output to the
// caller — that is, whether it is SYNCHRONOUS (ADR-0042 §1).
//
// The choice is made by placing a node on the canvas rather than by a flag.
// A separate mode field would be a second source of truth about a question the
// graph already answers, and the two could disagree: a route marked "sync"
// pointing at a flow with no @response has no defined meaning. Making the
// terminal node the declaration keeps them from ever disagreeing.
func (d *Document) TerminatesAtResponse() bool {
	if d.Sink.Connector == ResponseSink {
		return true
	}
	for i := range d.Steps {
		if d.Steps[i].Type == "sink" && d.Steps[i].Connector == ResponseSink {
			return true
		}
	}
	return false
}
