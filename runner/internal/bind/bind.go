// Package bind prepares a flow document for execution: it merges the
// reusable Connections its nodes reference (ADR-0034) and substitutes the
// {"$secret":"name"} references both the document and those connections
// carry (ADR-0010).
//
// It is one package, and callers make one call, because this work is
// needed on FOUR execution paths — the hub-queued lease loop, the webhook
// trigger, direct execution, and synchronous run (ADR-0016 / ADR-0024).
// Secret resolution previously lived inside the lease loop, which is why
// the three runner-direct paths silently shipped unresolved references to
// connectors. Splitting connections and secrets into two steps would
// recreate exactly that failure mode, one step later.
//
// Plaintext lives only in the returned document and value slice, both of
// which exist for the duration of one task. Nothing here writes to disk,
// and the values are handed to the service's redactor so they cannot
// reappear in a result, a log, or a capture sample.
package bind

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// fetchTimeout bounds the hub call. A trigger that cannot get its config
// must fail visibly rather than hang the caller — on the synchronous path
// there is a client waiting on the other end.
const fetchTimeout = 15 * time.Second

// ErrNoResolver reports a document that needs the hub — because it
// references a secret or a connection — on a runner with no hub attached.
// Failing is deliberate: passing a reference through would hand a
// connector `{"$secret":"name"}` where it expects a value, and the
// resulting error would describe a malformed host or a bad password rather
// than a missing hub.
var ErrNoResolver = errors.New("flow references secrets or connections but this runner is not attached to a hub")

// Connection is a reusable connector configuration as the hub serves it.
// Config keeps its {"$secret":...} references intact — substitution
// happens here, runner-side.
type Connection struct {
	Connector string          `json:"connector"`
	Config    json.RawMessage `json:"config"`
}

// Fetch retrieves, in ONE hub round trip, the named connections plus every
// secret the task needs — including those the connections themselves
// reference, which the runner cannot name until it has them. Asking for
// connections and then for their secrets would be two sequential calls,
// doubling hub latency on precisely the request-reply and webhook paths
// that exist to avoid it (ADR-0035 §3).
type Fetch func(ctx context.Context, connections, secrets []string) (map[string]Connection, map[string]string, error)

// FetchFrom adapts a hub client's ResolveTaskConfig to Fetch. It lives here
// so every caller wires the binder the same way and none has to know the
// shape of the hub response.
func FetchFrom[C any](
	resolve func(ctx context.Context, connections, secrets []string) (map[string]C, map[string]string, error),
	convert func(C) Connection,
) Fetch {
	if resolve == nil {
		return nil
	}
	return func(ctx context.Context, connections, secrets []string) (map[string]Connection, map[string]string, error) {
		raw, values, err := resolve(ctx, connections, secrets)
		if err != nil {
			return nil, nil, err
		}
		out := make(map[string]Connection, len(raw))
		for name, c := range raw {
			out[name] = convert(c)
		}
		return out, values, nil
	}
}

// Binder resolves a document's external references. The zero value (nil
// Fetch) is valid and rejects any document that needs one — the
// standalone-runner case.
type Binder struct {
	fetch Fetch
}

// New returns a Binder backed by fetch. A nil fetch yields a binder that
// rejects documents carrying references.
func New(fetch Fetch) *Binder { return &Binder{fetch: fetch} }

// Apply returns a copy of doc with every connection merged into the node
// that references it and every secret reference replaced, plus the
// resolved plaintext values for redaction. A document with no connections
// and no secrets is returned unchanged and costs no round trip.
//
// The caller MUST pass the returned values to the service as
// SubmitOpts.SecretValues; that is what keeps them out of results, logs
// and capture samples.
func (b *Binder) Apply(ctx context.Context, doc *flowdoc.Document) (*flowdoc.Document, []string, error) {
	conns := doc.Connections()
	refs, err := doc.SecretRefs()
	if err != nil {
		return nil, nil, err
	}
	if len(conns) == 0 && len(refs) == 0 {
		return doc, nil, nil
	}
	if b == nil || b.fetch == nil {
		return nil, nil, ErrNoResolver
	}

	fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	connections, values, err := b.fetch(fctx, conns, refs)
	if err != nil {
		return nil, nil, err
	}

	// Connections first: merging can introduce secret references the
	// document did not carry, and those must be substituted too. The other
	// order leaves a live {"$secret":…} in the merged config.
	doc, err = applyConnections(doc, connections)
	if err != nil {
		return nil, nil, err
	}

	resolved, err := doc.ResolveSecrets(func(name string) (string, error) {
		v, ok := values[name]
		if !ok {
			// Names only — a missing secret must not be diagnosed by
			// echoing anything that was returned.
			return "", fmt.Errorf("secret %q not returned by hub", name)
		}
		return v, nil
	})
	if err != nil {
		return nil, nil, err
	}

	plaintext := make([]string, 0, len(values))
	for _, v := range values {
		plaintext = append(plaintext, v)
	}
	return resolved, plaintext, nil
}

// applyConnections computes each referencing node's effective config:
// connection config, then node config. It returns a copy; the caller's
// document is untouched.
func applyConnections(doc *flowdoc.Document, conns map[string]Connection) (*flowdoc.Document, error) {
	uses := doc.ConnectionUses()
	if len(uses) == 0 {
		return doc, nil
	}
	out := *doc
	if len(doc.Steps) > 0 {
		out.Steps = make([]flowdoc.Step, len(doc.Steps))
		copy(out.Steps, doc.Steps)
	}
	for _, use := range uses {
		c, ok := conns[use.Connection]
		if !ok {
			return nil, fmt.Errorf("%s: connection %q not returned by hub", use.Label, use.Connection)
		}
		// The hub checks this at deploy, but a connection may be edited
		// afterwards — connections are not versioned (ADR-0034 open
		// question 3) — so the mismatch is reachable at run time.
		if c.Connector != use.Connector {
			return nil, fmt.Errorf("%s declares connector %q but connection %q configures %q",
				use.Label, use.Connector, use.Connection, c.Connector)
		}
		merged, err := flowdoc.MergeConnectionConfig(c.Config, use.Config)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", use.Label, err)
		}
		if err := setConfig(&out, use.Label, merged); err != nil {
			return nil, err
		}
	}
	return &out, nil
}

// setConfig writes a node's effective config back onto the copied
// document, addressing the node the same way ConnectionUses labelled it.
func setConfig(doc *flowdoc.Document, label string, cfg json.RawMessage) error {
	switch label {
	case "source":
		doc.Source.Config = cfg
		return nil
	case "sink":
		doc.Sink.Config = cfg
		return nil
	}
	id, ok := strings.CutPrefix(label, "step ")
	if !ok {
		return fmt.Errorf("bind: unrecognised node label %q", label)
	}
	for i := range doc.Steps {
		if doc.Steps[i].ID == id {
			doc.Steps[i].Config = cfg
			return nil
		}
	}
	return fmt.Errorf("bind: step %q not found", id)
}
