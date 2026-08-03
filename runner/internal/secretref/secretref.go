// Package secretref resolves a flow document's {"$secret":"name"}
// references before execution (ADR-0010).
//
// It exists as its own package because resolution is needed on FOUR
// execution paths, not one: the hub-queued lease loop, the webhook
// trigger, direct execution, and synchronous run (ADR-0016 / ADR-0024).
// Living inside the lease loop is why the three runner-direct paths
// silently shipped unresolved references to connectors — a document
// arriving at a connector with a reference object where a string belongs.
//
// Plaintext lives only in the returned document and value slice, both of
// which stay in memory for the duration of one task. Nothing here writes
// to disk, and the values are handed to the service's redactor so they
// cannot reappear in a result, a log, or a capture sample.
package secretref

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// fetchTimeout bounds one hub resolve call. A trigger that cannot get its
// credentials must fail visibly rather than hang the caller — on the
// synchronous path there is a client waiting on the other end.
const fetchTimeout = 15 * time.Second

// ErrNoResolver reports a document that references secrets on a runner
// with no hub attached. Failing is deliberate: passing the reference
// through would hand a connector `{"$secret":"name"}` where it expects a
// value, and the resulting error would describe a malformed host or a bad
// password rather than a missing hub.
var ErrNoResolver = errors.New("flow references secrets but this runner is not attached to a hub")

// Fetch retrieves plaintext values for the named secrets. It is the hub
// client's resolve call in production; tests substitute their own.
type Fetch func(ctx context.Context, names []string) (map[string]string, error)

// Resolver substitutes secret references into flow documents. The zero
// value (nil Fetch) is valid and fails any document that references a
// secret — the standalone-runner case.
type Resolver struct {
	fetch Fetch
}

// New returns a Resolver backed by fetch. A nil fetch yields a resolver
// that rejects documents carrying references.
func New(fetch Fetch) *Resolver { return &Resolver{fetch: fetch} }

// Apply returns a copy of doc with every secret reference replaced, plus
// the resolved plaintext values for redaction. A document with no
// references is returned unchanged and costs no round trip.
//
// The caller MUST pass the returned values to the service as
// SubmitOpts.SecretValues; that is what keeps them out of results, logs
// and capture samples.
func (r *Resolver) Apply(ctx context.Context, doc *flowdoc.Document) (*flowdoc.Document, []string, error) {
	refs, err := doc.SecretRefs()
	if err != nil {
		return nil, nil, err
	}
	if len(refs) == 0 {
		return doc, nil, nil
	}
	if r == nil || r.fetch == nil {
		return nil, nil, ErrNoResolver
	}
	fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	values, err := r.fetch(fctx, refs)
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
