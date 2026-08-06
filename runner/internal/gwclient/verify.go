package gwclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/schema"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Input verification on the accept path (ADR-0042 §4).
//
// The point of doing this BEFORE the flow runs is that a malformed payload is
// the one failure the caller can actually fix, and the only moment they still
// hold the data is the moment they sent it. Verified here, it is a 400 with the
// offending field named. Not verified, it is a dead letter somebody reads
// tomorrow, by which time the caller has moved on and the data is gone.

// schemaCache compiles each distinct schema once.
//
// Keyed by the schema TEXT rather than by flow name: the same text always
// compiles to the same validator, so there is no invalidation problem when a
// flow is republished — a changed schema is simply a different key, and the old
// entry ages out with the process. Compiling per request would parse JSON and
// rebuild a tree on the hot path, which is the cost this whole design avoids.
type schemaCache struct {
	m sync.Map // string(schema JSON) -> *schema.Schema
}

func (c *schemaCache) get(in *flowdoc.Input) (*schema.Schema, error) {
	if in == nil || len(in.Schema) == 0 {
		return nil, nil
	}
	key := string(in.Schema)
	if v, ok := c.m.Load(key); ok {
		s, _ := v.(*schema.Schema)
		return s, nil
	}
	s, err := in.Compile()
	if err != nil {
		// The document was validated at publish, so this is a bug rather than
		// user input — but it must not be a panic on the request path.
		return nil, err
	}
	c.m.Store(key, s)
	return s, nil
}

// verifyResult is the outcome of checking a request against the flow's input
// schema.
type verifyResult struct {
	// TooLarge means the body exceeded what the flow agreed to verify. It is a
	// 413, not a 400: the request may be perfectly valid, we simply refuse to
	// buffer it to find out.
	TooLarge bool
	Limit    int64
	// Violations is empty when the request is acceptable.
	Violations []schema.Violation
	// Err is a malformed body (not valid JSON at all), which is a 400 with no
	// per-field detail because there are no fields to point at.
	Err error
}

func (v verifyResult) ok() bool {
	return !v.TooLarge && v.Err == nil && len(v.Violations) == 0
}

// verify checks body against the flow's declared input, if it has one.
func (l *Loop) verify(doc *flowdoc.Document, body []byte) verifyResult {
	in, ok := doc.InputSpec()
	if !ok {
		return verifyResult{}
	}
	if limit := in.Limit(); int64(len(body)) > limit {
		// scope: records could in principle validate the first record of an
		// over-limit body, but the runner has already read the whole thing to
		// get here (the @webhook source binds a byte slice), so refusing is
		// honest about what the limit is protecting.
		return verifyResult{TooLarge: true, Limit: limit}
	}
	s, err := l.schemas.get(in)
	if err != nil || s == nil {
		if err != nil {
			l.log.Error("input schema failed to compile at request time; "+
				"the document should have been rejected at publish",
				"flow", doc.Name, "error", err)
		}
		// Fail OPEN on a compile bug: the schema is ours, the request is the
		// caller's, and refusing every request because our own document is bad
		// turns a publishing mistake into an outage. It is logged loudly.
		return verifyResult{}
	}

	batch := record.NewBatch()
	switch in.EffectiveScope() {
	case flowdoc.ScopeRecords:
		v, err := firstRecord(body, batch)
		if err != nil {
			return verifyResult{Err: err}
		}
		if v == nil {
			// An empty stream satisfies a per-record schema vacuously: there is
			// no record to be wrong.
			return verifyResult{}
		}
		return verifyResult{Violations: s.Validate(*v, nil)}
	default: // ScopeBody
		v, err := ndjson.ParseValue(bytes.TrimSpace(body), batch, ndjson.ReaderOptions{})
		if err != nil {
			return verifyResult{Err: err}
		}
		return verifyResult{Violations: s.Validate(v, nil)}
	}
}

// firstRecord returns the first record of a stream, or nil for an empty one.
// It accepts every shape the @webhook source does: NDJSON, a JSON array, or a
// single value.
func firstRecord(body []byte, b *record.Batch) (*record.Value, error) {
	r := ndjson.NewJSONReader(bytes.NewReader(body), ndjson.ReaderOptions{})
	defer func() { _ = r.Close() }()
	batch, err := r.Next(context.Background())
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	if batch.Len() == 0 {
		return nil, nil
	}
	// Copy out: the reader's batch is valid only until its next Next/Close,
	// and this value is held across the validation pass (engine contract).
	v := record.CopyValue(b, batch.Record(0))
	return &v, nil
}

// problem renders the ADR-0023 error envelope for a rejected request.
//
// The details array is the substance: "your JSON is wrong" is not a fixable
// message, whereas "/lines/2/qty: expected an integer, got \"three\"" is a
// one-line fix at the caller.
func problem(status int, code, message string, vs []schema.Violation) []byte {
	type detail struct {
		Path    string `json:"path"`
		Message string `json:"message"`
	}
	type envelope struct {
		Error struct {
			Status  int      `json:"status"`
			Code    string   `json:"code"`
			Message string   `json:"message"`
			Details []detail `json:"details,omitempty"`
		} `json:"error"`
	}
	var e envelope
	e.Error.Status = status
	e.Error.Code = code
	e.Error.Message = message
	for _, v := range vs {
		e.Error.Details = append(e.Error.Details, detail{Path: v.Path, Message: v.Message})
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Appendf(nil, `{"error":{"status":%d,"code":%q,"message":"%s"}}`, status, code, "request rejected")
	}
	return b
}
