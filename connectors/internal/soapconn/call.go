package soapconn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// callSource performs one SOAP request-reply and emits the response body's
// element(s) as records. It is a config-driven source: pick the verb, give the
// endpoint + envelope template + params, deploy — it runs standalone. The
// request fires on the first Next; the second Next returns io.EOF.
type callSource struct {
	cfg    config
	client *http.Client
	done   bool
	batch  *record.Batch
}

func (s *callSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	s.client = s.cfg.client()
	return nil
}

func (s *callSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true

	envelope, err := renderEnvelope(s.cfg.EnvelopeTemplate, s.cfg.Params)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Endpoint, strings.NewReader(envelope))
	if err != nil {
		return nil, fmt.Errorf("soap: %w", err)
	}
	s.cfg.apply(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("soap: call %s: %w", s.cfg.Endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Buffer the (bounded) body: a SOAP fault is delivered with an HTTP 500,
	// so the status alone can't distinguish a fault from a transport error —
	// we must parse the body to know. The body is a single bounded document.
	//
	// Read ONE byte past the limit so exceeding it is detectable. Reading
	// exactly MaxResponseBytes cannot distinguish "the document ended here"
	// from "there is more", so an oversized response was silently truncated
	// and then surfaced as `xml decode: unexpected EOF` — a size problem
	// reported as a syntax problem, and, for any XML that happens to stay
	// well-formed under truncation, silent data loss. The cap itself holds
	// either way (it is applied to the DECOMPRESSED stream, so a gzip bomb is
	// bounded too); this only makes hitting it honest. TC-020/TC-021.
	limit := int64(s.cfg.MaxResponseBytes)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("soap: reading response from %s: %w", s.cfg.Endpoint, err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("soap: response from %s exceeds max_response_bytes (%d)", s.cfg.Endpoint, limit)
	}

	root, perr := parseTree(body, s.cfg.MaxResponseElements)
	if perr != nil {
		// Unparseable body: surface the HTTP status if it was an error, else
		// the parse failure.
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("soap: call %s: status %d: %.200s", s.cfg.Endpoint, resp.StatusCode, body)
		}
		return nil, fmt.Errorf("soap: parsing response from %s: %w", s.cfg.Endpoint, perr)
	}

	bodyEl := findLocal(root, "Body")
	if bodyEl == nil {
		return nil, fmt.Errorf("soap: no <Body> in response from %s", s.cfg.Endpoint)
	}
	if fault := directChild(bodyEl, "Fault"); fault != nil {
		return nil, soapFault(fault)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("soap: call %s: status %d (no SOAP fault in body)", s.cfg.Endpoint, resp.StatusCode)
	}

	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	for _, child := range bodyEl.children {
		child.build(bld)
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *callSource) Close() error { return nil }
