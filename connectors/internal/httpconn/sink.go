package httpconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// postSink POSTs each incoming batch as one NDJSON request body. Memory is
// bounded by batch size; per-batch requests keep failures attributable and
// retryable (flow-level retry policy arrives in M5).
type postSink struct {
	cfg    commonConfig
	client *http.Client
	buf    bytes.Buffer
}

func (s *postSink) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.validate(); err != nil {
		return err
	}
	s.client = s.cfg.client()
	return nil
}

func (s *postSink) Write(ctx context.Context, b *record.Batch) error {
	s.buf.Reset()
	w := ndjson.NewWriter(&s.buf)
	if err := w.Write(ctx, b); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(s.buf.Bytes()))
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	s.cfg.apply(req)
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http: post %s: %w", s.cfg.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("http: post %s: status %d: %.200s", s.cfg.URL, resp.StatusCode, body)
	}
	_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
	return nil
}

func (s *postSink) Close() error { return nil }
