package httpconn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// recordReader is the reader surface getSource selects at runtime (NDJSON
// line reader vs standard-JSON reader) based on the response Content-Type.
type recordReader interface {
	Next(context.Context) (*record.Batch, error)
	Close() error
}

// getSource GETs a URL and streams the response body into record batches.
// The body is parsed incrementally — it is never buffered whole.
type getSource struct {
	cfg    commonConfig
	client *http.Client
	resp   *http.Response
	reader recordReader
}

func (s *getSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.validate(); err != nil {
		return err
	}
	s.client = s.cfg.client()
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.resp == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.URL, nil)
		if err != nil {
			return nil, fmt.Errorf("http: %w", err)
		}
		s.cfg.apply(req)
		req.Header.Set("Accept", "application/x-ndjson, application/json")
		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("http: get %s: %w", s.cfg.URL, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			_ = resp.Body.Close()
			return nil, fmt.Errorf("http: get %s: status %d: %.200s", s.cfg.URL, resp.StatusCode, body)
		}
		s.resp = resp
		// Pick the parser by Content-Type: an NDJSON stream keeps the strict
		// line reader (tight per-line memory bound); anything else (typical REST
		// application/json — an array or a single object) uses the streaming JSON
		// reader. Both stream; neither buffers the whole body.
		if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "ndjson") {
			s.reader = ndjson.NewReader(resp.Body, ndjson.ReaderOptions{})
		} else {
			s.reader = ndjson.NewJSONReader(resp.Body, ndjson.ReaderOptions{})
		}
	}
	return s.reader.Next(ctx) // io.EOF surfaces naturally at stream end
}

func (s *getSource) Close() error {
	if s.resp != nil {
		return s.resp.Body.Close()
	}
	return nil
}
