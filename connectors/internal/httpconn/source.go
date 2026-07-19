package httpconn

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// getSource GETs a URL and streams the response body into record batches.
// The body is parsed incrementally — it is never buffered whole.
type getSource struct {
	cfg    commonConfig
	client *http.Client
	resp   *http.Response
	reader *ndjson.Reader
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
		s.reader = ndjson.NewReader(resp.Body, ndjson.ReaderOptions{})
	}
	return s.reader.Next(ctx) // io.EOF surfaces naturally at stream end
}

func (s *getSource) Close() error {
	if s.resp != nil {
		return s.resp.Body.Close()
	}
	return nil
}
