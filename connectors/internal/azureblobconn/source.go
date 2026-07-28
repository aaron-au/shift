package azureblobconn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// recordReader is satisfied by both the ndjson and csvf readers: emit batches
// until io.EOF. The batch is valid only until the next Next (reused).
type recordReader interface {
	Next(ctx context.Context) (*record.Batch, error)
}

// getSource downloads a blob and parses it into record batches via the
// configured format. The blob body is a live stream — the format reader wraps
// the io.ReadCloser directly, so the blob is never buffered whole.
type getSource struct {
	cfg    config
	open   storeOpener // nil in production; a fake in tests
	body   io.ReadCloser
	reader recordReader
}

func (s *getSource) Open(ctx context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireBlobFormat(); err != nil {
		return err
	}
	store, err := resolveOpener(s.open)(ctx, &s.cfg)
	if err != nil {
		return err
	}
	body, err := store.Download(ctx, s.cfg.Blob)
	if err != nil {
		return err
	}
	s.body = body
	switch s.cfg.Format {
	case "csv":
		s.reader = csvf.NewReader(body, csvf.ReaderOptions{})
	default:
		s.reader = ndjson.NewReader(body, ndjson.ReaderOptions{})
	}
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	return s.reader.Next(ctx)
}

func (s *getSource) Close() error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
