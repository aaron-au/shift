package azureblobconn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
)

// getSource downloads a blob and parses it into record batches via the
// configured format. The blob body is a live stream — the format reader wraps
// the io.ReadCloser directly, so the blob is never buffered whole.
type getSource struct {
	cfg    config
	open   storeOpener // nil in production; a fake in tests
	body   io.ReadCloser
	reader fileformat.Reader
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
	rd, err := fileformat.NewReader(s.cfg.Format, body, fileformat.Options{RecordElement: s.cfg.RecordElement})
	if err != nil {
		return err
	}
	s.reader = rd
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
