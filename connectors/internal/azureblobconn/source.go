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
	rd, err := fileformat.NewReader(s.cfg.Format, body, fileformat.Options{RecordElement: s.cfg.RecordElement, Columns: s.cfg.Columns})
	if err != nil {
		return err
	}
	s.reader = rd
	return nil
}

// trippable is implemented by a bounded decompressing body. It is an interface
// rather than a concrete type so the test fakes, which hand over a plain
// ReadCloser, are unaffected.
type trippable interface{ Tripped() error }

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	b, err := s.reader.Next(ctx)
	if err == nil {
		return b, nil
	}
	// Consult the bound on EVERY failure AND on EOF. A tripped ratio truncates
	// the stream mid-record, so the format reader reports its own parse error
	// first — or, worse, a clean EOF — and the operator is sent hunting a data
	// bug that does not exist. The size problem is the true one.
	if t, ok := s.body.(trippable); ok {
		if tripped := t.Tripped(); tripped != nil {
			return nil, tripped
		}
	}
	return b, err
}

func (s *getSource) Close() error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
