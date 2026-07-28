package fsconn

import (
	"context"
	"os"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// recordReader is satisfied by both the ndjson and csvf readers: emit batches
// until io.EOF. The batch is valid only until the next Next (reused).
type recordReader interface {
	Next(ctx context.Context) (*record.Batch, error)
}

// getSource streams a file, parsing it into record batches via the configured
// format. The file is never buffered whole — the format reader wraps the
// *os.File (an io.Reader) directly.
type getSource struct {
	cfg    config
	f      *os.File
	reader recordReader
}

func (s *getSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireFileFormat(); err != nil {
		return err
	}
	full, err := s.cfg.resolve(s.cfg.Path)
	if err != nil {
		return err
	}
	f, err := os.Open(full) //nolint:gosec // G304: full is jail-validated by config.resolve against the configured root
	if err != nil {
		return err
	}
	s.f = f
	switch s.cfg.Format {
	case "csv":
		s.reader = csvf.NewReader(f, csvf.ReaderOptions{})
	default:
		s.reader = ndjson.NewReader(f, ndjson.ReaderOptions{})
	}
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	return s.reader.Next(ctx)
}

func (s *getSource) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}
