package ftpconn

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

// getSource streams a remote file, parsing it into record batches via the
// configured format. The file is never buffered whole — the format reader wraps
// the FTP transfer stream (an io.Reader) directly.
type getSource struct {
	dial   dialFunc // nil ⇒ realDial (test seam)
	cfg    config
	closer func() error
	rc     io.ReadCloser
	reader recordReader
}

func (s *getSource) Open(ctx context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireFileFormat(); err != nil {
		return err
	}
	conn, closer, err := dialOr(s.dial)(ctx, &s.cfg)
	if err != nil {
		return err
	}
	rc, err := conn.Retr(s.cfg.Path)
	if err != nil {
		_ = closer()
		return err
	}
	s.closer, s.rc = closer, rc
	switch s.cfg.Format {
	case "csv":
		s.reader = csvf.NewReader(rc, csvf.ReaderOptions{})
	default:
		s.reader = ndjson.NewReader(rc, ndjson.ReaderOptions{})
	}
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	return s.reader.Next(ctx)
}

func (s *getSource) Close() error {
	if s.rc != nil {
		_ = s.rc.Close()
	}
	if s.closer != nil {
		return s.closer()
	}
	return nil
}
