package ftpconn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
)

// getSource streams a remote file, parsing it into record batches via the
// configured format. The file is never buffered whole — the format reader wraps
// the FTP transfer stream (an io.Reader) directly.
type getSource struct {
	dial   dialFunc // nil ⇒ realDial (test seam)
	cfg    config
	closer func() error
	rc     io.ReadCloser
	reader fileformat.Reader
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
	rd, err := fileformat.NewReader(s.cfg.Format, rc, fileformat.Options{RecordElement: s.cfg.RecordElement, Columns: s.cfg.Columns})
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
	if s.rc != nil {
		_ = s.rc.Close()
	}
	if s.closer != nil {
		return s.closer()
	}
	return nil
}
