package sftpconn

import (
	"context"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
	"github.com/pkg/sftp"
)

// getSource streams a remote file, parsing it into record batches via the
// configured format. The file is never buffered whole — the format reader
// wraps the *sftp.File (an io.Reader) directly.
type getSource struct {
	cfg    config
	closer func() error
	f      *sftp.File
	reader fileformat.Reader
}

func (s *getSource) Open(ctx context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireFileFormat(); err != nil {
		return err
	}
	sc, closer, err := s.cfg.dial(ctx)
	if err != nil {
		return err
	}
	f, err := sc.Open(s.cfg.Path)
	if err != nil {
		_ = closer()
		return err
	}
	s.closer, s.f = closer, f
	rd, err := fileformat.NewReader(s.cfg.Format, f, fileformat.Options{RecordElement: s.cfg.RecordElement})
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
	if s.f != nil {
		_ = s.f.Close()
	}
	if s.closer != nil {
		return s.closer()
	}
	return nil
}
