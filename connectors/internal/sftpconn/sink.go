package sftpconn

import (
	"context"
	"errors"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/pkg/sftp"
)

// recordWriter is satisfied by both the ndjson and csvf writers.
type recordWriter interface {
	Write(ctx context.Context, b *record.Batch) error
	Close() error
}

// putSink writes records to a remote file via the configured format. It writes
// to a temp path and atomically renames on Close, so a partial/failed transfer
// never leaves a half-written file at the destination.
type putSink struct {
	cfg     config
	sc      *sftp.Client
	closer  func() error
	f       *sftp.File
	w       recordWriter
	tmpPath string
}

func (s *putSink) Open(ctx context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	sc, closer, err := s.cfg.dial(ctx)
	if err != nil {
		return err
	}
	s.sc, s.closer = sc, closer
	s.tmpPath = s.cfg.Path + ".shift-partial"
	f, err := sc.Create(s.tmpPath)
	if err != nil {
		_ = closer()
		return err
	}
	s.f = f
	switch s.cfg.Format {
	case "csv":
		s.w = csvf.NewWriter(f, csvf.WriterOptions{})
	default:
		s.w = ndjson.NewWriter(f)
	}
	return nil
}

func (s *putSink) Write(ctx context.Context, b *record.Batch) error {
	return s.w.Write(ctx, b)
}

// Close flushes the format writer, closes the remote file, then atomically
// renames the temp file onto the destination. Any step failing aborts the
// rename so a bad transfer never overwrites the target.
func (s *putSink) Close() error {
	var errs []error
	if s.w != nil {
		if err := s.w.Close(); err != nil { // flush buffered format output
			errs = append(errs, err)
		}
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.sc != nil {
		if len(errs) == 0 {
			// PosixRename replaces the destination atomically.
			if err := s.sc.PosixRename(s.tmpPath, s.cfg.Path); err != nil {
				errs = append(errs, err)
			}
		} else {
			// Failed transfer: drop the partial, keep the destination intact.
			_ = s.sc.Remove(s.tmpPath)
		}
	}
	if s.closer != nil {
		if err := s.closer(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
