package fsconn

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
)

// putSink writes records to a file via the configured format. It writes to a
// temp file in the destination directory and atomically os.Renames it into
// place on Close, so a partial/failed write never leaves a half-written file at
// the destination (and never overwrites an existing good file on failure). The
// temp file shares the destination's directory so the rename stays on one
// filesystem (rename across filesystems is not atomic and can fail with EXDEV).
type putSink struct {
	cfg     config
	dest    string
	tmpPath string
	f       *os.File
	w       fileformat.Writer
}

func (s *putSink) Open(_ context.Context, config []byte) error {
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
	s.dest = full
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(full)+".shift-*")
	if err != nil {
		return err
	}
	s.f = f
	s.tmpPath = f.Name()
	wr, err := fileformat.NewWriter(s.cfg.Format, f, fileformat.Options{RecordElement: s.cfg.RecordElement})
	if err != nil {
		return err
	}
	s.w = wr
	return nil
}

func (s *putSink) Write(ctx context.Context, b *record.Batch) error {
	return s.w.Write(ctx, b)
}

// Close flushes the format writer, fsyncs and closes the temp file, then
// atomically renames it onto the destination. Any step failing aborts the
// rename and drops the temp file, so a bad write never overwrites the target.
func (s *putSink) Close() error {
	var errs []error
	if s.w != nil {
		if err := s.w.Close(); err != nil { // flush buffered format output
			errs = append(errs, err)
		}
	}
	if s.f != nil {
		if len(errs) == 0 {
			if err := s.f.Sync(); err != nil {
				errs = append(errs, err)
			}
		}
		if err := s.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.tmpPath != "" {
		if len(errs) == 0 {
			// os.Rename replaces the destination atomically (same filesystem).
			if err := os.Rename(s.tmpPath, s.dest); err != nil {
				errs = append(errs, err)
				_ = os.Remove(s.tmpPath)
			}
		} else {
			// Failed write: drop the partial, keep the destination intact.
			_ = os.Remove(s.tmpPath)
		}
	}
	return errors.Join(errs...)
}
