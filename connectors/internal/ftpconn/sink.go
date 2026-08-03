package ftpconn

import (
	"context"
	"errors"
	"io"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// recordWriter is satisfied by both the ndjson and csvf writers.
type recordWriter interface {
	Write(ctx context.Context, b *record.Batch) error
	Close() error
}

// putSink writes records to a remote file via the configured format. FTP's Stor
// takes a single io.Reader and pulls from it until EOF, so the sink bridges the
// batch-at-a-time Write contract to that streaming reader with an io.Pipe: Stor
// runs in a goroutine reading the pipe while Write encodes batches into it.
// Nothing is buffered whole. It stores to a temp path and renames onto the
// destination on Close, so a partial/failed transfer never leaves a
// half-written file at the target.
type putSink struct {
	dial    dialFunc // nil ⇒ realDial (test seam)
	cfg     config
	conn    ftpConn
	closer  func() error
	pw      *io.PipeWriter
	w       recordWriter
	tmpPath string
	storErr chan error
}

func (s *putSink) Open(ctx context.Context, config []byte) error {
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
	s.conn, s.closer = conn, closer
	s.tmpPath = s.cfg.Path + ".shift-partial"

	pr, pw := io.Pipe()
	s.pw = pw
	switch s.cfg.Format {
	case "csv":
		s.w = csvf.NewWriter(pw, csvf.WriterOptions{})
	default:
		s.w = ndjson.NewWriter(pw)
	}
	s.storErr = make(chan error, 1)
	go func() {
		err := conn.Stor(s.tmpPath, pr)
		// If Stor stops early (server/connection error), close the read end so a
		// pending or subsequent Write returns rather than blocking on the pipe.
		_ = pr.CloseWithError(err)
		s.storErr <- err
	}()
	return nil
}

func (s *putSink) Write(ctx context.Context, b *record.Batch) error {
	return s.w.Write(ctx, b)
}

// Close flushes the format writer, signals EOF to the Stor goroutine, waits for
// the transfer to complete, then atomically renames the temp file onto the
// destination. Any step failing aborts the rename and drops the partial so a
// bad transfer never overwrites the target.
func (s *putSink) Close() error {
	var errs []error
	if s.w != nil {
		if err := s.w.Close(); err != nil { // flush buffered format output into the pipe
			errs = append(errs, err)
		}
	}
	if s.pw != nil {
		_ = s.pw.Close() // EOF: let Stor finish reading
	}
	if s.storErr != nil {
		if err := <-s.storErr; err != nil {
			errs = append(errs, err)
		}
	}
	if s.conn != nil {
		if len(errs) == 0 {
			if err := s.conn.Rename(s.tmpPath, s.cfg.Path); err != nil {
				errs = append(errs, err)
			}
		} else {
			// Failed transfer: drop the partial, keep the destination intact.
			_ = s.conn.Delete(s.tmpPath)
		}
	}
	if s.closer != nil {
		if err := s.closer(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
