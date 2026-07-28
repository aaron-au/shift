package azureblobconn

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

// putSink streams records up to a block blob. UploadStream needs a single
// io.Reader for the whole object, but the sink is fed batch-by-batch — so an
// io.Pipe bridges the two: Write encodes each batch into the pipe, a background
// goroutine feeds UploadStream from the read end, and Close flushes then waits.
// A re-dispatched put overwrites the same blob key, so it is idempotent under
// at-least-once redelivery (ADR-0002) — no per-record idempotency key needed.
type putSink struct {
	cfg    config
	open   storeOpener // nil in production; a fake in tests
	ctx    context.Context
	cancel context.CancelFunc
	pw     *io.PipeWriter
	w      recordWriter
	done   chan error
}

func (s *putSink) Open(ctx context.Context, cfg []byte) error {
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
	// Detach the upload from Open's ctx (which may be per-call): the stream
	// lives until Close, which cancels this context.
	s.ctx, s.cancel = context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	s.pw = pw
	switch s.cfg.Format {
	case "csv":
		s.w = csvf.NewWriter(pw, csvf.WriterOptions{})
	default:
		s.w = ndjson.NewWriter(pw)
	}
	s.done = make(chan error, 1)
	go func() {
		err := store.Upload(s.ctx, s.cfg.Blob, pr)
		// Unblock any in-flight Write: on error propagate it back through the
		// pipe; on success the reader already saw EOF from Close's pw.Close.
		if err != nil {
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
		s.done <- err
	}()
	return nil
}

func (s *putSink) Write(ctx context.Context, b *record.Batch) error {
	// If Upload already failed, the pipe is closed-with-error and this returns
	// that error, attributing the failure to the write.
	return s.w.Write(ctx, b)
}

// Close flushes the format writer into the pipe, signals EOF to the uploader,
// and waits for UploadStream to finish, joining any errors.
func (s *putSink) Close() error {
	var errs []error
	if s.w != nil {
		if err := s.w.Close(); err != nil { // flush buffered format output into the pipe
			errs = append(errs, err)
		}
	}
	if s.pw != nil {
		_ = s.pw.Close() // EOF to the reader → UploadStream completes
	}
	if s.done != nil {
		if err := <-s.done; err != nil {
			errs = append(errs, err)
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	return errors.Join(errs...)
}
