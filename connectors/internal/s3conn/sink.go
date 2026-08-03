package s3conn

import (
	"context"
	"errors"
	"io"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// recordWriter is satisfied by both the ndjson and csvf writers.
type recordWriter interface {
	Write(ctx context.Context, b *record.Batch) error
	Close() error
}

// putSink uploads the pipeline's records to a single object via PutObject,
// encoded with the configured format. To avoid buffering the whole payload in
// memory it streams: an io.Pipe connects the format writer (fed by Write) to a
// background PutObject whose request body is the pipe's read end. Backpressure
// flows naturally — Write blocks when S3 is slow to read.
type putSink struct {
	cfg    config
	cancel context.CancelFunc
	pw     *io.PipeWriter
	pr     *io.PipeReader
	w      recordWriter
	done   chan error // receives the PutObject result once the body is consumed
}

func (s *putSink) Open(_ context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireKeyFormat(); err != nil {
		return err
	}
	api, err := newClient(&s.cfg)
	if err != nil {
		return err
	}

	// The upload runs across the whole sink lifetime (many Write calls, then
	// Close), so it needs a context that outlives Open's; cancelled on Close.
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	s.cancel, s.pr, s.pw = cancel, pr, pw
	switch s.cfg.Format {
	case "csv":
		s.w = csvf.NewWriter(pw, csvf.WriterOptions{})
	default:
		s.w = ndjson.NewWriter(pw)
	}

	s.done = make(chan error, 1)
	go func() {
		_, err := api.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.cfg.Bucket),
			Key:    aws.String(s.cfg.Key),
			Body:   pr,
		})
		// If PutObject stopped reading early (error), unblock any pending
		// Write by failing the pipe; on success this is a plain EOF close.
		_ = pr.CloseWithError(err)
		s.done <- err
	}()
	return nil
}

func (s *putSink) Write(ctx context.Context, b *record.Batch) error {
	return s.w.Write(ctx, b)
}

// Close flushes the format writer, signals EOF to the upload by closing the
// pipe, waits for PutObject to finish, and reports any error. Closing the pipe
// even on a flush error guarantees the background goroutine never deadlocks.
func (s *putSink) Close() error {
	var errs []error
	if s.w != nil {
		if err := s.w.Close(); err != nil { // flush buffered format output
			errs = append(errs, err)
		}
	}
	if s.pw != nil {
		_ = s.pw.Close() // EOF to the PutObject body reader
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
