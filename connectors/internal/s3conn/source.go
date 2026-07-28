package s3conn

import (
	"context"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// recordReader is satisfied by both the ndjson and csvf readers: emit batches
// until io.EOF. The batch is valid only until the next Next (reused).
type recordReader interface {
	Next(ctx context.Context) (*record.Batch, error)
}

// getSource streams a single object via GetObject, parsing the body into record
// batches with the configured format. The body is never buffered whole — the
// format reader wraps the response's io.ReadCloser directly.
type getSource struct {
	cfg    config
	body   interface{ Close() error }
	reader recordReader
}

func (s *getSource) Open(ctx context.Context, cfg []byte) error {
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
	out, err := api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.cfg.Key),
	})
	if err != nil {
		return err
	}
	s.body = out.Body
	switch s.cfg.Format {
	case "csv":
		s.reader = csvf.NewReader(out.Body, csvf.ReaderOptions{})
	default:
		s.reader = ndjson.NewReader(out.Body, ndjson.ReaderOptions{})
	}
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	return s.reader.Next(ctx)
}

func (s *getSource) Close() error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
