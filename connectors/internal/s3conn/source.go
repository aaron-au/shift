package s3conn

import (
	"context"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// getSource streams a single object via GetObject, parsing the body into record
// batches with the configured format. The body is never buffered whole — the
// format reader wraps the response's io.ReadCloser directly.
type getSource struct {
	cfg    config
	body   interface{ Close() error }
	reader fileformat.Reader
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
	rd, err := fileformat.NewReader(s.cfg.Format, out.Body, fileformat.Options{RecordElement: s.cfg.RecordElement})
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
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
