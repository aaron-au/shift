package s3conn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// deleteSource performs a config-driven DeleteObject and emits a single status
// record ({op:"delete", bucket, key, ok:true}). It is a source so a one-verb
// flow is runnable on its own. DeleteObject on a missing key is a success on
// S3, so the op is naturally idempotent under at-least-once redelivery.
type deleteSource struct {
	cfg   config
	api   s3API
	done  bool
	batch *record.Batch
}

func (s *deleteSource) Open(_ context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireKey(); err != nil {
		return err
	}
	api, err := newClient(&s.cfg)
	if err != nil {
		return err
	}
	s.api = api
	return nil
}

func (s *deleteSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	if _, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.cfg.Key),
	}); err != nil {
		return nil, err
	}
	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("op")
	bld.StringLiteral("delete")
	bld.KeyLiteral("bucket")
	bld.StringLiteral(s.cfg.Bucket)
	bld.KeyLiteral("key")
	bld.StringLiteral(s.cfg.Key)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *deleteSource) Close() error { return nil }
