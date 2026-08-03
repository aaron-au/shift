package s3conn

import (
	"context"
	"io"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// listSource lists a bucket by prefix via ListObjectsV2, emitting one record
// per object: {key, size, etag, last_modified}. It streams page by page — each
// Next fetches the next page (S3 returns up to 1000 keys) and emits it as one
// batch, following the continuation token until the listing is exhausted.
type listSource struct {
	cfg   config
	api   s3API
	token *string // ListObjectsV2 continuation token for the next page
	done  bool    // set once the last (non-truncated) page has been emitted
	batch *record.Batch
}

func (s *listSource) Open(_ context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	api, err := newClient(&s.cfg)
	if err != nil {
		return err
	}
	s.api, s.batch = api, record.NewBatch()
	return nil
}

func (s *listSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.Bucket)}
	if s.cfg.Prefix != "" {
		in.Prefix = aws.String(s.cfg.Prefix)
	}
	if s.token != nil {
		in.ContinuationToken = s.token
	}
	out, err := s.api.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, err
	}
	if aws.ToBool(out.IsTruncated) && out.NextContinuationToken != nil {
		s.token = out.NextContinuationToken
	} else {
		s.done = true
	}

	s.batch.Reset()
	bld := s.batch.Builder()
	for i := range out.Contents {
		o := out.Contents[i]
		bld.BeginMap()
		bld.KeyLiteral("key")
		bld.StringLiteral(aws.ToString(o.Key))
		bld.KeyLiteral("size")
		bld.Int(aws.ToInt64(o.Size))
		bld.KeyLiteral("etag")
		bld.StringLiteral(aws.ToString(o.ETag))
		bld.KeyLiteral("last_modified")
		bld.StringLiteral(aws.ToTime(o.LastModified).UTC().Format(time.RFC3339))
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }
