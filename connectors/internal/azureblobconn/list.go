package azureblobconn

import (
	"context"
	"io"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// listBatch caps how many blob entries one Next emits.
const listBatch = 512

// listSource enumerates blobs under a prefix, emitting one record per blob:
// {name, size, etag, last_modified, content_type}. The listing is drained at
// Open (metadata only — small) and iterated from memory; the connection is not
// held open across Next calls.
type listSource struct {
	cfg   config
	open  storeOpener // nil in production; a fake in tests
	infos []blobInfo
	idx   int
	batch *record.Batch
}

func (s *listSource) Open(ctx context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	store, err := resolveOpener(s.open)(ctx, &s.cfg)
	if err != nil {
		return err
	}
	if err := store.List(ctx, s.cfg.Prefix, func(bi blobInfo) error {
		s.infos = append(s.infos, bi)
		return nil
	}); err != nil {
		return err
	}
	s.batch = record.NewBatch()
	return nil
}

func (s *listSource) Next(_ context.Context) (*record.Batch, error) {
	if s.idx >= len(s.infos) {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	for n := 0; s.idx < len(s.infos) && n < listBatch; s.idx, n = s.idx+1, n+1 {
		e := s.infos[s.idx]
		bld.BeginMap()
		bld.KeyLiteral("name")
		bld.StringLiteral(e.Name)
		bld.KeyLiteral("size")
		bld.Int(e.Size)
		bld.KeyLiteral("etag")
		bld.StringLiteral(e.ETag)
		bld.KeyLiteral("last_modified")
		bld.StringLiteral(e.LastModified.UTC().Format(time.RFC3339))
		bld.KeyLiteral("content_type")
		bld.StringLiteral(e.ContentType)
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }
