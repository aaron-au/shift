package azureblobconn

import (
	"context"
	"errors"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// deleteSource is the config-driven delete verb: it removes the configured blob
// and emits a single status record ({op:"delete", container, blob, ok:true}). It
// is a SOURCE so a one-verb flow runs standalone (ADR-0024). Deletion is
// idempotent — a missing blob (errNotFound) is reported as success, safe under
// at-least-once redelivery (ADR-0002).
type deleteSource struct {
	cfg   config
	open  storeOpener // nil in production; a fake in tests
	done  bool
	batch *record.Batch
}

func (s *deleteSource) Open(_ context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	return s.cfg.requireBlob()
}

func (s *deleteSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	store, err := resolveOpener(s.open)(ctx, &s.cfg)
	if err != nil {
		return nil, err
	}
	if err := store.Delete(ctx, s.cfg.Blob); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}

	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("op")
	bld.StringLiteral("delete")
	bld.KeyLiteral("container")
	bld.StringLiteral(s.cfg.Container)
	bld.KeyLiteral("blob")
	bld.StringLiteral(s.cfg.Blob)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *deleteSource) Close() error { return nil }
