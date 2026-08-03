package redisconn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// deleteSource is the config-driven delete verb: it DELs the configured key and
// emits a single status record {op:"delete", key, deleted, ok}. It is a source
// so a one-verb flow runs standalone (ADR-0024). DEL is idempotent under
// at-least-once redelivery — a missing key deletes 0 and still reports ok.
type deleteSource struct {
	open   func(*config) (redisClient, error)
	cfg    config
	client redisClient
	done   bool
	batch  *record.Batch
}

func (s *deleteSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireKey(); err != nil {
		return err
	}
	client, err := s.open(&s.cfg)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func (s *deleteSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	deleted, err := s.client.Del(ctx, s.cfg.Key)
	if err != nil {
		return nil, err
	}
	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("op")
	bld.StringLiteral("delete")
	bld.KeyLiteral("key")
	bld.StringLiteral(s.cfg.Key)
	bld.KeyLiteral("deleted")
	bld.Int(deleted)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *deleteSource) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
