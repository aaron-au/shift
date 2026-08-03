package redisconn

import (
	"context"
	"fmt"

	"github.com/aaron-au/shift/engine/record"
)

// setSink writes each record to Redis with SET: one record -> SET key value
// [EX ttl]. The key is either static (config key) or read per-record from the
// record's "key" field; the value is read from the configured value_field. SET
// is naturally idempotent under at-least-once redelivery — replaying the same
// record re-writes the same value.
type setSink struct {
	open   func(*config) (redisClient, error)
	cfg    config
	client redisClient
}

// recordKeyField is the record field read for a per-record key when no static
// key is configured.
const recordKeyField = "key"

func (s *setSink) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireValueField(); err != nil {
		return err
	}
	client, err := s.open(&s.cfg)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func (s *setSink) Write(ctx context.Context, b *record.Batch) error {
	for _, rec := range b.Records() {
		key, err := s.keyFor(rec)
		if err != nil {
			return err
		}
		vv, ok := rec.Field(s.cfg.ValueField)
		if !ok {
			return fmt.Errorf("redis: record has no %q field", s.cfg.ValueField)
		}
		val, err := valueToString(vv)
		if err != nil {
			return err
		}
		if err := s.client.Set(ctx, key, val, s.cfg.ttl()); err != nil {
			return err
		}
	}
	return nil
}

// keyFor resolves the SET key: the static config key when set, otherwise the
// record's "key" field (which must be a non-empty scalar).
func (s *setSink) keyFor(rec record.Value) (string, error) {
	if s.cfg.Key != "" {
		return s.cfg.Key, nil
	}
	kv, ok := rec.Field(recordKeyField)
	if !ok {
		return "", fmt.Errorf("redis: record has no %q field and no static key is configured", recordKeyField)
	}
	key, err := valueToString(kv)
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", fmt.Errorf("redis: %q field is empty", recordKeyField)
	}
	return key, nil
}

func (s *setSink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
