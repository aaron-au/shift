package redisconn

import (
	"context"
	"io"
	"sort"

	"github.com/aaron-au/shift/engine/record"
)

// scanCount is the COUNT hint passed to SCAN and the soft cap on how many keys
// one Next call decodes into a batch. SCAN never blocks the server (unlike
// KEYS), so a large keyspace is walked in bounded pages.
const scanCount = 256

// listMax bounds how many list elements a get decodes per key (best-effort:
// huge lists are truncated rather than buffered whole).
const listMax = 1024

// getSource walks the keyspace with SCAN (never KEYS on a live server) and emits
// one record per key: {key, value, type}. String values are first-class; hash
// and list values are decoded best-effort into a map/list; other types
// (set/zset/stream) emit a null value with their type recorded.
type getSource struct {
	open      func(*config) (redisClient, error)
	cfg       config
	client    redisClient
	cursor    uint64
	exhausted bool
	batch     *record.Batch
}

func (s *getSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requirePattern(); err != nil {
		return err
	}
	client, err := s.open(&s.cfg)
	if err != nil {
		return err
	}
	s.client, s.batch = client, record.NewBatch()
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.exhausted {
		return nil, io.EOF
	}
	// Accumulate at least one page of keys; keep scanning past empty pages
	// (SCAN may return 0 keys with a non-zero cursor) until we have keys or the
	// cursor is exhausted.
	var keys []string
	for len(keys) < scanCount {
		page, next, err := s.client.Scan(ctx, s.cursor, s.cfg.Pattern, scanCount)
		if err != nil {
			return nil, err
		}
		s.cursor = next
		keys = append(keys, page...)
		if next == 0 {
			s.exhausted = true
			break
		}
	}
	if len(keys) == 0 {
		return nil, io.EOF // exhausted with no matches
	}

	s.batch.Reset()
	bld := s.batch.Builder()
	for _, key := range keys {
		if err := s.decode(ctx, bld, key); err != nil {
			return nil, err
		}
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

// decode reads one key's type and value and builds its {key, value, type} map.
func (s *getSource) decode(ctx context.Context, bld *record.Builder, key string) error {
	typ, err := s.client.Type(ctx, key)
	if err != nil {
		return err
	}
	bld.BeginMap()
	bld.KeyLiteral("key")
	bld.StringLiteral(key)
	bld.KeyLiteral("type")
	bld.StringLiteral(typ)
	bld.KeyLiteral("value")
	switch typ {
	case "string":
		v, err := s.client.Get(ctx, key)
		if err != nil {
			return err
		}
		bld.StringLiteral(v)
	case "hash":
		m, err := s.client.HGetAll(ctx, key)
		if err != nil {
			return err
		}
		fields := make([]string, 0, len(m))
		for f := range m {
			fields = append(fields, f)
		}
		sort.Strings(fields) // deterministic field order
		bld.BeginMap()
		for _, f := range fields {
			bld.KeyLiteral(f)
			bld.StringLiteral(m[f])
		}
		bld.EndMap()
	case "list":
		items, err := s.client.LRange(ctx, key, 0, listMax-1)
		if err != nil {
			return err
		}
		bld.BeginList()
		for _, it := range items {
			bld.StringLiteral(it)
		}
		bld.EndList()
	default:
		// none (vanished key), set, zset, stream: type is recorded, value null.
		bld.Null()
	}
	bld.EndMap()
	return nil
}

func (s *getSource) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
