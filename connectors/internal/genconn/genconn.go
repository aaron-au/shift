// Package genconn is the test/benchmark connector: a deterministic
// synthetic record source and a counting discard sink. It exists to
// exercise and measure the connector transport itself.
package genconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
)

// Connector returns the gen connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "gen",
		Version: "0.1.0",
		Sources: map[string]func() sdk.SourceAction{
			"gen": func() sdk.SourceAction { return &source{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"discard": func() sdk.SinkAction { return &discard{} },
		},
		Schemas: map[string][]byte{
			"gen": []byte(genSchema),
			// discard takes no config; omit (builder shows a raw editor).
		},
	}
}

// genSchema is the JSON Schema (draft-07 subset) for SourceConfig.
const genSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "Synthetic source",
  "required": ["records"],
  "properties": {
    "records": {"type": "integer", "title": "Records to emit"},
    "groups": {"type": "integer", "title": "Distinct group cardinality", "default": 1000},
    "batch_records": {"type": "integer", "title": "Records per batch", "default": 1024},
    "delay_ms": {"type": "integer", "title": "Delay per batch (ms)", "default": 0}
  }
}`

// SourceConfig configures the gen source.
type SourceConfig struct {
	// Records to emit.
	Records int64 `json:"records"`
	// Groups is the distinct-region cardinality (default 1000).
	Groups int64 `json:"groups"`
	// BatchRecords sizes emitted batches (default 1024).
	BatchRecords int `json:"batch_records"`
	// DelayMS sleeps per batch — makes deliberately slow flows for
	// crash/drain testing (0 = full speed).
	DelayMS int `json:"delay_ms"`
}

type source struct {
	cfg   SourceConfig
	next  int64
	rng   uint64
	batch *record.Batch
}

func (s *source) Open(_ context.Context, config []byte) error {
	if err := json.Unmarshal(config, &s.cfg); err != nil {
		return fmt.Errorf("gen: bad config: %w", err)
	}
	if s.cfg.Records <= 0 {
		return errors.New("gen: records must be positive")
	}
	if s.cfg.Groups <= 0 {
		s.cfg.Groups = 1000
	}
	if s.cfg.BatchRecords <= 0 {
		s.cfg.BatchRecords = 1024
	}
	s.rng = 0x5DEECE66D
	s.batch = record.NewBatch()
	return nil
}

func (s *source) Next(ctx context.Context) (*record.Batch, error) {
	if s.next >= s.cfg.Records {
		return nil, io.EOF
	}
	if s.cfg.DelayMS > 0 {
		t := time.NewTimer(time.Duration(s.cfg.DelayMS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	var nameBuf, emailBuf, regionBuf []byte
	for range s.cfg.BatchRecords {
		if s.next >= s.cfg.Records {
			break
		}
		s.rng = s.rng*6364136223846793005 + 1442695040888963407
		r := s.rng
		id := s.next
		nameBuf = fmt.Appendf(nameBuf[:0], "customer-%06d", id%1_000_000)
		emailBuf = fmt.Appendf(emailBuf[:0], "user%d@example.com", id)
		regionBuf = fmt.Appendf(regionBuf[:0], "g%05d", r%uint64(s.cfg.Groups)) //nolint:gosec // groups validated positive

		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(id)
		bld.KeyLiteral("name")
		bld.String(nameBuf)
		bld.KeyLiteral("email")
		bld.String(emailBuf)
		bld.KeyLiteral("amount")
		bld.Float(float64(r%1_000_000) / 100.0)
		bld.KeyLiteral("active")
		bld.Bool(r&1 == 0)
		bld.KeyLiteral("region")
		bld.String(regionBuf)
		bld.KeyLiteral("address")
		bld.BeginMap()
		bld.KeyLiteral("city")
		bld.StringLiteral(cities[r%uint64(len(cities))])
		bld.KeyLiteral("postcode")
		bld.Int(int64(3000 + r%8000)) //nolint:gosec // small bounded value
		bld.EndMap()
		bld.EndMap()
		s.batch.Append(bld.Finish())
		s.next++
	}
	return s.batch, nil
}

func (s *source) Close() error { return nil }

var cities = []string{"Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide", "Hobart", "Darwin", "Canberra"}

// LocalSource is the gen source usable in-process (for parity
// benchmarking against the subprocess transport).
type LocalSource interface {
	Next(ctx context.Context) (*record.Batch, error)
	Close() error
}

// OpenLocal opens a gen source in-process with the given JSON config.
func OpenLocal(config []byte) (LocalSource, error) {
	s := &source{}
	if err := s.Open(context.Background(), config); err != nil {
		return nil, err
	}
	return s, nil
}

type discard struct {
	records int64
}

func (d *discard) Open(context.Context, []byte) error { return nil }

func (d *discard) Write(_ context.Context, b *record.Batch) error {
	d.records += int64(b.Len())
	return nil
}

func (d *discard) Close() error { return nil }
