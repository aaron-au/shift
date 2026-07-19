package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aaron-au/shift/engine/stream"
)

// runBaseline is the naive buffered implementation: decode the entire input
// into []map[string]any, transform, marshal everything back out. This is
// the memory/allocation profile the streaming engine exists to avoid —
// quantified, not assumed.
func runBaseline(ctx context.Context, in io.Reader) (stream.Report, error) {
	start := time.Now()

	// Ingest everything.
	var rows []map[string]any
	dec := json.NewDecoder(in)
	for {
		if err := ctx.Err(); err != nil {
			return stream.Report{}, err
		}
		var m map[string]any
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return stream.Report{}, fmt.Errorf("baseline: %w", err)
		}
		rows = append(rows, m)
	}
	total := int64(len(rows))

	// Transform: filter active, project the same fields as the streaming
	// transform scenario.
	out := make([]map[string]any, 0, len(rows))
	for _, m := range rows {
		if active, _ := m["active"].(bool); !active {
			continue
		}
		city := any(nil)
		if addr, ok := m["address"].(map[string]any); ok {
			city = addr["city"]
		}
		out = append(out, map[string]any{
			"id":     m["id"],
			"name":   m["name"],
			"city":   city,
			"amount": m["amount"],
			"region": m["region"],
		})
	}

	// Serialize everything.
	var written int64
	for _, m := range out {
		b, err := json.Marshal(m)
		if err != nil {
			return stream.Report{}, err
		}
		written += int64(len(b)) + 1
	}

	return stream.Report{
		RecordsOut: int64(len(out)),
		WallNanos:  time.Since(start).Nanoseconds(),
		Ops: []stream.OpStats{
			{Name: "baseline-buffered", RecordsIn: total, RecordsOut: int64(len(out)), Nanos: time.Since(start).Nanoseconds()},
		},
	}, nil
}
