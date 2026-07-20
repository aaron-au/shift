package service

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/runner/internal/task"
)

// captureSampler implements stream.Sampler: it keeps a bounded, redacted
// sample of the records leaving each flow step, for post-run inspection in
// test mode (M5c). The data is payload — it stays runner-side, is redacted
// like every error path (ADR-0010), and never reaches the hub.
//
// It is called synchronously on the pipeline goroutine (one at a time per
// task); the mutex only guards against a later concurrent reader.
type captureSampler struct {
	max    int
	redact func(string) string

	mu    sync.Mutex
	steps map[string]*stepAcc
	order []string
	tmp   *record.Batch // scratch for deep-copying sampled records
}

type stepAcc struct {
	records []json.RawMessage
	more    bool
}

func newCaptureSampler(max int, redact func(string) string) *captureSampler {
	if max <= 0 {
		max = 20
	}
	if redact == nil {
		redact = func(s string) string { return s }
	}
	return &captureSampler{max: max, redact: redact, steps: map[string]*stepAcc{}, tmp: record.NewBatch()}
}

// Sample records up to max records per step. Batches are reused downstream,
// so sampled records are deep-copied out immediately (record.CopyValue).
func (c *captureSampler) Sample(step string, b *record.Batch) {
	c.mu.Lock()
	defer c.mu.Unlock()

	acc := c.steps[step]
	if acc == nil {
		acc = &stepAcc{}
		c.steps[step] = acc
		c.order = append(c.order, step)
	}
	have := len(acc.records)
	if have >= c.max {
		acc.more = true
		return
	}
	take := b.Len()
	if have+take > c.max {
		take = c.max - have
		acc.more = true
	}

	c.tmp.Reset()
	for i := range take {
		c.tmp.Append(record.CopyValue(c.tmp, b.Record(i)))
	}
	var buf bytes.Buffer
	w := ndjson.NewWriter(&buf)
	if err := w.Write(context.Background(), c.tmp); err != nil {
		return // capture is best-effort — never fail or slow the task on it
	}
	if err := w.Close(); err != nil {
		return
	}
	// Redact at the serialized-text layer so every value (not just strings)
	// passes through the mask.
	text := c.redact(buf.String())
	for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
		if line != "" {
			acc.records = append(acc.records, json.RawMessage(line))
		}
	}
}

// result freezes the accumulated samples in stage order.
func (c *captureSampler) result() []task.StepCapture {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]task.StepCapture, 0, len(c.order))
	for _, id := range c.order {
		acc := c.steps[id]
		out = append(out, task.StepCapture{StepID: id, Records: acc.records, More: acc.more})
	}
	return out
}
