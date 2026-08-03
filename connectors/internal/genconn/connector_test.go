package genconn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

// TestConnectorDefinition pins the manifest the runner and the studio rely on:
// the gen verb is a source, discard is a sink, and the source ships a config
// schema the builder can render (ADR-0018).
func TestConnectorDefinition(t *testing.T) {
	c := Connector()
	if c.Name != "gen" || c.Version == "" {
		t.Fatalf("name/version = %q/%q", c.Name, c.Version)
	}
	mk, ok := c.Sources["gen"]
	if !ok {
		t.Fatal("no gen source")
	}
	if mk() == nil {
		t.Fatal("gen source factory returned nil")
	}
	mkSink, ok := c.Sinks["discard"]
	if !ok {
		t.Fatal("no discard sink")
	}
	if mkSink() == nil {
		t.Fatal("discard sink factory returned nil")
	}
	var schema struct {
		Type       string   `json:"type"`
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(c.Schemas["gen"], &schema); err != nil {
		t.Fatalf("gen schema is not valid JSON: %v", err)
	}
	if schema.Type != "object" || len(schema.Required) != 1 || schema.Required[0] != "records" {
		t.Fatalf("schema = %+v, want object requiring records", schema)
	}
	for _, field := range []string{"records", "groups", "batch_records", "delay_ms"} {
		if _, ok := schema.Properties[field]; !ok {
			t.Errorf("schema omits %q, which SourceConfig accepts", field)
		}
	}
}

// TestSourceDelayIsInterruptible: delay_ms exists to make deliberately slow
// flows for drain/crash testing, so the wait must abort on cancellation rather
// than pin a runner goroutine for its full duration.
func TestSourceDelayIsInterruptible(t *testing.T) {
	s := &source{}
	if err := s.Open(context.Background(), []byte(`{"records":10,"delay_ms":60000}`)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := s.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancelled Next took %v, want an immediate return", elapsed)
	}
}

func TestSourceDelayStillEmits(t *testing.T) {
	s := &source{}
	if err := s.Open(context.Background(), []byte(`{"records":2,"batch_records":1,"delay_ms":1}`)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 2 {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
		if b.Len() != 1 {
			t.Fatalf("batch %d has %d records, want 1", i, b.Len())
		}
	}
	if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after exhaustion = %v, want io.EOF", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// TestOpenLocal covers the in-process entry point used for transport-parity
// benchmarking: same config contract, same record stream, same EOF.
func TestOpenLocal(t *testing.T) {
	if _, err := OpenLocal([]byte(`{"records":0}`)); err == nil {
		t.Fatal("OpenLocal accepted records=0, want the source's validation error")
	}
	if _, err := OpenLocal([]byte(`not json`)); err == nil {
		t.Fatal("OpenLocal accepted malformed config")
	}

	ls, err := OpenLocal([]byte(`{"records":300,"groups":4,"batch_records":128}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var n int64
	for {
		b, err := ls.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range b.Records() {
			id, ok := rec.Field("id")
			if !ok || id.Int() != n {
				t.Fatalf("record %d has id %v", n, id)
			}
			n++
		}
	}
	if n != 300 {
		t.Fatalf("OpenLocal emitted %d records, want 300", n)
	}
	if err := ls.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestDiscardCloseIsClean(t *testing.T) {
	d := &discard{}
	if err := d.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}
