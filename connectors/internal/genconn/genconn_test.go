package genconn

import (
	"context"
	"errors"
	"io"
	"testing"
)

func drain(t *testing.T, s *source) (n int64, firstRegion string) {
	t.Helper()
	ctx := context.Background()
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			return n, firstRegion
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 && b.Len() > 0 {
			r, _ := b.Record(0).Field("region")
			firstRegion = r.String()
		}
		for _, rec := range b.Records() {
			id, ok := rec.Field("id")
			if !ok || id.Int() != n {
				t.Fatalf("record %d has id %v", n, id)
			}
			addr, ok := rec.Field("address")
			if !ok || addr.Len() != 2 {
				t.Fatalf("record %d bad address", n)
			}
			n++
		}
	}
}

func TestSourceDeterministicAndComplete(t *testing.T) {
	mk := func() *source {
		s := &source{}
		if err := s.Open(context.Background(), []byte(`{"records":2500,"groups":10,"batch_records":100}`)); err != nil {
			t.Fatal(err)
		}
		return s
	}
	n1, r1 := drain(t, mk())
	n2, r2 := drain(t, mk())
	if n1 != 2500 || n2 != 2500 {
		t.Fatalf("counts %d/%d", n1, n2)
	}
	if r1 != r2 || r1 == "" {
		t.Fatalf("not deterministic: %q vs %q", r1, r2)
	}
}

func TestSourceConfigValidation(t *testing.T) {
	s := &source{}
	if err := s.Open(context.Background(), []byte(`{"records":0}`)); err == nil {
		t.Fatal("want error for records=0")
	}
	if err := s.Open(context.Background(), []byte(`not json`)); err == nil {
		t.Fatal("want error for bad json")
	}
}

func TestDiscardCounts(t *testing.T) {
	s := &source{}
	if err := s.Open(context.Background(), []byte(`{"records":500}`)); err != nil {
		t.Fatal(err)
	}
	d := &discard{}
	if err := d.Open(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Write(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if d.records != 500 {
		t.Fatalf("discard counted %d", d.records)
	}
}
