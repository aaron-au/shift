package ndjson

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// TC-019 (docs/assurance/test-conformance.md §2e).
//
// JSONReader hands the stream to encoding/json, which materialises one whole
// top-level value into a json.RawMessage before anything else gets a say. A
// pretty-printed document has no lines, so MaxLineBytes could not apply and a
// single value was the unbounded unit. Measured: 521,845 gzipped wire bytes
// expanded to one JSON value produced 2,561 MiB of peak heap — a runner OOM
// bought for half a megabyte of upload.
//
// This is the reader the http connector uses for JSON-array REST APIs, so the
// hostile source is any endpoint a flow points at.
func TestASingleEnormousValueIsRefusedInsteadOfBufferingForever(t *testing.T) {
	const limit = 1 << 20

	// An array whose first element is an endless string: no line breaks, and
	// the value never terminates, so nothing but a byte budget can stop it.
	endless := readerFunc(func(p []byte) (int, error) {
		for i := range p {
			p[i] = 'A'
		}
		return len(p), nil
	})
	in := io.MultiReader(strings.NewReader(`[{"blob":"`), endless)

	r := NewJSONReader(in, ReaderOptions{MaxLineBytes: limit})
	_, err := r.Next(context.Background())
	if err == nil {
		t.Fatal("an endless JSON value was accepted: the reader will buffer until the runner dies")
	}
	if !errors.Is(err, ErrValueTooLong) {
		t.Fatalf("err = %v, want ErrValueTooLong", err)
	}
}

// The bound is per VALUE. A long array of ordinary elements must stream, or the
// fix has broken every legitimate JSON API response — a worse bug than the one
// it closed.
func TestALongArrayOfOrdinaryValuesIsNotBoundedInAggregate(t *testing.T) {
	const n = 20000
	var sb strings.Builder
	sb.WriteByte('[')
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":`)
		sb.WriteString(strings.Repeat("7", 6))
		sb.WriteString(`,"pad":"` + strings.Repeat("z", 60) + `"}`)
	}
	sb.WriteByte(']')

	r := NewJSONReader(strings.NewReader(sb.String()), ReaderOptions{MaxLineBytes: 8 << 10})
	var got int
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("an ordinary array was refused after %d elements: %v", got, err)
		}
		got += b.Len()
	}
	if got != n {
		t.Errorf("read %d elements, want %d", got, n)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
