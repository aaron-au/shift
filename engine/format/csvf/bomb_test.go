package csvf

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// TC-019 (docs/assurance/test-conformance.md §2e).
//
// encoding/csv has no size limit. An opening quote that is never closed makes
// it read to EOF hunting for the closing one, so one field grows to the size of
// the input — and in an iPaaS the input is bytes a connector fetched from a
// system nobody here controls. Measured before the fix: 64 MiB of an unclosed
// quoted field was buffered, and the reader only failed at column 67108866 —
// because the INPUT ran out, not because anything stopped it. Against a source
// that keeps trickling, it grows without bound.
//
// Same shape as the EDI bomb TC-003 found: the streaming architecture bounds
// batches, so the bombs that get through are the ones where a single unit is
// unbounded.
func TestAnUnclosedQuotedFieldIsRefusedInsteadOfBufferingForever(t *testing.T) {
	const limit = 1 << 20

	// endless never returns EOF, which is the case that matters: with a finite
	// file the reader eventually stops on its own and the bug looks survivable.
	endless := readerFunc(func(p []byte) (int, error) {
		for i := range p {
			p[i] = 'A'
		}
		return len(p), nil
	})
	in := io.MultiReader(strings.NewReader("a,b\n\""), endless)

	r := NewReader(in, ReaderOptions{MaxRecordBytes: limit})
	_, err := r.Next(context.Background())
	if err == nil {
		t.Fatal("an endless quoted field was accepted: the reader will buffer until the runner dies")
	}
	if !errors.Is(err, ErrRecordTooLong) {
		t.Fatalf("err = %v, want ErrRecordTooLong", err)
	}
}

// The bound is per ROW. A long file of ordinary rows must stream forever, or
// the fix for the bomb has broken every legitimate large CSV — which would be
// a worse bug than the one it closed.
func TestManyOrdinaryRowsAreNotBoundedInAggregate(t *testing.T) {
	const rows = 20000
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for range rows {
		sb.WriteString(strings.Repeat("x", 40))
		sb.WriteByte(',')
		sb.WriteString(strings.Repeat("y", 40))
		sb.WriteByte('\n')
	}
	// A budget far smaller than the whole file, comfortably larger than a row.
	r := NewReader(strings.NewReader(sb.String()), ReaderOptions{MaxRecordBytes: 8 << 10})

	var got int
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("a file of ordinary rows was refused after %d rows: %v", got, err)
		}
		got += b.Len()
	}
	if got != rows {
		t.Errorf("read %d rows, want %d", got, rows)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
