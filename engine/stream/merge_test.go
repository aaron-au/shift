package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// oneRecReader builds an ndjson source that emits one record per batch, so
// concat interleaving is observable.
func oneRecReader(s string) Source {
	return ndjson.NewReader(strings.NewReader(s), ndjson.ReaderOptions{BatchRecords: 1})
}

func drainConcat(t *testing.T, inputs ...Source) string {
	t.Helper()
	p := New(Concat(inputs...), "merge")
	var out bytes.Buffer
	if _, err := p.Run(context.Background(), ndjson.NewWriter(&out), "write"); err != nil {
		t.Fatalf("run: %v", err)
	}
	return out.String()
}

// Round-robin interleave, one record per batch, even lengths.
func TestConcatInterleaveEven(t *testing.T) {
	got := drainConcat(t,
		oneRecReader(`{"s":"a1"}`+"\n"+`{"s":"a2"}`+"\n"),
		oneRecReader(`{"s":"b1"}`+"\n"+`{"s":"b2"}`+"\n"),
	)
	want := `{"s":"a1"}` + "\n" + `{"s":"b1"}` + "\n" + `{"s":"a2"}` + "\n" + `{"s":"b2"}` + "\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

// When one input drains early, the remaining input continues alone.
func TestConcatUnevenLengths(t *testing.T) {
	got := drainConcat(t,
		oneRecReader(`{"s":"a1"}`+"\n"+`{"s":"a2"}`+"\n"+`{"s":"a3"}`+"\n"),
		oneRecReader(`{"s":"b1"}`+"\n"),
	)
	want := `{"s":"a1"}` + "\n" + `{"s":"b1"}` + "\n" + `{"s":"a2"}` + "\n" + `{"s":"a3"}` + "\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

// Three inputs union every record exactly once (count check, order aside).
func TestConcatThreeInputsUnion(t *testing.T) {
	got := drainConcat(t,
		oneRecReader(`{"s":"a"}`+"\n"),
		oneRecReader(`{"s":"b"}`+"\n"),
		oneRecReader(`{"s":"c"}`+"\n"),
	)
	for _, want := range []string{`{"s":"a"}`, `{"s":"b"}`, `{"s":"c"}`} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in %q", want, got)
		}
	}
	if n := strings.Count(got, "\n"); n != 3 {
		t.Fatalf("record count = %d, want 3", n)
	}
}

func TestConcatSinglePassthrough(t *testing.T) {
	got := drainConcat(t, oneRecReader(`{"s":"only"}`+"\n"))
	if got != `{"s":"only"}`+"\n" {
		t.Fatalf("got %q", got)
	}
}

func TestConcatEmptyInputs(t *testing.T) {
	c := Concat()
	if _, err := c.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("empty concat Next = %v, want EOF", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("empty concat Close = %v", err)
	}
}

// fakeSource emits nothing and can return a hard error and/or a Close error,
// and counts Close calls — for the error and teardown paths.
type fakeSource struct {
	nextErr  error
	closeErr error
	closed   int
}

func (f *fakeSource) Next(context.Context) (*record.Batch, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	return nil, io.EOF
}
func (f *fakeSource) Close() error { f.closed++; return f.closeErr }

// A hard (non-EOF) error from an input surfaces immediately.
func TestConcatPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	f0 := &fakeSource{nextErr: boom}
	f1 := &fakeSource{}
	c := Concat(f0, f1)
	_, err := c.Next(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Next err = %v, want boom", err)
	}
	// Close still tears down every input.
	if err := c.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if f0.closed != 1 || f1.closed != 1 {
		t.Fatalf("closed counts = %d,%d, want 1,1", f0.closed, f1.closed)
	}
}

// Close attempts every input and returns the first error.
func TestConcatCloseAggregatesErrors(t *testing.T) {
	e := errors.New("close-fail")
	f0 := &fakeSource{}
	f1 := &fakeSource{closeErr: e}
	f2 := &fakeSource{}
	c := Concat(f0, f1, f2)
	if err := c.Close(); !errors.Is(err, e) {
		t.Fatalf("Close = %v, want close-fail", err)
	}
	if f0.closed != 1 || f1.closed != 1 || f2.closed != 1 {
		t.Fatalf("not all closed: %d,%d,%d", f0.closed, f1.closed, f2.closed)
	}
}
