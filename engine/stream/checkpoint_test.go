package stream

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// cursorSource emits n single-record batches and reports its position as a
// decimal count. failAt >= 0 fails the source at that ordinal.
type cursorSource struct {
	n, next int
	batch   *record.Batch
}

func newCursorSource(n int) *cursorSource {
	return &cursorSource{n: n, batch: record.NewBatch()}
}

func (s *cursorSource) Next(context.Context) (*record.Batch, error) {
	if s.next >= s.n {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(int64(s.next))
	bld.EndMap()
	s.batch.Append(bld.Finish())
	s.next++
	return s.batch, nil
}

func (s *cursorSource) Close() error       { return nil }
func (s *cursorSource) Checkpoint() []byte { return []byte(strconv.Itoa(s.next)) }

// countingSink accepts batches, optionally failing after k of them.
type countingSink struct {
	n, failAfter int
}

func (c *countingSink) Write(context.Context, *record.Batch) error {
	if c.failAfter > 0 && c.n >= c.failAfter {
		return errors.New("sink refused")
	}
	c.n++
	return nil
}
func (c *countingSink) Close() error { return nil }

// The confirm point is the whole safety property: a position must only be
// reported once the SINK has taken the batch it covers.
func TestCheckpointReportedOnlyAfterTheSinkConfirms(t *testing.T) {
	src := newCursorSource(4)
	var seen []string
	p := New(src, "src").WithCheckpoint(func(cur []byte) { seen = append(seen, string(cur)) })
	if _, err := p.Run(t.Context(), &countingSink{}, "sink"); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"1", "2", "3", "4"}
	if len(seen) != len(want) {
		t.Fatalf("checkpoints = %v, want one per confirmed batch %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("checkpoints = %v, want %v", seen, want)
		}
	}
}

// A batch the sink REJECTED must not advance the position. Reporting it would
// record progress for records that were read but never written — the silent
// data loss resume exists to avoid.
func TestCheckpointDoesNotAdvancePastAFailedWrite(t *testing.T) {
	src := newCursorSource(5)
	var last string
	p := New(src, "src").WithCheckpoint(func(cur []byte) { last = string(cur) })
	if _, err := p.Run(t.Context(), &countingSink{failAfter: 2}, "sink"); err == nil {
		t.Fatal("expected the sink failure to surface")
	}
	if last != "2" {
		t.Fatalf("last checkpoint = %q, want %q — the third batch was never written", last, "2")
	}
}

// A source with no resume capability must cost nothing and report nothing.
func TestCheckpointIsANoOpForANonResumableSource(t *testing.T) {
	called := false
	// Wrapping in a struct embedding only Source strips the optional method.
	plain := &struct{ Source }{newCursorSource(3)}
	p := New(plain, "src").WithCheckpoint(func([]byte) { called = true })
	if _, err := p.Run(t.Context(), &countingSink{}, "sink"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if called {
		t.Fatal("a non-resumable source reported a checkpoint")
	}
}

// Running without WithCheckpoint must not touch the source's Checkpoint at
// all — resume is opt-in and costs nothing when unused.
func TestNoCallbackMeansNoCheckpointing(t *testing.T) {
	src := newCursorSource(3)
	if _, err := New(src, "src").Run(t.Context(), &countingSink{}, "sink"); err != nil {
		t.Fatalf("run: %v", err)
	}
}
