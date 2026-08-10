package spill

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// TestTwoWritersInterleaveWithoutCorruptingEitherSegment is the property
// TC-029 needs: a topology can require two operators to spill at the same time
// (a tee feeding both sides of a join buffers the probe WHILE the join builds),
// and neither may wait for the other to finish.
func TestTwoWritersInterleaveWithoutCorruptingEitherSegment(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	a, b := s.NewWriter(), s.NewWriter()
	var wantA, wantB bytes.Buffer

	// Alternate deliberately, so the two segments genuinely interleave in the
	// file rather than happening to land in two contiguous runs.
	for i := range 200 {
		pa := fmt.Appendf(nil, "A%06d-", i)
		pb := fmt.Appendf(nil, "B%06d=", i)
		if _, err := a.Write(pa); err != nil {
			t.Fatal(err)
		}
		wantA.Write(pa)
		if _, err := b.Write(pb); err != nil {
			t.Fatal(err)
		}
		wantB.Write(pb)
	}

	segA, err := a.Finish()
	if err != nil {
		t.Fatal(err)
	}
	segB, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	if len(segA.Extents) < 2 || len(segB.Extents) < 2 {
		t.Fatalf("segments did not interleave (A %d extents, B %d): the test is not exercising the property",
			len(segA.Extents), len(segB.Extents))
	}

	gotA, err := io.ReadAll(s.OpenSegment(segA))
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := io.ReadAll(s.OpenSegment(segB))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotA, wantA.Bytes()) {
		t.Fatalf("segment A read back %d bytes, want %d — interleaved extents were reassembled wrongly", len(gotA), wantA.Len())
	}
	if !bytes.Equal(gotB, wantB.Bytes()) {
		t.Fatalf("segment B read back %d bytes, want %d", len(gotB), wantB.Len())
	}
}

// TestAnUncontendedWriterStillProducesOneContiguousExtent. Every segment
// written before TC-029 was one contiguous range, and the readers, the codec
// and the existing tests all assume Off/Len describe the whole thing. Extents
// must therefore stay an exception, not become the norm.
func TestAnUncontendedWriterStillProducesOneContiguousExtent(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	w := s.NewWriter()
	for range 50 {
		if _, err := w.Write([]byte(strings.Repeat("x", 1000))); err != nil {
			t.Fatal(err)
		}
	}
	seg, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if seg.Extents != nil {
		t.Fatalf("an uncontended segment carries %d extents; adjacent writes must coalesce", len(seg.Extents))
	}
	if seg.Off != 0 || seg.Len != 50000 {
		t.Fatalf("segment = {Off:%d Len:%d}, want the whole contiguous range {0, 50000}", seg.Off, seg.Len)
	}
}

// TestConcurrentWritersAreRaceFree drives the writers from separate goroutines
// so -race has something to look at; the offset reservation is the shared state.
func TestConcurrentWritersAreRaceFree(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	const writers, writes = 8, 100
	segs := make([]Segment, writers)
	wants := make([][]byte, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			w := s.NewWriter()
			var want bytes.Buffer
			for j := range writes {
				p := fmt.Appendf(nil, "w%02d-%04d;", i, j)
				if _, err := w.Write(p); err != nil {
					t.Errorf("writer %d: %v", i, err)
					return
				}
				want.Write(p)
			}
			seg, err := w.Finish()
			if err != nil {
				t.Errorf("writer %d finish: %v", i, err)
				return
			}
			segs[i], wants[i] = seg, want.Bytes()
		})
	}
	wg.Wait()

	for i := range writers {
		got, err := io.ReadAll(s.OpenSegment(segs[i]))
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
		if !bytes.Equal(got, wants[i]) {
			t.Fatalf("writer %d read back %d bytes, want %d", i, len(got), len(wants[i]))
		}
	}
	if got, want := len(s.Segments()), writers; got != want {
		t.Fatalf("store sealed %d segments, want %d", got, want)
	}
}

func TestAFinishedWriterRefusesFurtherWrites(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	w := s.NewWriter()
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("y")); err == nil {
		t.Fatal("a finished segment accepted more bytes; they would belong to no segment and corrupt the next one's offsets")
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("Finish twice succeeded; the segment would be sealed into the inventory twice")
	}
}

// TestAnEmptySegmentReportsTheCurrentEnd preserves the pre-TC-029 meaning of a
// zero-length segment, which the store inventory test depends on.
func TestAnEmptySegmentReportsTheCurrentEnd(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	first := s.NewWriter()
	if _, err := first.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Finish(); err != nil {
		t.Fatal(err)
	}

	empty, err := s.NewWriter().Finish()
	if err != nil {
		t.Fatal(err)
	}
	if empty.Off != 8 || empty.Len != 0 {
		t.Fatalf("empty segment = {Off:%d Len:%d}, want {8, 0}", empty.Off, empty.Len)
	}
	got, err := io.ReadAll(s.OpenSegment(empty))
	if err != nil || len(got) != 0 {
		t.Fatalf("reading an empty segment = (%q, %v), want (empty, nil)", got, err)
	}
}
