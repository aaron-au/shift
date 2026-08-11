// Package spill provides the engine's scratch store: a single unlinked
// temporary file that stateful operators overflow into when the memory
// governor says stop. One file, append-only segments, automatic cleanup —
// never a directory of small files (ADR-0002/0003 doctrine).
package spill

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// Store is a segment-oriented scratch file.
//
// Concurrency (added for TC-029): several writers may fill segments at the
// same time, because a topology can require two stateful operators to spill
// concurrently. The enrichment shape is the motivating case — a tee feeding
// both sides of a join means the probe branch must be buffered WHILE the join
// builds, so neither can wait for the other to finish spilling.
//
// That is why a segment is a list of extents rather than one contiguous range:
// interleaved writers cannot each own a contiguous span of one file, and the
// alternative — a file per writer — is the directory of small files the
// doctrine exists to prevent. Adjacent writes coalesce, so a segment written
// without competition is still exactly one extent and costs nothing.
//
// Readers via OpenSegment use ReadAt and may overlap with each other and with
// writers, since sealed extents are never rewritten.
type Store struct {
	f *os.File

	mu   sync.Mutex
	off  int64
	segs []Segment

	// legacy backs the single-open-segment StartSegment/FinishSegment API.
	legacy    *Writer
	legacyBuf *bufio.Writer
}

// Extent is one contiguous run of bytes belonging to a segment.
type Extent struct {
	Off int64
	Len int64
}

// Segment identifies one spilled segment.
//
// Off and Len describe the whole segment when it is contiguous, which is the
// common case and what every caller before TC-029 produced. Extents is non-nil
// only when the segment was interleaved with another writer's, in which case
// Off is the first extent's offset and Len is the TOTAL byte count.
type Segment struct {
	ID      int
	Off     int64
	Len     int64
	Extents []Extent
}

// NewStore creates the scratch file in dir (or the OS temp dir when dir is
// empty). The file is unlinked immediately, so it disappears on Close or
// process death.
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	f, err := os.CreateTemp(dir, "shift-spill-*")
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	// Unlink now: the fd keeps the extent alive; nothing is left behind no
	// matter how the process exits.
	if err := os.Remove(f.Name()); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("spill: unlink scratch: %w", err)
	}
	return &Store{f: f}, nil
}

// NewWriter opens an independent segment. Any number may be open at once; each
// seals into its own Segment via Finish.
//
// The writer does no buffering of its own: callers already wrap it in a bufio
// of their chosen size, and a second layer would only add a copy. Every Write
// therefore reaches the file, so write in reasonable chunks.
func (s *Store) NewWriter() *Writer { return &Writer{s: s} }

// Writer fills one segment. It is safe to use concurrently with other Writers
// on the same Store; a single Writer is used by one goroutine at a time, like
// any io.Writer.
type Writer struct {
	s       *Store
	extents []Extent
	done    bool
}

// Write appends p to the store and records where it landed.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("spill: write to a finished segment")
	}
	if len(p) == 0 {
		return 0, nil
	}
	w.s.mu.Lock()
	off := w.s.off
	w.s.off += int64(len(p))
	w.s.mu.Unlock()

	// The offset is reserved under the lock but the write itself is not: two
	// writers never target the same bytes, so letting them write in parallel
	// costs nothing and keeps the lock off the I/O path.
	n, err := w.s.f.WriteAt(p, off)
	if n > 0 {
		// Coalesce with the previous extent when this write continued it. A
		// writer with no competition therefore produces exactly ONE extent,
		// which keeps every pre-TC-029 segment byte-for-byte what it was.
		if k := len(w.extents); k > 0 && w.extents[k-1].Off+w.extents[k-1].Len == off {
			w.extents[k-1].Len += int64(n)
		} else {
			w.extents = append(w.extents, Extent{Off: off, Len: int64(n)})
		}
	}
	if err != nil {
		return n, fmt.Errorf("spill: write: %w", err)
	}
	return n, nil
}

// Finish seals the segment and returns its handle.
func (w *Writer) Finish() (Segment, error) {
	if w.done {
		return Segment{}, errors.New("spill: segment already finished")
	}
	w.done = true

	var total int64
	for _, e := range w.extents {
		total += e.Len
	}
	seg := Segment{Len: total}
	// Only carry the extent list when it actually says something the Off/Len
	// pair cannot.
	if len(w.extents) > 1 {
		seg.Extents = w.extents
	}

	w.s.mu.Lock()
	if len(w.extents) > 0 {
		seg.Off = w.extents[0].Off
	} else {
		// An empty segment has no extent to take an offset from. Report the
		// current end of the store, which is what a zero-length contiguous
		// segment always meant.
		seg.Off = w.s.off
	}
	seg.ID = len(w.s.segs)
	w.s.segs = append(w.s.segs, seg)
	w.s.mu.Unlock()
	return seg, nil
}

// StartSegment begins a new segment and returns the writer to fill it.
// Only one such segment may be open at a time; call FinishSegment before
// starting the next. Concurrent spillers use NewWriter instead.
func (s *Store) StartSegment() (*bufio.Writer, error) {
	s.mu.Lock()
	open := s.legacy != nil
	s.mu.Unlock()
	if open {
		return nil, errors.New("spill: segment already open")
	}
	w := s.NewWriter()
	buf := bufio.NewWriterSize(w, 256<<10)

	s.mu.Lock()
	s.legacy, s.legacyBuf = w, buf
	s.mu.Unlock()
	return buf, nil
}

// FinishSegment flushes and seals the segment opened by StartSegment.
func (s *Store) FinishSegment() (Segment, error) {
	s.mu.Lock()
	w, buf := s.legacy, s.legacyBuf
	s.legacy, s.legacyBuf = nil, nil
	s.mu.Unlock()

	if w == nil {
		return Segment{}, errors.New("spill: no open segment")
	}
	if err := buf.Flush(); err != nil {
		return Segment{}, fmt.Errorf("spill: flush: %w", err)
	}
	return w.Finish()
}

// OpenSegment returns a reader over a sealed segment.
func (s *Store) OpenSegment(seg Segment) io.Reader {
	if len(seg.Extents) == 0 {
		return io.NewSectionReader(s.f, seg.Off, seg.Len)
	}
	rs := make([]io.Reader, len(seg.Extents))
	for i, e := range seg.Extents {
		rs[i] = io.NewSectionReader(s.f, e.Off, e.Len)
	}
	return io.MultiReader(rs...)
}

// BytesWritten reports total spill volume.
func (s *Store) BytesWritten() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.off
}

// Segments returns all sealed segments.
func (s *Store) Segments() []Segment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Segment(nil), s.segs...)
}

// Close releases the scratch file (already unlinked).
func (s *Store) Close() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
