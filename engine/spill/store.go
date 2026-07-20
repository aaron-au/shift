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
)

// Store is a segment-oriented scratch file. It is not safe for concurrent
// writers (engine pipelines are single-goroutine); readers via OpenSegment
// use ReadAt and may overlap with each other.
type Store struct {
	f       *os.File
	off     int64
	segs    []Segment
	w       *bufio.Writer
	writing bool
}

// Segment identifies one spilled extent.
type Segment struct {
	ID  int
	Off int64
	Len int64
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
	return &Store{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

// StartSegment begins a new segment and returns the writer to fill it.
// Only one segment may be open at a time; call FinishSegment before
// starting the next.
func (s *Store) StartSegment() (*bufio.Writer, error) {
	if s.writing {
		return nil, errors.New("spill: segment already open")
	}
	s.writing = true
	return s.w, nil
}

// FinishSegment flushes and seals the open segment, returning its handle.
func (s *Store) FinishSegment() (Segment, error) {
	if !s.writing {
		return Segment{}, errors.New("spill: no open segment")
	}
	if err := s.w.Flush(); err != nil {
		return Segment{}, fmt.Errorf("spill: flush: %w", err)
	}
	end, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return Segment{}, fmt.Errorf("spill: %w", err)
	}
	seg := Segment{ID: len(s.segs), Off: s.off, Len: end - s.off}
	s.segs = append(s.segs, seg)
	s.off = end
	s.writing = false
	return seg, nil
}

// OpenSegment returns a reader over a sealed segment.
func (s *Store) OpenSegment(seg Segment) *io.SectionReader {
	return io.NewSectionReader(s.f, seg.Off, seg.Len)
}

// BytesWritten reports total sealed spill volume.
func (s *Store) BytesWritten() int64 { return s.off }

// Segments returns all sealed segments.
func (s *Store) Segments() []Segment { return s.segs }

// Close releases the scratch file (already unlinked).
func (s *Store) Close() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
