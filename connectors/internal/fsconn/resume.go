package fsconn

import (
	"context"
	"encoding/json"
	"fmt"
)

// Resumption for the `get` source (ADR-0037). The cursor is a record ordinal:
// how many records the previous attempt emitted and had confirmed downstream.
//
// An ordinal rather than a byte offset, because the format readers buffer —
// the file position after a batch is wherever the scanner happened to fill to,
// not the logical end of the last record, so seeking to it would land
// mid-record. Skipping N records re-parses the prefix, which costs read
// bandwidth but never mis-positions. Re-reading is far cheaper than re-writing
// to the sink, which is the work resume exists to avoid. A byte offset is a
// later optimisation, and needs the readers to report consumed bytes first.
//
// The cursor also fingerprints the file (size + modification time) and pins the
// path. A record ordinal against a file that changed between attempts resumes
// at the wrong place — silently, and with no way for the runner to notice — so
// a changed fingerprint refuses to resume. The runner then falls back to a full
// replay: slower, and correct.

const fsCursorVersion = 1

type fsCursor struct {
	V     int    `json:"v"`
	Path  string `json:"path"`
	N     int64  `json:"n"`     // records emitted and confirmed
	Size  int64  `json:"size"`  // file fingerprint, guarding against a changed file
	MTime int64  `json:"mtime"` // unix nanoseconds
}

// Resume positions the stream after the first N records, provided the cursor
// refers to this action's file and that file has not changed.
func (s *getSource) Resume(_ context.Context, cur []byte) error {
	if len(cur) == 0 {
		return nil // "from the beginning" — identical to not resuming
	}
	var c fsCursor
	if err := json.Unmarshal(cur, &c); err != nil {
		return fmt.Errorf("fs: malformed resume cursor: %w", err)
	}
	if c.V != fsCursorVersion {
		return fmt.Errorf("fs: resume cursor version %d, want %d", c.V, fsCursorVersion)
	}
	if c.Path != s.cfg.Path {
		// The node's config can be edited between attempts. Resuming a count
		// taken against one file into a different file would skip an arbitrary
		// prefix of it.
		return fmt.Errorf("fs: resume cursor is for %q but this action reads %q", c.Path, s.cfg.Path)
	}
	if c.N < 0 {
		return fmt.Errorf("fs: resume cursor has a negative record count (%d)", c.N)
	}
	fi, err := s.f.Stat()
	if err != nil {
		return fmt.Errorf("fs: stat for resume: %w", err)
	}
	if fi.Size() != c.Size || fi.ModTime().UnixNano() != c.MTime {
		return fmt.Errorf("fs: %q changed since the cursor was taken (size %d→%d); replaying from the start is the only safe option",
			s.cfg.Path, c.Size, fi.Size())
	}
	s.skip, s.emitted = c.N, c.N
	return nil
}

// Checkpoint reports the records emitted so far. Safety comes from the runner,
// which persists a checkpoint only once the terminal sink has confirmed the
// records it covers — so reporting every batch here is free of risk.
func (s *getSource) Checkpoint() []byte {
	if s.f == nil {
		return nil
	}
	fi, err := s.f.Stat()
	if err != nil {
		return nil // no fingerprint, no safe cursor
	}
	cur, err := json.Marshal(fsCursor{
		V: fsCursorVersion, Path: s.cfg.Path, N: s.emitted,
		Size: fi.Size(), MTime: fi.ModTime().UnixNano(),
	})
	if err != nil {
		return nil
	}
	return cur
}
