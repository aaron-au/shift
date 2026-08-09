package ftpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/jlaffaye/ftp"
)

// listBatch caps how many directory entries one Next emits.
const listBatch = 512

// listSource lists a remote directory, emitting one record per entry:
// {name, path, size, type, mod_time}. The directory is read once at Open (the
// connection closes immediately after) and iterated from memory.
type listSource struct {
	dial    dialFunc // nil ⇒ realDial (test seam)
	cfg     config
	dir     string
	entries []*ftp.Entry
	idx     int
	batch   *record.Batch
}

func (s *listSource) Open(ctx context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireDir(); err != nil {
		return err
	}
	conn, closer, err := dialOr(s.dial)(ctx, &s.cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closer() }() // listing is one-shot; don't hold the connection
	entries, err := conn.List(s.cfg.Path)
	if err != nil {
		return err
	}
	// Vet every name the server sent BEFORE any of them is emitted, so a
	// hostile entry fails the whole listing rather than being discovered
	// half-way through a downstream delete loop.
	for _, e := range entries {
		if err := checkEntryName(e.Name); err != nil {
			return err
		}
	}
	s.dir, s.entries, s.batch = s.cfg.Path, entries, record.NewBatch()
	return nil
}

// checkEntryName refuses a directory-entry name that is not a single, safe path
// component.
//
// The name comes from the REMOTE server — in an iPaaS that is a trading
// partner's system, not ours, and the names in it are chosen by whoever can
// write there. list emits {name, path} and the `path` field exists to be fed
// into a following get/delete/rename node, so a name of "../../etc/passwd" is
// not data: path.Join CLEANS it and the emitted path leaves the directory the
// author listed. The flow then acts on that file with this connector's
// credentials. That is the zip-slip shape, and FTP is where it is easiest to
// reach: entry names are parsed out of free-form LIST output, so "/" and ".."
// survive the parse intact.
//
// Refused: empty, "." and "..", anything containing "/", and NUL or C0/DEL
// control characters. Control characters are refused because a name that
// cannot be logged or correlated is an incident-response problem in its own
// right, and because feeding one back into a verb is exactly the FTP command
// injection validateRemotePath exists to stop.
//
// Nothing a real server legitimately produces is lost: spaces, unicode in any
// normalisation (including RTL overrides and homoglyphs), leading "-", leading
// and trailing dots and spaces, and names up to the server's own length limit
// all still pass. Display-spoofing names are deliberately ALLOWED through —
// they are an audit-trail hazard, not a traversal, and refusing every name that
// merely looks confusing would reject legitimate non-Latin filenames.
//
// The listing FAILS rather than skipping the entry. Skipping would let a flow
// process 999 of 1000 files and report success; rewriting the name would make
// the flow act on a file the operator cannot trace back to the listing.
func checkEntryName(name string) error {
	switch {
	case name == "":
		return errors.New("ftp: server returned a directory entry with an empty name")
	case name == "." || name == "..":
		return fmt.Errorf("ftp: refusing directory entry %q: it names the directory itself or its parent, "+
			"so the emitted path would leave the listed directory", name)
	case strings.ContainsRune(name, '/'):
		return fmt.Errorf("ftp: refusing directory entry %q: a directory entry names one path component, "+
			"and a name containing %q would make the emitted path escape the listed directory", name, "/")
	}
	for i, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("ftp: refusing directory entry %q: control character at byte %d — "+
				"the name cannot be logged or fed back into a line-oriented FTP command", name, i)
		}
	}
	return nil
}

func (s *listSource) Next(_ context.Context) (*record.Batch, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	for n := 0; s.idx < len(s.entries) && n < listBatch; s.idx, n = s.idx+1, n+1 {
		e := s.entries[s.idx]
		bld.BeginMap()
		bld.KeyLiteral("name")
		bld.StringLiteral(e.Name)
		bld.KeyLiteral("path")
		bld.StringLiteral(path.Join(s.dir, e.Name))
		bld.KeyLiteral("size")
		bld.Int(int64(e.Size)) //nolint:gosec // G115: FTP entry sizes fit int64 in practice; record model is int64
		bld.KeyLiteral("type")
		bld.StringLiteral(entryType(e.Type))
		bld.KeyLiteral("mod_time")
		bld.StringLiteral(e.Time.UTC().Format(time.RFC3339))
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }

// entryType maps the FTP entry kind to a stable string in the emitted record.
func entryType(t ftp.EntryType) string {
	switch t {
	case ftp.EntryTypeFile:
		return "file"
	case ftp.EntryTypeFolder:
		return "dir"
	case ftp.EntryTypeLink:
		return "link"
	default:
		return "unknown"
	}
}
