package sftpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// listBatch caps how many directory entries one Next emits.
const listBatch = 512

// listSource lists a remote directory, emitting one record per entry:
// {name, path, size, mode, mod_time, is_dir}. The directory is read once at
// Open (the connection closes immediately after) and iterated from memory.
type listSource struct {
	cfg     config
	dir     string
	entries []os.FileInfo
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
	sc, closer, err := s.cfg.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = closer() }() // listing is a one-shot; don't hold the connection
	entries, err := sc.ReadDir(s.cfg.Path)
	if err != nil {
		return err
	}
	return s.load(s.cfg.Path, entries)
}

// load vets the server's reply and arms the source. It is separate from Open so
// the vetting can be tested against names a cooperating SFTP server would never
// produce — which is exactly the case the check exists for.
func (s *listSource) load(dir string, entries []os.FileInfo) error {
	// Vet every name the server sent BEFORE any of them is emitted, so a
	// hostile entry fails the whole listing rather than being discovered
	// half-way through a downstream delete loop.
	for _, e := range entries {
		if err := checkEntryName(e.Name()); err != nil {
			return err
		}
	}
	s.dir, s.entries, s.batch = dir, entries, record.NewBatch()
	return nil
}

// checkEntryName refuses a directory-entry name that is not a single, safe path
// component.
//
// The name comes from the REMOTE server. An SFTP SSH_FXP_NAME reply carries the
// filename as a length-prefixed string, so the server can put ANY bytes there —
// including "/" and ".." — regardless of what a POSIX directory could really
// hold. list emits {name, path} and the `path` field exists to be fed into a
// following get/delete/rename node, so such a name is not data: path.Join
// CLEANS it and the emitted path leaves the directory the author listed. The
// flow then acts on that file with this connector's credentials. That is the
// zip-slip shape.
//
// pkg/sftp happens to apply path.Base and to drop entries named exactly "." or
// ".." — but it applies the "."/".." test to the RAW name, so a server replying
// "x/.." yields ".." after Base, and path.Join(dir, "..") is the PARENT
// directory. Either way the guard belongs here: a library's incidental
// sanitising is not a property this connector may depend on across upgrades.
//
// Refused: empty, "." and "..", anything containing "/", and NUL or C0/DEL
// control characters. Control characters are refused because a name that cannot
// be logged or correlated is an incident-response problem in its own right, and
// because feeding one back into a line-oriented protocol (this flow's next node
// may be the ftp connector) is a command injection.
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
		return errors.New("sftp: server returned a directory entry with an empty name")
	case name == "." || name == "..":
		return fmt.Errorf("sftp: refusing directory entry %q: it names the directory itself or its parent, "+
			"so the emitted path would leave the listed directory", name)
	case strings.ContainsRune(name, '/'):
		return fmt.Errorf("sftp: refusing directory entry %q: a directory entry names one path component, "+
			"and a name containing %q would make the emitted path escape the listed directory", name, "/")
	}
	for i, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("sftp: refusing directory entry %q: control character at byte %d — "+
				"the name cannot be logged, and feeding it back into a line-oriented protocol would inject a command", name, i)
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
		bld.StringLiteral(e.Name())
		bld.KeyLiteral("path")
		bld.StringLiteral(path.Join(s.dir, e.Name()))
		bld.KeyLiteral("size")
		bld.Int(e.Size())
		bld.KeyLiteral("mode")
		bld.StringLiteral(e.Mode().String())
		bld.KeyLiteral("mod_time")
		bld.StringLiteral(e.ModTime().UTC().Format(time.RFC3339))
		bld.KeyLiteral("is_dir")
		bld.Bool(e.IsDir())
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }
