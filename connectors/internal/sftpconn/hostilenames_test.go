package sftpconn

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// legitimateNames are names a real remote system genuinely produces. The
// entry-name guard must let every one of them through: rejecting these would
// break ordinary customers, which is worse than the attack being defended
// against.
var legitimateNames = []string{
	"invoices 2026-08.csv",             // spaces
	"rapport-financier-ao\u00fbt.csv",  // non-ASCII, NFC (precomposed û)
	"rapport-financier-aou\u0302t.csv", // the same name in NFD (u + combining circumflex)
	"发票.ndjson",                        // non-Latin script
	"файл.csv",                         // Cyrillic homoglyphs of Latin letters
	"invoice\u202Efdp.csv",             // right-to-left override: displays reversed
	"-rf",                              // looks like a flag, is a filename
	".hidden",                          // leading dot
	"archive.tar.gz.",                  // trailing dot
	"trailing space ",                  // trailing space
	"...",                              // three dots is a legal, ordinary name
	"..hidden",                         // starts with two dots but is not ".."
	"a..b",                             // dots in the middle
	"C:\\secrets\\key.pem",             // backslash is an ordinary character on a POSIX server, so
	// this is ONE component and cannot escape; if it is later handed to fsconn
	// on Windows, that connector's own jail rejects it as drive-absolute.
	strings.Repeat("x", 255), // the longest name most filesystems allow
}

// hostileNames must be refused: each either escapes the listed directory once
// path.Join cleans it, or cannot be logged or fed back into a line protocol.
var hostileNames = []string{
	"..",                                     // exactly the parent — reachable in practice, see below
	".",                                      // exactly the directory itself
	"",                                       // no name at all
	"../../etc/passwd",                       // traversal at the start
	"data/../../etc/passwd",                  // traversal in the middle
	"data/..",                                // traversal at the end
	"/etc/passwd",                            // absolute
	"sub/dir/file.csv",                       // a separator at all, even without traversal
	"file\x00.csv",                           // NUL truncates the name in a C server
	"file\n.csv",                             // newline: a command injection when fed to the ftp node
	"file\r.csv",                             // carriage return, likewise
	"file\x1b[2Klog.csv",                     // ANSI escape: rewrites the operator's terminal
	"\x7f",                                   // DEL
	strings.Repeat("../", 80) + "etc/passwd", // long traversal
}

// fakeInfo is an os.FileInfo carrying only a name — enough to stand in for an
// SSH_FXP_NAME entry, which is what the guard inspects.
type fakeInfo struct{ name string }

func (f fakeInfo) Name() string       { return f.name }
func (f fakeInfo) Size() int64        { return 0 }
func (f fakeInfo) Mode() fs.FileMode  { return 0 }
func (f fakeInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (f fakeInfo) IsDir() bool        { return false }
func (f fakeInfo) Sys() any           { return nil }

func infos(names ...string) []os.FileInfo {
	out := make([]os.FileInfo, 0, len(names))
	for _, n := range names {
		out = append(out, fakeInfo{n})
	}
	return out
}

// TestAListingEntryThatEscapesTheListedDirectoryIsRefused is the regression test
// for the zip-slip shape.
//
// An SSH_FXP_NAME reply carries each filename as a length-prefixed string, so
// the server can put ANY bytes there — "/" and ".." included — whatever a POSIX
// directory could really hold. list emits {name, path}, and `path` exists to be
// fed into a following get/delete/rename node, so path.Join silently cleaning a
// hostile name means the next node operates outside the directory the author
// listed, with this connector's credentials.
//
// ".." is not hypothetical even with a well-behaved library in the way:
// pkg/sftp drops entries named exactly "." or ".." but applies that test to the
// RAW name and then path.Base()es it, so a server replying "x/.." delivers ".."
// and path.Join(dir, "..") is the parent directory.
func TestAListingEntryThatEscapesTheListedDirectoryIsRefused(t *testing.T) {
	for _, name := range hostileNames {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			s := &listSource{}
			err := s.load("/incoming", infos("harmless.csv", name))
			if err == nil {
				// Emit and show the path — that is the damage.
				b, nerr := s.Next(context.Background())
				got := "<no batch>"
				if nerr == nil && b.Len() > 1 {
					p, _ := b.Records()[1].Field("path")
					got = p.String()
				}
				t.Fatalf("listing accepted entry %q; emitted path %q leaves the listed directory", name, got)
			}
			if !strings.Contains(err.Error(), "refusing directory entry") &&
				!strings.Contains(err.Error(), "empty name") {
				t.Errorf("listing failed for the wrong reason: %v", err)
			}
		})
	}
}

// TestAListingOfOrdinaryNamesStillSucceeds: the entry-name guard is a narrow
// containment check, not a filename policy. Spaces, every unicode form, RTL
// overrides, homoglyphs, leading/trailing dots and 255-byte names are all
// legitimate on a real server and must survive — including the ones that
// display misleadingly, which are an audit-trail hazard rather than a traversal.
func TestAListingOfOrdinaryNamesStillSucceeds(t *testing.T) {
	s := &listSource{}
	if err := s.load("/incoming", infos(legitimateNames...)); err != nil {
		t.Fatalf("a listing of ordinary names was refused: %v", err)
	}
	b, err := s.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != len(legitimateNames) {
		t.Fatalf("emitted %d records for %d legitimate names", b.Len(), len(legitimateNames))
	}
	for i, want := range legitimateNames {
		// Byte-identical: normalising the name here would make the emitted path
		// fail to match the file that is actually on the server.
		got, _ := b.Records()[i].Field("name")
		if got.String() != want {
			t.Errorf("name %d = %q, want %q", i, got.String(), want)
		}
		p, _ := b.Records()[i].Field("path")
		if p.String() != "/incoming/"+want {
			t.Errorf("path %d = %q, want %q", i, p.String(), "/incoming/"+want)
		}
	}
}

// TestARealListingOfAwkwardFilenamesRoundTrips proves the guard against a real
// SFTP server rather than a fabricated reply: the names below are ones a
// customer really has, and they must survive list → get end to end.
func TestARealListingOfAwkwardFilenamesRoundTrips(t *testing.T) {
	dir := t.TempDir()
	// A subset that every filesystem the CI runs on accepts.
	names := []string{"invoices 2026-08.ndjson", "rapport-ao\u00fbt.ndjson", "发票.ndjson", "-rf.ndjson"}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(`{"a":1}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	host, port, user, pass := startSFTPServer(t)
	src := &listSource{}
	cfg := fmt.Appendf(nil, `{"host":%q,"port":%d,"user":%q,"password":%q,"path":%q,"allow_local":true}`,
		host, port, user, pass, dir)
	if err := src.Open(context.Background(), cfg); err != nil {
		t.Fatalf("real listing of ordinary-but-awkward names refused: %v", err)
	}
	defer func() { _ = src.Close() }()
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, rec := range b.Records() {
		n, _ := rec.Field("name")
		seen[n.String()] = true
	}
	for _, n := range names {
		if !seen[n] {
			t.Errorf("listing dropped %q; seen = %v", n, seen)
		}
	}
}

// TestAConfiguredRemotePathIsSentToTheServerUnaltered records the deliberate
// position for sftp: a configured path is a REMOTE path, and the SFTP server's
// own chroot/permissions are the boundary — not anything this connector can
// decide. A "../" in a configured path is therefore not a vulnerability here,
// and it must NOT be cleaned client-side: silently rewriting it would make the
// verb act on a file other than the one the flow document names, which is the
// failure this whole audit is about. The path never becomes a LOCAL path.
func TestAConfiguredRemotePathIsSentToTheServerUnaltered(t *testing.T) {
	outside := t.TempDir()
	target := filepath.Join(outside, "held.ndjson")
	if err := os.WriteFile(target, []byte(`{"a":1}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Address the same file through a "../" path relative to a sibling
	// directory. The server resolves it; the connector must not.
	sibling := filepath.Join(outside, "sub")
	if err := os.Mkdir(sibling, 0o750); err != nil {
		t.Fatal(err)
	}
	// Built by concatenation, not filepath.Join, which would clean the ".." away
	// before the connector ever saw it.
	traversal := sibling + "/../held.ndjson"
	if !strings.Contains(traversal, "..") {
		t.Fatalf("test setup lost the traversal: %q", traversal)
	}

	host, port, user, pass := startSFTPServer(t)
	src := &getSource{}
	if err := src.Open(context.Background(),
		sourceConfig(t, host, port, user, pass, traversal, "ndjson")); err != nil {
		t.Fatalf("connector rewrote or refused a remote path the server would have resolved: %v", err)
	}
	defer func() { _ = src.Close() }()
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 1 {
		t.Fatalf("read %d records, want 1", b.Len())
	}
}

// TestASymlinkOutOfTheTreeIsListedButNeverResolvedIntoItsTargetPath.
//
// Symlink containment on a REMOTE server is the server's job — its chroot and
// permissions are the boundary, and an SFTP client has no jail to enforce. What
// this connector must not do is help: the emitted path for a symlink entry has
// to stay "<listed dir>/<link name>", never the link's target. Emitting the
// target would hand a downstream get/delete node a path outside the directory
// the author listed, which is the escape the entry-name guard exists to stop —
// just arriving by a different route.
func TestASymlinkOutOfTheTreeIsListedButNeverResolvedIntoItsTargetPath(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.ndjson"), []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.ndjson"), filepath.Join(dir, "leak.ndjson")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}

	host, port, user, pass := startSFTPServer(t)
	src := &listSource{}
	cfg := fmt.Appendf(nil, `{"host":%q,"port":%d,"user":%q,"password":%q,"path":%q,"allow_local":true}`,
		host, port, user, pass, dir)
	if err := src.Open(context.Background(), cfg); err != nil {
		t.Fatalf("a directory containing symlinks could not be listed: %v", err)
	}
	defer func() { _ = src.Close() }()
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, rec := range b.Records() {
		n, _ := rec.Field("name")
		p, _ := rec.Field("path")
		if n.String() != "leak.ndjson" && n.String() != "escape" {
			continue
		}
		seen++
		if want := dir + "/" + n.String(); p.String() != want {
			t.Errorf("symlink %q emitted path %q, want %q — the target must never leak into the path",
				n.String(), p.String(), want)
		}
		if strings.Contains(p.String(), outside) {
			t.Errorf("the emitted path %q exposes the symlink's target outside the listed directory", p.String())
		}
	}
	if seen != 2 {
		t.Errorf("saw %d of the 2 symlinks; an operator must still be able to SEE them in a listing", seen)
	}
}
