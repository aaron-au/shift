package fsconn

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// escapingPaths must never resolve to a host path outside root. These are the
// classic traversal spellings — at the start, in the middle, at the end, plus
// absolute paths and the bare parent — and they are the only reason this
// connector's jail exists: a runner holds several tenants' work, so a path that
// leaves root reads or overwrites somebody else's data, the runner's own hub
// credentials, or its connector socket token.
var escapingPaths = []string{
	"..",
	"../",
	"../etc/passwd",
	"../../../../../../../../etc/passwd",
	"sub/../../etc/passwd",
	"sub/dir/../../../etc/passwd",
	"/etc/passwd",
	"/",
	"./../etc/passwd",
	"sub/./../../etc/passwd",
	strings.Repeat("../", 200) + "etc/passwd",
}

// ordinaryPaths are paths customers really configure. The jail must accept
// every one: remote and local systems legitimately use spaces, unicode of any
// normalisation, dots, and deep trees, and rejecting them would break real
// flows for no security gain.
var ordinaryPaths = []string{
	"file.ndjson",
	"sub/dir/file.ndjson",
	"./file.ndjson",
	"invoices 2026-08.ndjson",
	"rapport-ao\u00fbt.ndjson",  // NFC
	"rapport-aou\u0302t.ndjson", // NFD — a different byte string, same look
	"发票.ndjson",
	"файл.ndjson",
	"invoice\u202Efdp.ndjson", // right-to-left override: displays reversed
	"-rf.ndjson",              // looks like a flag
	".hidden",
	"archive.tar.gz.",  // trailing dot
	"trailing space ",  // trailing space
	"...",              // three dots: an ordinary name, NOT a traversal
	"..hidden",         // starts with two dots but is not ".."
	"a..b/c..d.ndjson", // dots in the middle of components
	// A ".." that only cancels a component it introduced itself stays inside
	// root, so it is legitimate, not a traversal. Refusing it would reject a
	// path the operator can also spell without the "..".
	"a/b/..",
	"a/b/../..",
	"sub/../file.ndjson",
	"a/b/c/d/e/f/g/h/i/j/k/file.ndjson",
	"sub/..hidden/file.ndjson",
	// A Windows-shaped path is not special on this platform: backslash is an
	// ordinary filename character, so this stays a single component under root.
	"C:\\windows\\system32",
	"..\\..\\windows\\system32",
}

// TestNoSpellingOfDotDotEscapesTheRoot pins the jail against the whole traversal
// family at once. resolve() is the single choke point every verb goes through,
// so proving it here proves it for get/put/list/delete/mkdir/rmdir.
func TestNoSpellingOfDotDotEscapesTheRoot(t *testing.T) {
	root := testRoot(t)
	c := &config{Root: root}
	for _, p := range escapingPaths {
		t.Run(p, func(t *testing.T) {
			got, err := c.resolve(p)
			if err == nil {
				t.Fatalf("resolve(%q) = %q — a path that leaves root was accepted", p, got)
			}
			if !strings.Contains(err.Error(), "escapes root") && !strings.Contains(err.Error(), "outside root") {
				t.Errorf("resolve(%q) failed for the wrong reason: %v", p, err)
			}
		})
	}
}

// TestOrdinaryPathsStillResolve: the jail must be a containment check, not a
// filename policy. If this fails, the fix for something else has started
// rejecting real customer data.
func TestOrdinaryPathsStillResolve(t *testing.T) {
	root := testRoot(t)
	c := &config{Root: root}
	for _, p := range ordinaryPaths {
		t.Run(p, func(t *testing.T) {
			got, err := c.resolve(p)
			if err != nil {
				t.Fatalf("resolve(%q) rejected a legitimate path: %v", p, err)
			}
			// Belt and braces: whatever it resolved to must still be under root.
			rel, rerr := filepath.Rel(root, got)
			if rerr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				t.Fatalf("resolve(%q) = %q, which is not under root %q", p, got, root)
			}
		})
	}
}

// TestAPathWithANULByteIsRefused. A NUL cannot appear in any POSIX filename;
// every syscall takes a C string and would truncate the name there, so the file
// opened would not be the file named. Go's os package rejects it — this test
// pins that the connector surfaces the refusal rather than acting on a
// truncated path, and that the refusal happens for every verb.
func TestAPathWithANULByteIsRefused(t *testing.T) {
	root := testRoot(t)
	ctx := context.Background()
	const evil = "sub/file\x00.ndjson"

	if _, err := (&config{Root: root}).resolve(evil); err == nil {
		t.Error("resolve accepted a path containing NUL")
	}
	for name, run := range map[string]func() error{
		"get":  func() error { return (&getSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": evil})) },
		"put":  func() error { return (&putSink{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": evil})) },
		"list": func() error { return (&listSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": evil})) },
		"delete": func() error {
			s := &opSource{op: opDelete}
			if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": evil})); err != nil {
				return err
			}
			_, err := s.Next(ctx)
			return err
		},
		"mkdir": func() error {
			s := &opSource{op: opMkdir}
			if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": evil})); err != nil {
				return err
			}
			_, err := s.Next(ctx)
			return err
		},
	} {
		if err := run(); err == nil {
			t.Errorf("%s accepted a path containing NUL", name)
		}
	}
	// Nothing was created under root by the attempt.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("a refused NUL path still left %d entries under root", len(entries))
	}
}

// TestAnEscapingPathIsRefusedByEveryVerb proves the jail where it is actually
// used, not only at the helper: a verb that forgot to call resolve would pass
// the helper test and fail this one. put and mkdir are the dangerous pair —
// they CREATE, so an unjailed one writes outside root rather than merely
// failing to find a file.
func TestAnEscapingPathIsRefusedByEveryVerb(t *testing.T) {
	root := testRoot(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.ndjson")
	if err := os.WriteFile(secret, []byte(`{"a":1}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Address the file outside root by traversing out of root.
	rel, err := filepath.Rel(root, secret)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rel, "..") {
		t.Fatalf("test setup: %q is not outside root", rel)
	}

	for _, tc := range []struct {
		verb string
		path string
		run  func(p string) error
	}{
		{"get", rel, func(p string) error {
			return (&getSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p}))
		}},
		{"put", filepath.Join(rel, "..", "written.ndjson"), func(p string) error {
			return (&putSink{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p}))
		}},
		{"list", filepath.Dir(rel), func(p string) error {
			return (&listSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p}))
		}},
		{"delete", rel, func(p string) error {
			s := &opSource{op: opDelete}
			if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p})); err != nil {
				return err
			}
			_, err := s.Next(ctx)
			return err
		}},
		{"mkdir", filepath.Join(rel, "..", "created"), func(p string) error {
			s := &opSource{op: opMkdir}
			if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p})); err != nil {
				return err
			}
			_, err := s.Next(ctx)
			return err
		}},
		{"rmdir", filepath.Dir(rel), func(p string) error {
			s := &opSource{op: opRmdir, cfg: config{Recursive: true}}
			if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p, "recursive": true})); err != nil {
				return err
			}
			_, err := s.Next(ctx)
			return err
		}},
	} {
		t.Run(tc.verb, func(t *testing.T) {
			if err := tc.run(tc.path); err == nil {
				t.Fatalf("%s accepted %q, which resolves outside root", tc.verb, tc.path)
			}
		})
	}

	// The escaping verbs must have had no effect outside root.
	if _, err := os.Stat(secret); err != nil {
		t.Errorf("a refused verb deleted or damaged a file outside root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "written.ndjson")); err == nil {
		t.Error("put wrote outside root")
	}
	if _, err := os.Stat(filepath.Join(outside, "created")); err == nil {
		t.Error("mkdir created a directory outside root")
	}
}

// TestAListedPathNeverLeavesTheRoot. The list verb emits a root-RELATIVE path
// for each entry precisely so it can be fed back into a get/delete node on the
// same root. That makes the emitted path security-relevant even though the
// names themselves are only data: if one could ever start with "..", the
// following node would act outside the jail. Symlinks are the interesting case
// — a listing shows them, and the emitted path must still be one the jail
// refuses when it is used.
func TestAListedPathNeverLeavesTheRoot(t *testing.T) {
	root := testRoot(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.ndjson"), []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"ordinary.ndjson", "invoices 2026-08.ndjson", "发票.ndjson", "..hidden"} {
		if err := os.WriteFile(filepath.Join(root, "sub", n), []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "sub", "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	src := &listSource{}
	if err := src.Open(context.Background(),
		cfgJSON(t, map[string]any{"root": root, "path": ".", "recursive": true})); err != nil {
		t.Fatalf("recursive list of an ordinary tree failed: %v", err)
	}
	defer func() { _ = src.Close() }()

	c := &config{Root: root}
	var sawSymlink bool
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range b.Records() {
		p, _ := rec.Field("path")
		got := p.String()
		if filepath.IsAbs(got) || got == ".." || strings.HasPrefix(got, ".."+string(filepath.Separator)) {
			t.Errorf("listing emitted %q, which leaves the root", got)
		}
		if filepath.Base(got) == "escape" {
			sawSymlink = true
			// The listing reports the symlink (an operator wants to see it) but
			// the jail must refuse to follow it when the path is used.
			if _, err := c.resolve(got); err == nil {
				t.Errorf("the emitted path %q for an escaping symlink resolved successfully", got)
			}
		}
	}
	if !sawSymlink {
		t.Error("the symlink was not listed at all; the test proved nothing")
	}
	// A recursive walk must not have descended THROUGH the symlink either.
	for _, rec := range b.Records() {
		p, _ := rec.Field("path")
		if strings.Contains(p.String(), "escape"+string(filepath.Separator)) {
			t.Errorf("the recursive walk followed a symlink out of root: %q", p.String())
		}
	}
}

// TestAVeryLongPathIsRefusedRatherThanTruncated. A name longer than the
// filesystem allows must produce an error, never a silently shortened path that
// writes to a different file than the one configured.
func TestAVeryLongPathIsRefusedRatherThanTruncated(t *testing.T) {
	root := testRoot(t)
	long := strings.Repeat("n", 4096) + ".ndjson"
	if _, err := (&config{Root: root}).resolve(long); err == nil {
		t.Error("a path far longer than any filesystem allows was accepted")
	}
	// A name at the ordinary per-component limit is legitimate and must work.
	if _, err := (&config{Root: root}).resolve(strings.Repeat("n", 200) + ".ndjson"); err != nil {
		t.Errorf("a 208-byte filename — legal everywhere — was rejected: %v", err)
	}
}
