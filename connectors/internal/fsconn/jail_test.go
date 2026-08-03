package fsconn

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRootMustBePermittedByDeployment is the regression test for the hole that
// made the jail decorative: `root` arrives in the FLOW document, so before this
// gate any author who could deploy a flow could set it to "/" and reach
// everything the runner user could — hub credentials, the connector socket
// token, spill files, host keys.
func TestRootMustBePermittedByDeployment(t *testing.T) {
	allowed := t.TempDir()

	t.Run("unset permits nothing", func(t *testing.T) {
		t.Setenv(RootsEnv, "")
		err := (&config{Root: allowed}).validateRoot()
		if err == nil {
			t.Fatal("root accepted with no deployment allow-list; the gate must fail closed")
		}
		if !strings.Contains(err.Error(), RootsEnv) {
			t.Errorf("error should name %s so an operator knows the fix: %v", RootsEnv, err)
		}
	})

	t.Run("filesystem root rejected", func(t *testing.T) {
		t.Setenv(RootsEnv, allowed)
		if err := (&config{Root: string(filepath.Separator)}).validateRoot(); err == nil {
			t.Fatal(`root "/" accepted; a flow author must not be able to escape the deployment's roots`)
		}
	})

	t.Run("sibling of a permitted root rejected", func(t *testing.T) {
		t.Setenv(RootsEnv, filepath.Join(allowed, "data"))
		if err := (&config{Root: filepath.Join(allowed, "data-other")}).validateRoot(); err == nil {
			t.Fatal("prefix-sibling accepted; containment must be path-segment-wise, not string-prefix-wise")
		}
	})

	t.Run("permitted root and subdirectory accepted", func(t *testing.T) {
		t.Setenv(RootsEnv, allowed)
		if err := (&config{Root: allowed}).validateRoot(); err != nil {
			t.Fatalf("permitted root rejected: %v", err)
		}
		sub := filepath.Join(allowed, "sub")
		if err := (&config{Root: sub}).validateRoot(); err != nil {
			t.Fatalf("subdirectory of a permitted root rejected: %v", err)
		}
	})

	t.Run("several permitted roots", func(t *testing.T) {
		other := t.TempDir()
		t.Setenv(RootsEnv, strings.Join([]string{allowed, other}, string(filepath.ListSeparator)))
		for _, r := range []string{allowed, other} {
			if err := (&config{Root: r}).validateRoot(); err != nil {
				t.Errorf("root %q rejected: %v", r, err)
			}
		}
	})
}

// TestSymlinkEscapeRejected: the lexical containment check passes for a path
// that stays under root spelling-wise, so a symlink planted under root and
// aimed outside it would otherwise be followed. Containment is re-checked with
// symlinks resolved.
func TestSymlinkEscapeRejected(t *testing.T) {
	root := testRoot(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("classified\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A symlink to a file outside root, and one to the outside directory.
	if err := os.Symlink(secret, filepath.Join(root, "leak.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}

	c := &config{Root: root}
	for _, p := range []string{"leak.txt", "escape/secret.txt", "escape/new-file.txt"} {
		if _, err := c.resolve(p); err == nil {
			t.Errorf("resolve(%q) allowed a path that leaves root via a symlink", p)
		}
	}

	// A real file under root still resolves — the check must not be a blanket
	// refusal.
	real := filepath.Join(root, "ok.txt")
	if err := os.WriteFile(real, []byte("fine\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.resolve("ok.txt"); err != nil {
		t.Errorf("resolve of a genuine in-root file failed: %v", err)
	}
	// So does a not-yet-existing path under root (a put's destination).
	if _, err := c.resolve("new/dir/out.ndjson"); err != nil {
		t.Errorf("resolve of a new in-root path failed: %v", err)
	}
}

// TestGetRefusesSymlinkedFile exercises the guard through a real action rather
// than the helper, so the protection is proven where it is actually used.
func TestGetRefusesSymlinkedFile(t *testing.T) {
	root := testRoot(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.ndjson")
	if err := os.WriteFile(secret, []byte(`{"a":1}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(root, "link.ndjson")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	err := (&getSource{}).Open(context.Background(),
		cfgJSON(t, map[string]any{"root": root, "path": "link.ndjson"}))
	if err == nil {
		t.Fatal("get followed a symlink out of root")
	}
}
