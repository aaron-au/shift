package connpool

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func buildGen(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", //nolint:gosec // G204: builds our own package for the test
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return dir
}

func TestReuseAndIdleReap(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	dir := buildGen(t)
	p := New(Options{Dir: dir, IdleTTL: 150 * time.Millisecond, ReapEvery: 50 * time.Millisecond})
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	ctx := context.Background()

	a, err := p.Get(ctx, "gen", "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(ctx, "gen", "")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || p.Launches() != 1 {
		t.Fatalf("expected process reuse: launches = %d", p.Launches())
	}
	if snap := p.Snapshot(); len(snap) != 1 || snap[0].InUse != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	p.Put("gen", "")
	p.Put("gen", "")

	// Idle beyond TTL: the reaper should shut it down; next Get relaunches.
	deadline := time.Now().Add(5 * time.Second)
	for len(p.Snapshot()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("idle connector never reaped")
		}
		time.Sleep(25 * time.Millisecond)
	}
	c, err := p.Get(ctx, "gen", "")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen", "")
	if c == a || p.Launches() != 2 {
		t.Fatalf("expected relaunch after reap: launches = %d", p.Launches())
	}
}

func TestInvalidNamesAndMissingBinary(t *testing.T) {
	p := New(Options{Dir: t.TempDir()})
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	for _, bad := range []string{"", "UPPER", "../escape", "a b", "-lead"} {
		if _, err := p.Get(ctx, bad, ""); err == nil {
			t.Errorf("name %q accepted", bad)
		}
	}
	if _, err := p.Get(ctx, "ghost", ""); err == nil {
		t.Error("missing binary accepted")
	}
}

func TestClosedPoolRejects(t *testing.T) {
	p := New(Options{Dir: t.TempDir()})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(context.Background(), "gen", ""); err == nil {
		t.Fatal("closed pool accepted Get")
	}
}

// Two published flows may legitimately pin different builds of the same
// connector (ADR-0047 §1). Keying the pool by name alone would hand the second
// flow whichever build the first happened to start — the silent substitution
// the pin exists to prevent, moved from the registry into the process pool.
func TestPinnedVersionsGetTheirOwnProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns connector subprocesses")
	}
	dir := buildGen(t)
	// Locate ignores the version here; what is under test is the KEYING, not
	// the artifact fetch (that is connstore's job).
	p := New(Options{Locate: func(context.Context, string, string) (string, error) {
		return filepath.Join(dir, "shift-connector-gen"), nil
	}})
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	ctx := context.Background()

	one, err := p.Get(ctx, "gen", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen", "1.0.0")
	two, err := p.Get(ctx, "gen", "2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen", "2.0.0")
	if one == two {
		t.Fatal("two pinned versions shared one process; a flow would run a build it did not pin")
	}
	if p.Launches() != 2 {
		t.Fatalf("launches = %d, want 2", p.Launches())
	}

	// The same pin reuses, exactly as an unpinned one does.
	again, err := p.Get(ctx, "gen", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen", "1.0.0")
	if again != one {
		t.Fatal("the same pinned version launched a second process")
	}

	// An unpinned request is its own key: "newest" is not a claim about which
	// build, so it must not silently reuse a pinned one.
	if _, err := p.Get(ctx, "gen", ""); err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen", "")
	if p.Launches() != 3 {
		t.Fatalf("launches = %d, want 3", p.Launches())
	}
}

// The version reaches a cache path and a registry query, so it is bounded the
// same way the connector name is.
func TestAMalformedVersionIsRefused(t *testing.T) {
	p := New(Options{Dir: t.TempDir()})
	defer func() { _ = p.Close() }()
	for _, bad := range []string{"../evil", "1.0 0", ".hidden", "a/b"} {
		if _, err := p.Get(context.Background(), "gen", bad); err == nil {
			t.Fatalf("version %q was accepted", bad)
		}
	}
}
