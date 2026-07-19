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

	a, err := p.Get(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || p.Launches() != 1 {
		t.Fatalf("expected process reuse: launches = %d", p.Launches())
	}
	if snap := p.Snapshot(); len(snap) != 1 || snap[0].InUse != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	p.Put("gen")
	p.Put("gen")

	// Idle beyond TTL: the reaper should shut it down; next Get relaunches.
	deadline := time.Now().Add(5 * time.Second)
	for len(p.Snapshot()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("idle connector never reaped")
		}
		time.Sleep(25 * time.Millisecond)
	}
	c, err := p.Get(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put("gen")
	if c == a || p.Launches() != 2 {
		t.Fatalf("expected relaunch after reap: launches = %d", p.Launches())
	}
}

func TestInvalidNamesAndMissingBinary(t *testing.T) {
	p := New(Options{Dir: t.TempDir()})
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	for _, bad := range []string{"", "UPPER", "../escape", "a b", "-lead"} {
		if _, err := p.Get(ctx, bad); err == nil {
			t.Errorf("name %q accepted", bad)
		}
	}
	if _, err := p.Get(ctx, "ghost"); err == nil {
		t.Error("missing binary accepted")
	}
}

func TestClosedPoolRejects(t *testing.T) {
	p := New(Options{Dir: t.TempDir()})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(context.Background(), "gen"); err == nil {
		t.Fatal("closed pool accepted Get")
	}
}
