package connectors_test

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/sdk/host"
)

// buildGenConnector compiles the gen connector binary once per test run.
func buildGenConnector(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "shift-connector-gen")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "./cmd/shift-connector-gen") //nolint:gosec // G204: test compiles our own package to a temp path
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build connector: %v\n%s", err, out)
	}
	return bin
}

// TestLaunchRealSubprocess covers the production spawn path end to end:
// spawn, handshake, pull a stream, health, graceful shutdown, cleanup.
func TestLaunchRealSubprocess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess and compiles a binary")
	}
	bin := buildGenConnector(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	p, err := host.Launch(ctx, bin, host.LaunchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	info := p.Info()
	if info.Name != "gen" || len(info.SourceActions) != 1 || len(info.SinkActions) != 1 {
		t.Fatalf("handshake info = %+v", info)
	}
	if err := p.Health(ctx); err != nil {
		t.Fatal(err)
	}

	src := p.Source("gen", []byte(`{"records":25000}`))
	var n int64
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n += int64(b.Len())
	}
	if n != 25000 {
		t.Fatalf("pulled %d records, want 25000", n)
	}

	// Round-trip through the subprocess sink too.
	sink := p.Sink("discard", nil)
	src2 := p.Source("gen", []byte(`{"records":5000}`))
	for {
		b, err := src2.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Write(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.Records != 5000 {
		t.Fatalf("sink confirmed %d records, want 5000", sink.Records)
	}
}

// TestLaunchBadBinaryFailsFast: a binary that exits immediately must
// surface a handshake failure, not hang until timeout.
func TestLaunchBadBinaryFailsFast(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err := host.Launch(ctx, "/usr/bin/true", host.LaunchOptions{HandshakeTimeout: 20 * time.Second})
	if err == nil {
		t.Fatal("expected launch failure")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("failure took %v; should fail fast on process exit", time.Since(start))
	}
}
