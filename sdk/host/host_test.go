package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
)

// TestMain lets a test re-exec its own binary as a connector subprocess so
// the production Launch/ExtractDescriptor spawn path is covered without a
// cross-module `go build`. Launch passes the socket+token via env; sdk.Serve
// reads them. SHIFT_HOST_TEST_MODE is set on the parent (t.Setenv) so it
// rides in os.Environ() into the child.
func TestMain(m *testing.M) {
	switch os.Getenv("SHIFT_HOST_TEST_MODE") {
	case "serve":
		if err := sdk.Serve(reexecConnector(&testState{})); err != nil {
			fmt.Fprintln(os.Stderr, "reexec connector:", err)
			os.Exit(1)
		}
		os.Exit(0)
	case "exit":
		os.Exit(3) // crash before ever serving the socket
	}
	os.Exit(m.Run())
}

// ---- test connector -------------------------------------------------------

type testState struct {
	seen   atomic.Int64
	closed atomic.Bool
}

// countSource emits {"i":0..n-1} in batches of 100, optionally failing.
type countSource struct {
	n, failAt, next int
	batch           *record.Batch
}

func (s *countSource) Open(_ context.Context, config []byte) error {
	var cfg struct {
		N      int `json:"n"`
		FailAt int `json:"fail_at"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return err
	}
	if cfg.N <= 0 {
		return errors.New("n must be positive")
	}
	s.n, s.failAt = cfg.N, cfg.FailAt
	s.batch = record.NewBatch()
	return nil
}

func (s *countSource) Next(context.Context) (*record.Batch, error) {
	if s.next >= s.n {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	for range 100 {
		if s.next >= s.n {
			break
		}
		if s.failAt > 0 && s.next == s.failAt {
			return nil, fmt.Errorf("injected source failure at %d", s.next)
		}
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(s.next))
		bld.EndMap()
		s.batch.Append(bld.Finish())
		s.next++
	}
	return s.batch, nil
}

func (s *countSource) Close() error { return nil }

type collectSink struct {
	st     *testState
	failAt int64
}

func (s *collectSink) Open(_ context.Context, config []byte) error {
	var cfg struct {
		FailAt int64 `json:"fail_at"`
	}
	if len(config) > 0 {
		if err := json.Unmarshal(config, &cfg); err != nil {
			return err
		}
	}
	s.failAt = cfg.FailAt
	return nil
}

func (s *collectSink) Write(_ context.Context, b *record.Batch) error {
	for range b.Records() {
		n := s.st.seen.Add(1)
		if s.failAt > 0 && n == s.failAt {
			return fmt.Errorf("injected sink failure at %d", n)
		}
	}
	return nil
}

func (s *collectSink) Close() error {
	s.st.closed.Store(true)
	return nil
}

func reexecConnector(st *testState) sdk.Connector {
	return sdk.Connector{
		Name:    "test",
		Version: "1.2.3",
		Sources: map[string]func() sdk.SourceAction{
			"count": func() sdk.SourceAction { return &countSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"collect": func() sdk.SinkAction { return &collectSink{st: st} },
		},
		Schemas: map[string][]byte{
			"count": []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`),
		},
	}
}

// ---- in-process helpers ---------------------------------------------------

// attachConn serves c in-process over a real socket and returns an attached
// Process; cleanup closes it (which stops the server) and reports serve errs.
func attachConn(t *testing.T, c sdk.Connector) *Process {
	t.Helper()
	socket, token, errc := serveRaw(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Attach(ctx, socket, token, 5*time.Second)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return p
}

// serveRaw starts a connector server in-process and returns its socket,
// token, and the channel that receives ServeOn's result. Cleanup is
// non-blocking: callers that never Close a Process leave the serve
// goroutine to end when the test binary exits.
func serveRaw(t *testing.T, c sdk.Connector) (socket, token string, errc chan error) {
	t.Helper()
	socket = filepath.Join(t.TempDir(), "conn.sock")
	token = "tok-secret"
	errc = make(chan error, 1)
	go func() { errc <- sdk.ServeOn(socket, token, c) }()
	return socket, token, errc
}

// ---- Attach / connect -----------------------------------------------------

func TestAttachHandshakeAndInfo(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	info := p.Info()
	if info.Name != "test" || info.Version != "1.2.3" {
		t.Fatalf("info identity = %+v", info)
	}
	if info.ProtocolVersion != sdk.ProtocolVersion {
		t.Fatalf("protocol = %d, want %d", info.ProtocolVersion, sdk.ProtocolVersion)
	}
	if len(info.SourceActions) != 1 || info.SourceActions[0] != "count" {
		t.Fatalf("sources = %v", info.SourceActions)
	}
	if len(info.SinkActions) != 1 || info.SinkActions[0] != "collect" {
		t.Fatalf("sinks = %v", info.SinkActions)
	}
	if err := p.Health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
}

func TestAttachDefaultTimeout(t *testing.T) {
	socket, token, _ := serveRaw(t, reexecConnector(&testState{}))
	// timeout <= 0 exercises the 10s default branch.
	p, err := Attach(context.Background(), socket, token, 0)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if p.Info().Name != "test" {
		t.Fatalf("info = %+v", p.Info())
	}
}

func TestAttachWrongToken(t *testing.T) {
	socket, _, _ := serveRaw(t, reexecConnector(&testState{}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Attach(ctx, socket, "wrong-token", time.Second)
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("err = %v, want token rejection", err)
	}
}

func TestAttachHandshakeTimeout(t *testing.T) {
	// A socket path with nobody serving: handshake retries until the
	// deadline, then reports a timeout.
	socket := filepath.Join(t.TempDir(), "dead.sock")
	start := time.Now()
	_, err := Attach(context.Background(), socket, "tok", 250*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want handshake timeout", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("timeout took %v, expected ~250ms", d)
	}
}

func TestAttachContextCanceled(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "dead.sock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before the first handshake attempt
	_, err := Attach(ctx, socket, "tok", 5*time.Second)
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

// ---- Launch (real subprocess: re-exec of the test binary) -----------------

func TestLaunchAndClose(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	t.Setenv("SHIFT_HOST_TEST_MODE", "serve")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := Launch(ctx, os.Args[0], LaunchOptions{HandshakeTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if p.Info().Name != "test" {
		t.Fatalf("info = %+v", p.Info())
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}

	// Pull a stream through the real subprocess.
	src := p.Source("count", []byte(`{"n":250}`))
	var n int64
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		n += int64(b.Len())
	}
	if n != 250 {
		t.Fatalf("pulled %d, want 250", n)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Second Close is a no-op (waitCh already consumed, dir already gone).
	_ = p.Close()
}

func TestLaunchCustomStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	t.Setenv("SHIFT_HOST_TEST_MODE", "serve")
	f, err := os.CreateTemp(t.TempDir(), "stderr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := Launch(ctx, os.Args[0], LaunchOptions{Stderr: f, HandshakeTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestLaunchStartError(t *testing.T) {
	// A path that cannot start exercises cmd.Start's error branch (no
	// subprocess is created, so no Short skip needed).
	_, err := Launch(context.Background(), "/nonexistent/shift-bogus-binary-xyz", LaunchOptions{})
	if err == nil || !strings.Contains(err.Error(), "start") {
		t.Fatalf("err = %v, want start failure", err)
	}
}

func TestLaunchConnectorExitsBeforeHandshake(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	t.Setenv("SHIFT_HOST_TEST_MODE", "exit")
	start := time.Now()
	_, err := Launch(context.Background(), os.Args[0], LaunchOptions{HandshakeTimeout: 20 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "exited before handshake") {
		t.Fatalf("err = %v, want exit-before-handshake", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("failure took %v; should fail fast on process exit", d)
	}
}

// ---- Describe / ExtractDescriptor -----------------------------------------

func TestDescribeInProcess(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	d, err := p.Describe(context.Background())
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if d.Name != "test" || d.Version != "1.2.3" {
		t.Fatalf("descriptor identity = %+v", d)
	}
	if len(d.Actions) != 2 {
		t.Fatalf("actions = %d, want 2", len(d.Actions))
	}
	var sawSchema bool
	for _, a := range d.Actions {
		if a.Action == "count" && len(a.ConfigSchema) > 0 {
			sawSchema = true
		}
	}
	if !sawSchema {
		t.Fatalf("count action lost its config schema: %+v", d.Actions)
	}
}

func TestExtractDescriptor(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	t.Setenv("SHIFT_HOST_TEST_MODE", "serve")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	canon, d, err := ExtractDescriptor(ctx, os.Args[0])
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if d.Name != "test" || d.Version != "1.2.3" {
		t.Fatalf("descriptor = %+v", d)
	}
	// Canonical bytes must round-trip and match CanonicalDescriptor.
	want, err := sdk.CanonicalDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(canon) != string(want) {
		t.Fatalf("canonical mismatch:\n got %s\nwant %s", canon, want)
	}
	var parsed sdk.Descriptor
	if err := json.Unmarshal(canon, &parsed); err != nil {
		t.Fatalf("canonical bytes not valid JSON: %v", err)
	}
}

func TestExtractDescriptorLaunchError(t *testing.T) {
	_, _, err := ExtractDescriptor(context.Background(), "/nonexistent/shift-bogus-binary-xyz")
	if err == nil {
		t.Fatal("expected launch error")
	}
}

// ---- Close variants -------------------------------------------------------

func TestCloseAttachedNoProcess(t *testing.T) {
	// An attached Process (no cmd/waitCh/dir) still closes cleanly.
	socket, token, errc := serveRaw(t, reexecConnector(&testState{}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Attach(ctx, socket, token, 5*time.Second)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Errorf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server did not stop after Shutdown")
	}
}

func TestKillNilProcess(t *testing.T) {
	// kill on a Process with no cmd (attached) is a no-op.
	p := &Process{}
	if err := p.kill(); err != nil {
		t.Fatalf("kill nil cmd = %v, want nil", err)
	}
}

func TestNewToken(t *testing.T) {
	a, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := newToken()
	if a == b || len(a) != 64 {
		t.Fatalf("token not unique/64-hex: %q %q", a, b)
	}
}
