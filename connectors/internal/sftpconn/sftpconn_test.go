package sftpconn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// startSFTPServer runs an in-process SSH+SFTP server on loopback (fresh host
// key, password auth), serving the real filesystem. Returns its host/port and
// the accepted credentials. Torn down via t.Cleanup.
func startSFTPServer(t *testing.T) (host string, port int, user, pass string) {
	t.Helper()
	user, pass = "shift", "sesame"
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromSigner(priv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if c.User() == user && string(p) == pass {
				return nil, nil
			}
			return nil, errors.New("auth denied")
		},
	}
	cfg.AddHostKey(signer)

	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			nConn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go serveSSH(nConn, cfg)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, user, pass
}

func serveSSH(nConn net.Conn, cfg *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(nConn, cfg)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	go ssh.DiscardRequests(reqs)
	for newCh := range chans {
		if newCh.ChannelType() != "session" {
			_ = newCh.Reject(ssh.UnknownChannelType, "only sessions")
			continue
		}
		ch, requests, err := newCh.Accept()
		if err != nil {
			continue
		}
		go func() {
			for req := range requests {
				// Accept the sftp subsystem, reject everything else.
				ok := req.Type == "subsystem" && len(req.Payload) >= 4 && string(req.Payload[4:]) == "sftp"
				_ = req.Reply(ok, nil)
			}
		}()
		srv, err := sftp.NewServer(ch)
		if err != nil {
			_ = ch.Close()
			continue
		}
		go func() { _ = srv.Serve(); _ = ch.Close() }()
	}
}

func sourceConfig(t *testing.T, host string, port int, user, pass, path, format string) []byte {
	t.Helper()
	return fmt.Appendf(nil, `{"host":%q,"port":%d,"user":%q,"password":%q,"path":%q,"format":%q,"allow_local":true}`,
		host, port, user, pass, path, format)
}

func TestSFTPSourceReadsNDJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "in.ndjson")
	if err := os.WriteFile(path, []byte("{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	host, port, user, pass := startSFTPServer(t)

	s := &getSource{}
	ctx := context.Background()
	if err := s.Open(ctx, sourceConfig(t, host, port, user, pass, path, "ndjson")); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	var got []int64
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("i")
			got = append(got, v.Int())
		}
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("records = %v, want [1 2 3]", got)
	}
}

func TestSFTPSinkRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.ndjson")
	host, port, user, pass := startSFTPServer(t)

	s := &putSink{}
	ctx := context.Background()
	if err := s.Open(ctx, sourceConfig(t, host, port, user, pass, path, "ndjson")); err != nil {
		t.Fatalf("open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	for i := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(i))
		bld.EndMap()
		batch.Append(bld.Finish())
	}
	if err := s.Write(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path) //nolint:gosec // G304: test-controlled temp path
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1
	if lines != 3 || !strings.Contains(string(data), `"i":2`) {
		t.Fatalf("written file = %q", data)
	}
	// Atomic write: the temp file must be gone after rename.
	if _, err := os.Stat(path + ".shift-partial"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file not cleaned up: %v", err)
	}
}

func TestSFTPHostKeyRequiredWithoutAllowLocal(t *testing.T) {
	// No host_key and allow_local=false → fail closed before any dial.
	s := &getSource{}
	cfg := []byte(`{"host":"example.com","user":"u","password":"p","path":"/f"}`)
	err := s.Open(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "host_key is required") {
		t.Fatalf("expected host_key-required error, got %v", err)
	}
}

func TestSFTPConfigValidation(t *testing.T) {
	// Connection-level validation (parseConfig).
	for name, cfg := range map[string]string{
		"missing host": `{"user":"u","password":"p","allow_local":true}`,
		"missing auth": `{"host":"h","user":"u","allow_local":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(cfg), &c); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
	// File-format validation (get/put, via requireFileFormat).
	t.Run("bad format", func(t *testing.T) {
		c := config{Path: "/f", Format: "xml"}
		if err := c.requireFileFormat(); err == nil {
			t.Fatal("expected unsupported-format error")
		}
	})
}

func connConfig(t *testing.T, host string, port int, user, pass string) []byte {
	t.Helper()
	return fmt.Appendf(nil, `{"host":%q,"port":%d,"user":%q,"password":%q,"allow_local":true}`, host, port, user, pass)
}

func TestSFTPList(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	host, port, user, pass := startSFTPServer(t)

	s := &listSource{}
	ctx := context.Background()
	if err := s.Open(ctx, sourceConfig(t, host, port, user, pass, dir, "")); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	dirs := map[string]bool{}
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			name, _ := rec.Field("name")
			isDir, _ := rec.Field("is_dir")
			dirs[name.String()] = isDir.Bool()
		}
	}
	if len(dirs) != 3 || dirs["a.txt"] || !dirs["sub"] {
		t.Fatalf("listing = %v, want a.txt/b.txt (files) + sub (dir)", dirs)
	}
}

// opConfig builds a config JSON for an op verb: connection + the given extra
// string fields (path, or from/to for rename).
func opConfig(t *testing.T, host string, port int, user, pass string, extra ...[2]string) []byte {
	t.Helper()
	m := map[string]any{"host": host, "port": port, "user": user, "password": pass, "allow_local": true}
	for _, p := range extra {
		m[p[0]] = p[1]
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSFTPOps(t *testing.T) {
	dir := t.TempDir()
	host, port, user, pass := startSFTPServer(t)
	ctx := context.Background()

	// runOp opens a config-driven op source, asserts it emits one status
	// record then EOF, and returns that record.
	runOp := func(op opKind, cfg []byte) record.Value {
		t.Helper()
		s := &opSource{op: op}
		if err := s.Open(ctx, cfg); err != nil {
			t.Fatalf("%s open: %v", op.name(), err)
		}
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("%s next: %v", op.name(), err)
		}
		recs := b.Records()
		if len(recs) != 1 {
			t.Fatalf("%s emitted %d records, want 1", op.name(), len(recs))
		}
		if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("%s second Next = %v, want EOF", op.name(), err)
		}
		if ok, _ := recs[0].Field("ok"); !ok.Bool() {
			t.Fatalf("%s status not ok: %v", op.name(), recs[0])
		}
		_ = s.Close()
		return recs[0]
	}

	// mkdir — single node, path in config, runs standalone.
	created := filepath.Join(dir, "created")
	rec := runOp(opMkdir, opConfig(t, host, port, user, pass, [2]string{"path", created}))
	if v, _ := rec.Field("op"); v.String() != "mkdir" {
		t.Fatalf("status op = %q, want mkdir", v.String())
	}
	if fi, err := os.Stat(created); err != nil || !fi.IsDir() {
		t.Fatalf("mkdir: %v isDir=%v", err, fi != nil && fi.IsDir())
	}

	// delete, then delete again (missing → idempotent success).
	gone := filepath.Join(dir, "gone.txt")
	if err := os.WriteFile(gone, []byte("z"), 0o600); err != nil {
		t.Fatal(err)
	}
	runOp(opDelete, opConfig(t, host, port, user, pass, [2]string{"path", gone}))
	if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete: file still present: %v", err)
	}
	runOp(opDelete, opConfig(t, host, port, user, pass, [2]string{"path", gone})) // idempotent

	// rename
	old, renamed := filepath.Join(dir, "old.txt"), filepath.Join(dir, "new.txt")
	if err := os.WriteFile(old, []byte("r"), 0o600); err != nil {
		t.Fatal(err)
	}
	runOp(opRename, opConfig(t, host, port, user, pass, [2]string{"from", old}, [2]string{"to", renamed}))
	if _, err := os.Stat(renamed); err != nil {
		t.Fatalf("rename: new path missing: %v", err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rename: old path still present")
	}

	// rmdir (empty)
	empty := filepath.Join(dir, "empty")
	if err := os.Mkdir(empty, 0o750); err != nil {
		t.Fatal(err)
	}
	runOp(opRmdir, opConfig(t, host, port, user, pass, [2]string{"path", empty}))
	if _, err := os.Stat(empty); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rmdir: dir still present")
	}
}

func TestSFTPOpMissingArgs(t *testing.T) {
	host, port, user, pass := startSFTPServer(t)
	// delete without a path → Open fails before any dial.
	if err := (&opSource{op: opDelete}).Open(context.Background(), connConfig(t, host, port, user, pass)); err == nil {
		t.Fatal("delete without path: expected error")
	}
	// rename without from/to → Open fails.
	if err := (&opSource{op: opRename}).Open(context.Background(), connConfig(t, host, port, user, pass)); err == nil {
		t.Fatal("rename without from/to: expected error")
	}
}
