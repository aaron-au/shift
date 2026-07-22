package sftpconn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	cases := map[string]string{
		"missing host": `{"user":"u","password":"p","path":"/f","allow_local":true}`,
		"missing auth": `{"host":"h","user":"u","path":"/f","allow_local":true}`,
		"bad format":   `{"host":"h","user":"u","password":"p","path":"/f","format":"xml","allow_local":true}`,
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(cfg), &c); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
}
