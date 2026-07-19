package connstore

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/aaron-au/shift/pkg/consign"
	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// fakeHub serves resolve/artifact/keys for one signed artifact.
type fakeHub struct {
	t        *testing.T
	pub      ed25519.PublicKey
	manifest consign.Manifest
	sig      []byte
	artifact []byte
	fetches  atomic.Int64

	// mutators for failure-mode tests
	serveKey      func() []byte // publisher key returned by /publisher-keys
	serveArtifact func() []byte // bytes served by /artifact
}

func newFakeHub(t *testing.T) (*fakeHub, *httptest.Server) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	artifact := []byte("#!/bin/sh\necho fake-connector\n")
	m := consign.Manifest{
		Name: "gen", Version: "1.0.0", OS: runtime.GOOS, Arch: runtime.GOARCH,
		Digest: sha256.Sum256(artifact),
	}
	f := &fakeHub{t: t, pub: pub, manifest: m, sig: consign.Sign(priv, m), artifact: artifact}
	f.serveKey = func() []byte { return f.pub }
	f.serveArtifact = func() []byte { return f.artifact }

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/connectors/gen/resolve", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": m.Name, "version": m.Version, "os": m.OS, "arch": m.Arch,
			"digest":        hex.EncodeToString(m.Digest[:]),
			"signature":     base64.StdEncoding.EncodeToString(f.sig),
			"publisher_key": base64.StdEncoding.EncodeToString(f.serveKey()),
			"size_bytes":    len(f.artifact),
		})
	})
	mux.HandleFunc("GET /api/v1/connectors/gen/versions/1.0.0/artifact", func(w http.ResponseWriter, _ *http.Request) {
		f.fetches.Add(1)
		_, _ = w.Write(f.serveArtifact())
	})
	mux.HandleFunc("GET /api/v1/publisher-keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{
			{"public_key": base64.StdEncoding.EncodeToString(f.serveKey())},
		}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return f, srv
}

func newStore(t *testing.T, srv *httptest.Server, pinned [][]byte) *Store {
	t.Helper()
	s, err := New(Options{
		Dir:        t.TempDir(),
		Client:     hubclient.New(srv.URL, "rs_test"),
		PinnedKeys: pinned,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEnsureFetchVerifyCache(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, nil)

	path, err := s.Ensure(t.Context(), "gen")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("artifact file: %v mode %v", err, info.Mode())
	}
	if hub.fetches.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", hub.fetches.Load())
	}

	// Cache hit: verified by re-hash, no refetch.
	if _, err := s.Ensure(t.Context(), "gen"); err != nil {
		t.Fatal(err)
	}
	if hub.fetches.Load() != 1 {
		t.Fatalf("cache hit refetched: %d", hub.fetches.Load())
	}

	// On-disk tamper: next Ensure detects the bad hash and refetches.
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ensure(t.Context(), "gen"); err != nil {
		t.Fatal(err)
	}
	if hub.fetches.Load() != 2 {
		t.Fatalf("tampered cache not refetched: %d", hub.fetches.Load())
	}
	raw, _ := os.ReadFile(path) //nolint:gosec // G304: test cache path
	if string(raw) != string(hub.artifact) {
		t.Fatal("tampered file not replaced")
	}
}

func TestEnsureServerTamperFailsClosed(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, nil)

	// Registry serves different bytes than the signed manifest promises.
	hub.serveArtifact = func() []byte { return []byte("evil bytes") }
	path, err := s.Ensure(t.Context(), "gen")
	if err == nil {
		t.Fatal("tampered artifact accepted")
	}
	if path != "" {
		t.Fatalf("path returned on failure: %s", path)
	}
	// Nothing executable was left behind.
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if info, _ := e.Info(); info != nil && info.Mode().Perm()&0o100 != 0 {
			t.Fatalf("executable residue: %s", e.Name())
		}
	}
}

// Pinned trust is exclusive: a manifest signed by a key outside the
// pinned set fails closed, even though the hub vouches for it.
func TestEnsureUntrustedKeyFailsClosed(t *testing.T) {
	hub, srv := newFakeHub(t)
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(t, srv, [][]byte{otherPub})
	if _, err := s.Ensure(t.Context(), "gen"); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("untrusted key: err = %v, want ErrUntrustedKey", err)
	}
	if hub.fetches.Load() != 0 {
		t.Fatal("artifact fetched despite untrusted key")
	}
}

func TestEnsurePinnedKeyAccepts(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, [][]byte{hub.pub})
	if _, err := s.Ensure(t.Context(), "gen"); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBadSignature(t *testing.T) {
	hub, srv := newFakeHub(t)
	hub.sig = append([]byte{}, hub.sig...)
	hub.sig[0] ^= 0xff
	s := newStore(t, srv, [][]byte{hub.pub})
	if _, err := s.Ensure(t.Context(), "gen"); !errors.Is(err, consign.ErrBadSignature) {
		t.Fatalf("bad signature: err = %v", err)
	}
	if hub.fetches.Load() != 0 {
		t.Fatal("artifact fetched despite bad signature")
	}
}

func TestConcurrentEnsure(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, nil)
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			_, err := s.Ensure(t.Context(), "gen")
			errs <- err
		}()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if hub.fetches.Load() < 1 {
		t.Fatal("no fetch happened")
	}
}
