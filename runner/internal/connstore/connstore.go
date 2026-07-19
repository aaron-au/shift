// Package connstore fetches connector binaries from the hub registry
// and verifies them before anything executes: Ed25519 signature over
// the consign manifest, SHA-256 digest of the bytes, and a re-hash of
// the cached file on EVERY use (a tampered cache is discarded and
// refetched). Any verification failure is a hard error — fail closed,
// never fall through to an unverified binary.
//
// Trust root: the hub's trusted-key list, fetched over the runner's
// authenticated TLS channel (the runner already trusts the hub for the
// tasks it executes — no new trust edge). Operators who want
// hub-independent trust pin keys with SHIFT_TRUSTED_KEYS instead, which
// disables hub key fetching entirely. The signature still protects
// against storage/transport tampering either way.
package connstore

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aaron-au/shift/pkg/consign"
	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// ErrUntrustedKey means the artifact's publisher key is not in the
// trusted set (even after a refresh).
var ErrUntrustedKey = errors.New("connstore: artifact signed by untrusted key")

// Options configure the store.
type Options struct {
	// Dir is the local artifact cache (created 0700 if absent).
	Dir string
	// Client fetches manifests, artifacts, and (unless pinned) keys.
	Client *hubclient.Client
	// PinnedKeys, when non-empty, is the EXCLUSIVE trust set — hub key
	// fetching is disabled (SHIFT_TRUSTED_KEYS).
	PinnedKeys [][]byte
}

// Store caches verified connector binaries.
type Store struct {
	opts Options

	mu      sync.Mutex
	trusted map[string]bool // base64(key) → trusted
}

// New builds a store and ensures the cache dir exists.
func New(opts Options) (*Store, error) {
	if opts.Dir == "" || opts.Client == nil {
		return nil, fmt.Errorf("connstore: Dir and Client are required")
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("connstore: cache dir: %w", err)
	}
	s := &Store{opts: opts, trusted: map[string]bool{}}
	for _, k := range opts.PinnedKeys {
		s.trusted[base64.StdEncoding.EncodeToString(k)] = true
	}
	return s, nil
}

// Ensure returns a verified executable path for the named connector,
// fetching from the hub registry on cache miss. Safe for concurrent use.
func (s *Store) Ensure(ctx context.Context, name string) (string, error) {
	m, err := s.opts.Client.ResolveConnector(ctx, name, "")
	if err != nil {
		return "", err
	}
	digest, err := hex.DecodeString(m.Digest)
	if err != nil || len(digest) != sha256.Size {
		return "", fmt.Errorf("connstore: %s: malformed digest in manifest", name)
	}
	sig, err := base64.StdEncoding.DecodeString(m.Signature)
	if err != nil {
		return "", fmt.Errorf("connstore: %s: malformed signature in manifest", name)
	}
	pub, err := base64.StdEncoding.DecodeString(m.PublisherKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return "", fmt.Errorf("connstore: %s: malformed publisher key in manifest", name)
	}

	if err := s.checkTrusted(ctx, pub); err != nil {
		return "", fmt.Errorf("connstore: %s: %w", name, err)
	}
	manifest := consign.Manifest{Name: m.Name, Version: m.Version, OS: m.OS, Arch: m.Arch}
	copy(manifest.Digest[:], digest)
	if err := consign.Verify(pub, manifest, sig); err != nil {
		return "", fmt.Errorf("connstore: %s: %w", name, err)
	}

	// Content-addressed cache path: digest in the name means a version
	// re-publish (new digest) is a different file, never a stale hit.
	path := filepath.Join(s.opts.Dir,
		fmt.Sprintf("shift-connector-%s-%s-%s", m.Name, m.Version, m.Digest[:16]))

	if fileDigestMatches(path, digest) {
		return path, nil
	}
	// Miss or tamper: (re)fetch through a hashing tee into a temp file,
	// then rename atomically (concurrent Ensures converge on one file).
	_ = os.Remove(path)
	tmp, err := os.CreateTemp(s.opts.Dir, ".fetch-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	hasher := sha256.New()
	if err := s.opts.Client.FetchConnector(ctx, m, io.MultiWriter(tmp, hasher)); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if !hashEqual(hasher.Sum(nil), digest) {
		return "", fmt.Errorf("connstore: %s: fetched artifact digest mismatch (registry tampered or corrupt)", name)
	}
	//nolint:gosec // G302: 0500 — the artifact must be executable; no write bit, owner-only
	if err := os.Chmod(tmp.Name(), 0o500); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	return path, nil
}

// checkTrusted verifies the key against the trusted set, refreshing
// from the hub once on miss (unless keys are pinned).
func (s *Store) checkTrusted(ctx context.Context, pub []byte) error {
	k := base64.StdEncoding.EncodeToString(pub)
	s.mu.Lock()
	trusted := s.trusted[k]
	pinned := len(s.opts.PinnedKeys) > 0
	s.mu.Unlock()
	if trusted {
		return nil
	}
	if pinned {
		return ErrUntrustedKey // pinned set is exclusive — no refresh
	}
	keys, err := s.opts.Client.PublisherKeys(ctx)
	if err != nil {
		return fmt.Errorf("refreshing trusted keys: %w", err)
	}
	s.mu.Lock()
	for _, key := range keys {
		s.trusted[base64.StdEncoding.EncodeToString(key)] = true
	}
	trusted = s.trusted[k]
	s.mu.Unlock()
	if !trusted {
		return ErrUntrustedKey
	}
	return nil
}

func fileDigestMatches(path string, digest []byte) bool {
	f, err := os.Open(path) //nolint:gosec // G304: path is our own content-addressed cache entry
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }() // read-only
	got, _, err := consign.HashReader(f)
	return err == nil && hashEqual(got[:], digest)
}

func hashEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	same := true
	for i := range a {
		same = same && a[i] == b[i]
	}
	return same
}
