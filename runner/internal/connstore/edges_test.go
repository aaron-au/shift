package connstore

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// The store is the runner's last line of defence before a downloaded binary
// executes (ADR-0011). Every edge below must FAIL CLOSED: no path returned, no
// artifact fetched when the manifest itself cannot be trusted.

func TestNewRequiresDirAndClient(t *testing.T) {
	_, srv := newFakeHub(t)
	client := hubclient.New(srv.URL, "rs_test")
	cases := []struct {
		name string
		opts Options
	}{
		{"no-dir", Options{Client: client}},
		{"no-client", Options{Dir: t.TempDir()}},
		{"neither", Options{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.opts)
			if err == nil {
				t.Fatal("New accepted incomplete options")
			}
			if s != nil {
				t.Fatalf("store returned on error: %+v", s)
			}
		})
	}
}

func TestNewCacheDirFailure(t *testing.T) {
	_, srv := newFakeHub(t)
	client := hubclient.New(srv.URL, "rs_test")
	// A regular file where the cache dir's parent must be: MkdirAll fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{Dir: filepath.Join(blocker, "cache"), Client: client})
	if err == nil {
		t.Fatal("New accepted an uncreatable cache dir")
	}
	if s != nil {
		t.Fatalf("store returned on error: %+v", s)
	}
	if !strings.Contains(err.Error(), "cache dir") {
		t.Errorf("err = %v, want a cache-dir error", err)
	}
}

func TestEnsureResolveFailurePropagates(t *testing.T) {
	hub, srv := newFakeHub(t)
	hub.resolveStatus = 500
	s := newStore(t, srv, [][]byte{hub.pub})

	path, err := s.Ensure(t.Context(), "gen")
	if err == nil {
		t.Fatal("resolve failure accepted")
	}
	if path != "" {
		t.Fatalf("path returned on failure: %s", path)
	}
	if hub.fetches.Load() != 0 {
		t.Fatal("artifact fetched despite an unresolvable manifest")
	}
}

// A manifest field the runner cannot even decode is never "close enough":
// every malformation is rejected before the artifact is touched.
func TestEnsureMalformedManifestFieldsFailClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"digest-not-hex", func(m map[string]any) { m["digest"] = "zz-not-hex" }, "malformed digest"},
		{"digest-wrong-length", func(m map[string]any) { m["digest"] = "abcd" }, "malformed digest"},
		{"signature-not-base64", func(m map[string]any) { m["signature"] = "!!!not-base64!!!" }, "malformed signature"},
		{"key-not-base64", func(m map[string]any) { m["publisher_key"] = "!!!not-base64!!!" }, "malformed publisher key"},
		{"key-wrong-length", func(m map[string]any) {
			m["publisher_key"] = base64.StdEncoding.EncodeToString([]byte("too-short"))
		}, "malformed publisher key"},
		{"descriptor-not-base64", func(m map[string]any) { m["descriptor"] = "!!!not-base64!!!" }, "malformed descriptor"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, srv := newFakeHub(t)
			if strings.HasPrefix(tc.name, "descriptor") {
				// Only a v2 (descriptor-bearing) manifest reaches the
				// descriptor decode, so sign one first.
				hub.signV2([]byte(`{"actions":[]}`))
			}
			hub.mutate = tc.mutate
			s := newStore(t, srv, [][]byte{hub.pub})

			path, err := s.Ensure(t.Context(), "gen")
			if err == nil {
				t.Fatal("malformed manifest accepted")
			}
			if path != "" {
				t.Fatalf("path returned on failure: %s", path)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
			if hub.fetches.Load() != 0 {
				t.Error("artifact fetched despite a malformed manifest")
			}
		})
	}
}

// Unpinned trust refreshes from the hub once. A signer the refreshed list
// still does not vouch for is rejected — the refresh is not a rubber stamp.
func TestEnsureUnknownKeyAfterRefreshFailsClosed(t *testing.T) {
	hub, srv := newFakeHub(t)
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// The manifest claims a signer the hub's key list does not include.
	hub.resolveKey = func() []byte { return otherPub }
	s := newStore(t, srv, nil) // unpinned: a hub refresh is allowed

	if _, err := s.Ensure(t.Context(), "gen"); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("err = %v, want ErrUntrustedKey", err)
	}
	if hub.fetches.Load() != 0 {
		t.Fatal("artifact fetched despite an untrusted signer")
	}
	// The refresh happened and the hub's own key was learned — the rejection
	// is about this signer, not a broken refresh.
	s.mu.Lock()
	learned := s.trusted[base64.StdEncoding.EncodeToString(hub.pub)]
	s.mu.Unlock()
	if !learned {
		t.Error("hub key list was not merged into the trusted set")
	}
}

// A key refresh that fails is an error, never an implicit "trust it anyway".
func TestEnsureKeyRefreshFailureFailsClosed(t *testing.T) {
	hub, srv := newFakeHub(t)
	hub.keysStatus = 500
	s := newStore(t, srv, nil) // unpinned: refresh is attempted, and fails

	_, err := s.Ensure(t.Context(), "gen")
	if err == nil {
		t.Fatal("unavailable key list accepted")
	}
	if !strings.Contains(err.Error(), "refreshing trusted keys") {
		t.Errorf("err = %v, want a key-refresh error", err)
	}
	if hub.fetches.Load() != 0 {
		t.Fatal("artifact fetched without a trusted key")
	}
}

func TestEnsureArtifactFetchFailure(t *testing.T) {
	hub, srv := newFakeHub(t)
	hub.artifactStatus = 500
	s := newStore(t, srv, [][]byte{hub.pub})

	path, err := s.Ensure(t.Context(), "gen")
	if err == nil {
		t.Fatal("failed fetch accepted")
	}
	if path != "" {
		t.Fatalf("path returned on failure: %s", path)
	}
	// Nothing was left in the cache dir for a later run to pick up.
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("residue in cache dir after failed fetch: %v", entries)
	}
}

func TestEnsureTempFileFailure(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, [][]byte{hub.pub})
	// The cache dir disappears between New and Ensure (operator wiped it,
	// tmpfs reset): the staging file cannot be created.
	if err := os.RemoveAll(s.opts.Dir); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Ensure(t.Context(), "gen"); err == nil {
		t.Fatal("missing cache dir accepted")
	}
	if hub.fetches.Load() != 0 {
		t.Error("artifact fetched before staging file existed")
	}
}

func TestEnsurePublishFailure(t *testing.T) {
	hub, srv := newFakeHub(t)
	s := newStore(t, srv, [][]byte{hub.pub})
	// Occupy the content-addressed cache path with a non-empty directory:
	// verification passes, but the artifact cannot be published into place.
	digest := hex.EncodeToString(hub.manifest.Digest[:])
	path := filepath.Join(s.opts.Dir, "shift-connector-gen-1.0.0-"+digest[:16])
	if err := os.MkdirAll(filepath.Join(path, "occupied"), 0o700); err != nil {
		t.Fatal(err)
	}

	got, err := s.Ensure(t.Context(), "gen")
	if err == nil {
		t.Fatal("unpublishable artifact reported as ready")
	}
	if got != "" {
		t.Fatalf("path returned on failure: %s", got)
	}
	// The blocked path is still the directory — no half-written binary.
	info, statErr := os.Stat(path)
	if statErr != nil || !info.IsDir() {
		t.Fatalf("cache path replaced: %v %v", info, statErr)
	}
}

func TestHashEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []byte
		want bool
	}{
		{"equal", []byte{1, 2, 3}, []byte{1, 2, 3}, true},
		{"differ-last", []byte{1, 2, 3}, []byte{1, 2, 4}, false},
		{"differ-first", []byte{9, 2, 3}, []byte{1, 2, 3}, false},
		{"shorter", []byte{1, 2}, []byte{1, 2, 3}, false},
		{"longer", []byte{1, 2, 3, 4}, []byte{1, 2, 3}, false},
		{"both-empty", nil, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hashEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("hashEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
