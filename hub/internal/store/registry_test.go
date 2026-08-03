package store_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

// addKey registers a fresh Ed25519 publisher key and returns its row id.
func addKey(t *testing.T, s *store.Store, ctx context.Context, name string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.AddPublisherKey(ctx, name, pub)
	if err != nil {
		t.Fatalf("AddPublisherKey(%q): %v", name, err)
	}
	return id
}

// putArtifact stores one connector version whose blob is `data`, returning the
// content digest the store keyed it by.
func putArtifact(t *testing.T, s *store.Store, ctx context.Context, keyID, name, version, osName, arch string, data []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(data)
	if err := s.PutConnectorVersion(ctx, name, version, osName, arch,
		sum[:], []byte("signature-"+version), keyID, data, nil); err != nil {
		t.Fatalf("PutConnectorVersion(%s@%s): %v", name, version, err)
	}
	return sum[:]
}

// TestPublisherKeyLifecycle pins the trust bootstrap: only well-formed
// Ed25519 keys are accepted, lookups are by name, and revocation is a
// one-way door that removes the key from every trust-serving query.
func TestPublisherKeyLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	// Wrong-sized key material is rejected before it can ever be trusted.
	if _, err := s.AddPublisherKey(ctx, "short", []byte("not-32-bytes")); err == nil {
		t.Fatal("AddPublisherKey accepted a key that is not ed25519.PublicKeySize")
	}

	id := addKey(t, s, ctx, "pub1")

	k, err := s.PublisherKeyByName(ctx, "pub1")
	if err != nil {
		t.Fatalf("PublisherKeyByName: %v", err)
	}
	if k.ID != id || k.Name != "pub1" || len(k.PublicKey) != ed25519.PublicKeySize || k.Revoked != nil {
		t.Fatalf("key = %+v (want id %s, 32-byte pub, not revoked)", k, id)
	}
	if _, err := s.PublisherKeyByName(ctx, "ghost"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("PublisherKeyByName(ghost) = %v, want ErrNotFound", err)
	}

	keys, err := s.TrustedKeys(ctx)
	if err != nil || len(keys) != 1 || keys[0].ID != id {
		t.Fatalf("TrustedKeys = %+v, %v (want the one key)", keys, err)
	}

	// Revoking an id that does not exist must not report success.
	if err := s.RevokePublisherKey(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoke unknown id = %v, want ErrNotFound", err)
	}

	if err := s.RevokePublisherKey(ctx, id); err != nil {
		t.Fatalf("RevokePublisherKey: %v", err)
	}
	// Revocation is idempotent-rejecting: the second call finds no live row.
	if err := s.RevokePublisherKey(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double revoke = %v, want ErrNotFound", err)
	}
	// A revoked key is invisible to both trust-serving lookups.
	if keys, err := s.TrustedKeys(ctx); err != nil || len(keys) != 0 {
		t.Fatalf("TrustedKeys after revoke = %+v, %v (want none)", keys, err)
	}
	if _, err := s.PublisherKeyByName(ctx, "pub1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("PublisherKeyByName after revoke = %v, want ErrNotFound", err)
	}
}

// TestConnectorPublishAndResolve covers the publish→resolve→fetch chain a
// runner walks: latest resolves to the newest publish, an explicit version
// pins, and anything unknown is ErrNotFound rather than a silent zero value.
func TestConnectorPublishAndResolve(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")

	d100 := putArtifact(t, s, ctx, keyID, "myconn", "1.0.0", "linux", "amd64", []byte("artifact-1.0.0"))
	d110 := putArtifact(t, s, ctx, keyID, "myconn", "1.1.0", "linux", "amd64", []byte("artifact-1.1.0"))

	// "" and "latest" both mean newest publish (registry order, not semver).
	for _, v := range []string{"", "latest"} {
		cv, err := s.ResolveConnector(ctx, "myconn", v, "linux", "amd64")
		if err != nil {
			t.Fatalf("ResolveConnector(%q): %v", v, err)
		}
		if cv.Version != "1.1.0" {
			t.Fatalf("ResolveConnector(%q) = %s, want 1.1.0", v, cv.Version)
		}
		if string(cv.Digest) != string(d110) {
			t.Fatalf("resolved digest does not match the published artifact")
		}
		if len(cv.PublisherKey) != ed25519.PublicKeySize {
			t.Fatalf("resolve did not join the publisher key (len %d)", len(cv.PublisherKey))
		}
		if cv.SizeBytes != int64(len("artifact-1.1.0")) {
			t.Fatalf("size_bytes = %d, want %d", cv.SizeBytes, len("artifact-1.1.0"))
		}
	}
	// An explicit version pins even though a newer one exists.
	cv, err := s.ResolveConnector(ctx, "myconn", "1.0.0", "linux", "amd64")
	if err != nil || cv.Version != "1.0.0" || string(cv.Digest) != string(d100) {
		t.Fatalf("pinned resolve = %+v, %v", cv, err)
	}

	// Unknown name / version / platform all fail closed.
	for _, tc := range []struct{ name, version, os, arch string }{
		{"ghost", "latest", "linux", "amd64"},
		{"myconn", "9.9.9", "linux", "amd64"},
		{"myconn", "latest", "windows", "amd64"},
		{"myconn", "latest", "linux", "riscv"},
		{"myconn", "latest", "", ""},
	} {
		if _, err := s.ResolveConnector(ctx, tc.name, tc.version, tc.os, tc.arch); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ResolveConnector(%q,%q,%q,%q) = %v, want ErrNotFound", tc.name, tc.version, tc.os, tc.arch, err)
		}
	}

	// Blob fetch is by content digest.
	blob, err := s.ConnectorBlob(ctx, d110)
	if err != nil || string(blob) != "artifact-1.1.0" {
		t.Fatalf("ConnectorBlob = %q, %v", blob, err)
	}
	unknown := sha256.Sum256([]byte("never-published"))
	if _, err := s.ConnectorBlob(ctx, unknown[:]); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ConnectorBlob(unknown) = %v, want ErrNotFound", err)
	}
}

// TestPutConnectorVersionDedupesBlobsAndReplaces: republishing the same
// (name,version,os,arch) overwrites the row (digest/signature) rather than
// duplicating it, and identical bytes under two versions share one blob.
func TestPutConnectorVersionDedupesBlobsAndReplaces(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")

	same := []byte("identical-artifact-bytes")
	dA := putArtifact(t, s, ctx, keyID, "myconn", "1.0.0", "linux", "amd64", same)
	dB := putArtifact(t, s, ctx, keyID, "myconn", "1.0.1", "linux", "amd64", same)
	if string(dA) != string(dB) {
		t.Fatal("identical bytes produced different digests")
	}
	// Both versions resolve, both served from the one deduped blob row.
	for _, v := range []string{"1.0.0", "1.0.1"} {
		cv, err := s.ResolveConnector(ctx, "myconn", v, "linux", "amd64")
		if err != nil || string(cv.Digest) != string(dA) {
			t.Fatalf("resolve %s = %+v, %v", v, cv, err)
		}
	}

	// Republish 1.0.0 with different bytes: the version row is replaced.
	dNew := putArtifact(t, s, ctx, keyID, "myconn", "1.0.0", "linux", "amd64", []byte("rebuilt-artifact"))
	cv, err := s.ResolveConnector(ctx, "myconn", "1.0.0", "linux", "amd64")
	if err != nil || string(cv.Digest) != string(dNew) {
		t.Fatalf("republish did not replace the digest: %+v, %v", cv, err)
	}
	vs, err := s.ConnectorVersions(ctx, "myconn")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 2 {
		t.Fatalf("ConnectorVersions = %d rows, want 2 (republish must update, not append)", len(vs))
	}
}

// TestConnectorYankLifecycle: a yanked version vanishes from resolve and from
// the newest-per-connector listing (fail closed), but stays in the version
// history for provenance; restore puts it back.
func TestConnectorYankLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")

	putArtifact(t, s, ctx, keyID, "myconn", "1.0.0", "linux", "amd64", []byte("artifact-1.0.0"))
	putArtifact(t, s, ctx, keyID, "myconn", "1.1.0", "linux", "amd64", []byte("artifact-1.1.0"))

	if err := s.SetConnectorYanked(ctx, "myconn", "1.1.0", "linux", "amd64", true); err != nil {
		t.Fatalf("yank: %v", err)
	}
	// Yanking again finds no live row — no silent double-yank.
	if err := s.SetConnectorYanked(ctx, "myconn", "1.1.0", "linux", "amd64", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double yank = %v, want ErrNotFound", err)
	}
	// Yanking something that was never published is ErrNotFound.
	if err := s.SetConnectorYanked(ctx, "myconn", "9.9.9", "linux", "amd64", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("yank unknown version = %v, want ErrNotFound", err)
	}

	// Resolve falls back to the newest non-yanked version.
	cv, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64")
	if err != nil || cv.Version != "1.0.0" {
		t.Fatalf("resolve after yank = %+v, %v (want 1.0.0)", cv, err)
	}
	// Resolving the yanked version explicitly must also fail closed.
	if _, err := s.ResolveConnector(ctx, "myconn", "1.1.0", "linux", "amd64"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("explicit resolve of yanked version = %v, want ErrNotFound", err)
	}

	// History keeps both rows, newest first, with the yank timestamp set.
	vs, err := s.ConnectorVersions(ctx, "myconn")
	if err != nil || len(vs) != 2 {
		t.Fatalf("ConnectorVersions = %+v, %v (want 2)", vs, err)
	}
	if vs[0].Version != "1.1.0" || vs[0].Yanked == nil {
		t.Fatalf("history[0] = %+v, want yanked 1.1.0 first", vs[0])
	}
	if vs[1].Version != "1.0.0" || vs[1].Yanked != nil {
		t.Fatalf("history[1] = %+v, want live 1.0.0", vs[1])
	}
	// Newest-per-connector listing shows the live version only.
	list, err := s.Connectors(ctx)
	if err != nil || len(list) != 1 || list[0].Version != "1.0.0" {
		t.Fatalf("Connectors after yank = %+v, %v (want just 1.0.0)", list, err)
	}

	// Restore: the version is resolvable again; restoring twice is ErrNotFound.
	if err := s.SetConnectorYanked(ctx, "myconn", "1.1.0", "linux", "amd64", false); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := s.SetConnectorYanked(ctx, "myconn", "1.1.0", "linux", "amd64", false); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double restore = %v, want ErrNotFound", err)
	}
	if cv, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64"); err != nil || cv.Version != "1.1.0" {
		t.Fatalf("resolve after restore = %+v, %v (want 1.1.0)", cv, err)
	}

	// ConnectorVersions for a connector that was never published is ErrNotFound,
	// not an empty slice — the API layer turns that into a 404.
	if _, err := s.ConnectorVersions(ctx, "ghost"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ConnectorVersions(ghost) = %v, want ErrNotFound", err)
	}
}

// TestResolveConnectorFailsClosedOnRevokedKey: revoking the signing key makes
// every artifact it signed unresolvable (a runner must never be handed an
// artifact it cannot anchor to a trusted key), while the history keeps the row
// so an operator can still see what was published.
func TestResolveConnectorFailsClosedOnRevokedKey(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")
	putArtifact(t, s, ctx, keyID, "myconn", "1.0.0", "linux", "amd64", []byte("artifact-1.0.0"))

	if _, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64"); err != nil {
		t.Fatalf("pre-revoke resolve: %v", err)
	}
	if err := s.RevokePublisherKey(ctx, keyID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("resolve under a revoked key = %v, want ErrNotFound (fail closed)", err)
	}
	vs, err := s.ConnectorVersions(ctx, "myconn")
	if err != nil || len(vs) != 1 || vs[0].Version != "1.0.0" {
		t.Fatalf("history after revoke = %+v, %v (provenance must survive)", vs, err)
	}
}

// TestConnectorDescriptorRoundTrip: the signed descriptor blob (ADR-0018) is
// stored and served back byte-identically — the hub never parses or rewrites
// it, because the runner re-digests these exact bytes to verify the manifest.
func TestConnectorDescriptorRoundTrip(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")

	desc := []byte("{\"actions\":[{\"name\":\"get\"}]}\x00\x01binary-tail")
	data := []byte("artifact-with-descriptor")
	sum := sha256.Sum256(data)
	if err := s.PutConnectorVersion(ctx, "myconn", "2.0.0", "linux", "amd64",
		sum[:], []byte("sig"), keyID, data, desc); err != nil {
		t.Fatal(err)
	}
	cv, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	if string(cv.Descriptor) != string(desc) {
		t.Fatalf("descriptor round-trip altered the bytes: %q", cv.Descriptor)
	}
	// A v1 (descriptor-free) artifact keeps a nil descriptor, not empty bytes
	// that would change the manifest digest.
	putArtifact(t, s, ctx, keyID, "oldconn", "1.0.0", "linux", "amd64", []byte("v1-artifact"))
	old, err := s.ResolveConnector(ctx, "oldconn", "latest", "linux", "amd64")
	if err != nil || len(old.Descriptor) != 0 {
		t.Fatalf("v1 artifact descriptor = %q, %v (want empty)", old.Descriptor, err)
	}
}

// TestConnectorRegistryTenancy: the registry is account-scoped end to end —
// another tenant can neither resolve, list, enumerate nor download an
// artifact published under a different account, even knowing its digest.
func TestConnectorRegistryTenancy(t *testing.T) {
	s := open(t)
	ctxA := t.Context()

	acctB, err := s.CreateAccount(ctxA, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := store.WithAccount(ctxA, acctB)

	keyID := addKey(t, s, ctxA, "pub1")
	digest := putArtifact(t, s, ctxA, keyID, "myconn", "1.0.0", "linux", "amd64", []byte("tenant-a-artifact"))

	if _, err := s.ResolveConnector(ctxB, "myconn", "latest", "linux", "amd64"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account resolve = %v, want ErrNotFound", err)
	}
	if _, err := s.ConnectorBlob(ctxB, digest); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account blob fetch by digest = %v, want ErrNotFound", err)
	}
	if _, err := s.ConnectorVersions(ctxB, "myconn"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account version history = %v, want ErrNotFound", err)
	}
	if list, err := s.Connectors(ctxB); err != nil || len(list) != 0 {
		t.Fatalf("cross-account Connectors = %+v, %v (want none)", list, err)
	}
	if keys, err := s.TrustedKeys(ctxB); err != nil || len(keys) != 0 {
		t.Fatalf("cross-account TrustedKeys = %+v, %v (want none)", keys, err)
	}
	if _, err := s.PublisherKeyByName(ctxB, "pub1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account key lookup = %v, want ErrNotFound", err)
	}
	// B revoking A's key id must be a no-op miss, not a cross-tenant revoke.
	if err := s.RevokePublisherKey(ctxB, keyID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account revoke = %v, want ErrNotFound", err)
	}
	if _, err := s.ResolveConnector(ctxA, "myconn", "latest", "linux", "amd64"); err != nil {
		t.Fatalf("owner resolve broke after cross-tenant revoke attempt: %v", err)
	}
}

// TestConnectorsListsNewestPerConnector: the dashboard listing collapses each
// connector to its newest version across all published platforms.
func TestConnectorsListsNewestPerConnector(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	keyID := addKey(t, s, ctx, "pub1")

	putArtifact(t, s, ctx, keyID, "alpha", "1.0.0", "linux", "amd64", []byte("alpha-1"))
	putArtifact(t, s, ctx, keyID, "alpha", "2.0.0", "linux", "amd64", []byte("alpha-2"))
	putArtifact(t, s, ctx, keyID, "beta", "0.1.0", "darwin", "arm64", []byte("beta-1"))

	list, err := s.Connectors(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("Connectors = %+v, %v (want 2)", list, err)
	}
	got := map[string]string{}
	for _, cv := range list {
		got[cv.Name] = cv.Version
	}
	if got["alpha"] != "2.0.0" || got["beta"] != "0.1.0" {
		t.Fatalf("Connectors = %v, want alpha 2.0.0 + beta 0.1.0", got)
	}
}
