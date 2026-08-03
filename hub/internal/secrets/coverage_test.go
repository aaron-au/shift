package secrets_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
)

const plaintext = "correct-horse-battery-staple"

// envelopeOf reads back the stored envelope for one secret.
func envelopeOf(t *testing.T, st *store.Store, name string) store.SecretEnvelope {
	t.Helper()
	envs, err := st.SecretEnvelopes(t.Context(), []string{name})
	if err != nil || len(envs) != 1 {
		t.Fatalf("SecretEnvelopes(%q) = %+v, %v", name, envs, err)
	}
	return envs[0]
}

// TestListReturnsMetadataOnly: the admin listing exposes names and versions
// and has no channel for a value — plaintext is write-only by design.
func TestListReturnsMetadataOnly(t *testing.T) {
	svc, _ := openService(t, writeKey(t))
	ctx := t.Context()

	if metas, err := svc.List(ctx); err != nil || len(metas) != 0 {
		t.Fatalf("List on an empty store = %+v, %v", metas, err)
	}

	if _, err := svc.Put(ctx, "alpha", []byte(plaintext), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Put(ctx, "alpha", []byte("rotated"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Put(ctx, "beta", []byte("b"), ""); err != nil {
		t.Fatal(err)
	}

	metas, err := svc.List(ctx)
	if err != nil || len(metas) != 2 {
		t.Fatalf("List = %+v, %v (want 2)", metas, err)
	}
	// Sorted by name; the replace bumped alpha to version 2.
	if metas[0].Name != "alpha" || metas[0].Version != 2 {
		t.Fatalf("metas[0] = %+v, want alpha v2", metas[0])
	}
	if metas[1].Name != "beta" || metas[1].Version != 1 {
		t.Fatalf("metas[1] = %+v, want beta v1", metas[1])
	}
	if metas[0].Updated.Before(metas[0].Created) {
		t.Fatalf("alpha updated_at %v precedes created_at %v", metas[0].Updated, metas[0].Created)
	}

	// Deleting removes it from the listing.
	if err := svc.Delete(ctx, "beta"); err != nil {
		t.Fatal(err)
	}
	if metas, err := svc.List(ctx); err != nil || len(metas) != 1 || metas[0].Name != "alpha" {
		t.Fatalf("List after delete = %+v, %v", metas, err)
	}
	// Deleting what is not there is a miss, not a silent success.
	if err := svc.Delete(ctx, "beta"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Delete(missing) = %v, want ErrNotFound", err)
	}
}

// TestResolveFailsWithoutTheWrappingKEK: losing (or not holding) the KEK that
// wrapped a DEK makes the value unrecoverable — that is the whole point of
// envelope encryption. The error names the secret and never its value.
func TestResolveFailsWithoutTheWrappingKEK(t *testing.T) {
	keyA, keyB := writeKey(t), writeKey(t)
	svc, st := openService(t, keyA)
	ctx := t.Context()

	if _, err := svc.Put(ctx, "api_key", []byte(plaintext), ""); err != nil {
		t.Fatal(err)
	}

	// A service holding only key B cannot open an envelope wrapped by key A.
	providerB, err := kek.NewLocalFiles(keyB)
	if err != nil {
		t.Fatal(err)
	}
	_, err = secrets.New(st, providerB).Resolve(ctx, []string{"api_key"})
	if err == nil {
		t.Fatal("resolved an envelope wrapped by a KEK the service does not hold")
	}
	if !errors.Is(err, kek.ErrUnknownKEK) {
		t.Fatalf("err = %v, want it to wrap kek.ErrUnknownKEK", err)
	}
	if !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("err = %q, want it to name the secret", err)
	}
	if strings.Contains(err.Error(), plaintext) {
		t.Fatal("error leaked the secret value")
	}

	// The original service still resolves it — the data was never harmed.
	got, err := svc.Resolve(ctx, []string{"api_key"})
	if err != nil || got["api_key"] != plaintext {
		t.Fatalf("owner Resolve = %v, %v", got, err)
	}
}

// TestResolveRejectsTamperedCiphertext: AES-GCM authenticates, so a byte
// flipped in the DB is a hard failure, never a silently corrupted value.
func TestResolveRejectsTamperedCiphertext(t *testing.T) {
	svc, st := openService(t, writeKey(t))
	ctx := t.Context()

	if _, err := svc.Put(ctx, "api_key", []byte(plaintext), ""); err != nil {
		t.Fatal(err)
	}
	e := envelopeOf(t, st, "api_key")

	tampered := make([]byte, len(e.Ciphertext))
	copy(tampered, e.Ciphertext)
	tampered[len(tampered)-1] ^= 0xff // flip a bit in the GCM tag
	if _, _, err := st.UpsertSecret(ctx, "api_key", tampered, e.WrappedDEK, e.KEKID, ""); err != nil {
		t.Fatal(err)
	}

	_, err := svc.Resolve(ctx, []string{"api_key"})
	if err == nil {
		t.Fatal("a tampered ciphertext opened cleanly — authentication is not enforced")
	}
	if !strings.Contains(err.Error(), "authentication failed") || !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("err = %q, want an authentication failure naming the secret", err)
	}
	if strings.Contains(err.Error(), plaintext) {
		t.Fatal("error leaked the secret value")
	}
}

// TestResolveRejectsTruncatedEnvelope: an envelope too short to even carry a
// nonce is reported as malformed rather than panicking on a slice bound.
func TestResolveRejectsTruncatedEnvelope(t *testing.T) {
	svc, st := openService(t, writeKey(t))
	ctx := t.Context()

	if _, err := svc.Put(ctx, "api_key", []byte(plaintext), ""); err != nil {
		t.Fatal(err)
	}
	e := envelopeOf(t, st, "api_key")
	// Keep the real wrapped DEK so unwrapping succeeds and the length check
	// is what actually fires.
	if _, _, err := st.UpsertSecret(ctx, "api_key", []byte("\x00\x01\x02"), e.WrappedDEK, e.KEKID, ""); err != nil {
		t.Fatal(err)
	}

	_, err := svc.Resolve(ctx, []string{"api_key"})
	if err == nil {
		t.Fatal("a truncated envelope resolved cleanly")
	}
	if !strings.Contains(err.Error(), "malformed") || !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("err = %q, want a malformed-envelope error naming the secret", err)
	}
}

// TestRotateKEKStopsOnUnrecoverableEnvelope: rotation is a per-envelope loop,
// so an envelope wrapped by a KEK the provider does not hold aborts the pass,
// reports how many were rewrapped, and leaves the untouched ones intact.
func TestRotateKEKStopsOnUnrecoverableEnvelope(t *testing.T) {
	keyA, keyB := writeKey(t), writeKey(t)
	svc, st := openService(t, keyA)
	ctx := t.Context()

	if _, err := svc.Put(ctx, "api_key", []byte(plaintext), ""); err != nil {
		t.Fatal(err)
	}
	before := envelopeOf(t, st, "api_key")

	// A provider that holds ONLY the new key: it cannot unwrap the old DEK.
	providerB, err := kek.NewLocalFiles(keyB)
	if err != nil {
		t.Fatal(err)
	}
	n, err := secrets.New(st, providerB).RotateKEK(ctx)
	if err == nil {
		t.Fatal("RotateKEK reported success while unable to unwrap the old DEK")
	}
	if n != 0 {
		t.Fatalf("rewrapped = %d, want 0 (nothing could be unwrapped)", n)
	}
	if !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("err = %q, want it to name the secret it stalled on", err)
	}
	if strings.Contains(err.Error(), plaintext) {
		t.Fatal("error leaked the secret value")
	}

	// The envelope is untouched, so the original service still resolves it —
	// a failed rotation must not destroy recoverable secrets.
	after := envelopeOf(t, st, "api_key")
	if string(after.WrappedDEK) != string(before.WrappedDEK) || after.KEKID != before.KEKID {
		t.Fatal("a failed rotation mutated the envelope")
	}
	got, err := svc.Resolve(ctx, []string{"api_key"})
	if err != nil || got["api_key"] != plaintext {
		t.Fatalf("Resolve after failed rotation = %v, %v", got, err)
	}
}

// TestResolveNoNamesIsEmpty: the no-op path short-circuits before touching the
// store (the runner asks for nothing when a flow references no secrets).
func TestResolveNoNamesIsEmpty(t *testing.T) {
	svc, _ := openService(t, writeKey(t))
	got, err := svc.Resolve(t.Context(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("Resolve(nil) = %v, %v (want an empty map)", got, err)
	}
}
