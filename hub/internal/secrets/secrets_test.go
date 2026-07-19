package secrets_test

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
)

func writeKey(t *testing.T) string {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func openService(t *testing.T, keyPaths ...string) (*secrets.Service, *store.Store) {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, err := kek.NewLocalFiles(keyPaths[0], keyPaths[1:]...)
	if err != nil {
		t.Fatal(err)
	}
	return secrets.New(st, p), st
}

func TestPutResolveDelete(t *testing.T) {
	svc, _ := openService(t, writeKey(t))
	ctx := t.Context()

	if v, err := svc.Put(ctx, "api_key", []byte("s3cr3t-value"), ""); err != nil || v != 1 {
		t.Fatalf("Put: v=%d err=%v", v, err)
	}
	got, err := svc.Resolve(ctx, []string{"api_key"})
	if err != nil || got["api_key"] != "s3cr3t-value" {
		t.Fatalf("Resolve = %v, %v", got, err)
	}

	// Replace: version bumps, new value resolves.
	if v, err := svc.Put(ctx, "api_key", []byte("rotated-value"), ""); err != nil || v != 2 {
		t.Fatalf("replace: v=%d err=%v", v, err)
	}
	got, err = svc.Resolve(ctx, []string{"api_key"})
	if err != nil || got["api_key"] != "rotated-value" {
		t.Fatalf("post-replace Resolve = %v, %v", got, err)
	}

	// Missing names error carries names only, never values.
	_, err = svc.Resolve(ctx, []string{"api_key", "nope", "also_nope"})
	var missing *secrets.MissingError
	if !errors.As(err, &missing) || len(missing.Names) != 2 {
		t.Fatalf("missing err = %v", err)
	}
	if strings.Contains(err.Error(), "rotated-value") {
		t.Fatal("error leaked a secret value")
	}

	if err := svc.Delete(ctx, "api_key"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resolve(ctx, []string{"api_key"}); err == nil {
		t.Fatal("resolved a deleted secret")
	}
}

func TestEnvelopeSwapFailsAuthentication(t *testing.T) {
	svc, st := openService(t, writeKey(t))
	ctx := t.Context()

	if _, err := svc.Put(ctx, "alpha", []byte("value-a"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Put(ctx, "beta", []byte("value-b"), ""); err != nil {
		t.Fatal(err)
	}

	// Graft alpha's envelope contents onto beta: decryption must fail
	// (the AAD binds ciphertext to the secret's name).
	envs, err := st.SecretEnvelopes(ctx, []string{"alpha", "beta"})
	if err != nil || len(envs) != 2 {
		t.Fatalf("envelopes: %v, %v", envs, err)
	}
	byName := map[string]store.SecretEnvelope{envs[0].Name: envs[0], envs[1].Name: envs[1]}
	a, b := byName["alpha"], byName["beta"]
	if _, _, err := st.UpsertSecret(ctx, "beta", a.Ciphertext, a.WrappedDEK, a.KEKID, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resolve(ctx, []string{"beta"}); err == nil {
		t.Fatal("swapped envelope resolved cleanly — AAD binding is broken")
	}
	_ = b
}

func TestRotateKEK(t *testing.T) {
	oldKey, newKey := writeKey(t), writeKey(t)
	svc, st := openService(t, oldKey)
	ctx := t.Context()

	for _, name := range []string{"one", "two", "three"} {
		if _, err := svc.Put(ctx, name, []byte("value-"+name), ""); err != nil {
			t.Fatal(err)
		}
	}

	// New provider: rotated active key, old key retained.
	rotated, err := kek.NewLocalFiles(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := secrets.New(st, rotated)
	n, err := svc2.RotateKEK(ctx)
	if err != nil || n != 3 {
		t.Fatalf("RotateKEK = %d, %v (want 3)", n, err)
	}
	// Idempotent: nothing left to rewrap.
	if n, err := svc2.RotateKEK(ctx); err != nil || n != 0 {
		t.Fatalf("second RotateKEK = %d, %v (want 0)", n, err)
	}

	// Old key now removable: a provider holding ONLY the new key
	// resolves everything.
	fresh, err := kek.NewLocalFiles(newKey)
	if err != nil {
		t.Fatal(err)
	}
	got, err := secrets.New(st, fresh).Resolve(ctx, []string{"one", "two", "three"})
	if err != nil || got["two"] != "value-two" {
		t.Fatalf("post-rotation Resolve = %v, %v", got, err)
	}
}
