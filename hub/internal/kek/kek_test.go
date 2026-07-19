package kek

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeKey(t *testing.T, perm os.FileMode) string {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(path, key, perm); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWrapUnwrapRoundTrip(t *testing.T) {
	p, err := NewLocalFiles(writeKey(t, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	dek := []byte("0123456789abcdef0123456789abcdef")
	wrapped, id, err := p.Wrap(t.Context(), dek)
	if err != nil || id != p.ActiveID() {
		t.Fatalf("Wrap: id=%q err=%v", id, err)
	}
	got, err := p.Unwrap(t.Context(), id, wrapped)
	if err != nil || string(got) != string(dek) {
		t.Fatalf("Unwrap: %q, %v", got, err)
	}
}

func TestUnwrapWrongKeyFails(t *testing.T) {
	p1, err := NewLocalFiles(writeKey(t, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := NewLocalFiles(writeKey(t, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	wrapped, id, err := p1.Wrap(t.Context(), []byte("secret-dek"))
	if err != nil {
		t.Fatal(err)
	}
	// p2 doesn't hold p1's key id at all.
	if _, err := p2.Unwrap(t.Context(), id, wrapped); !errors.Is(err, ErrUnknownKEK) {
		t.Fatalf("foreign key id: err = %v, want ErrUnknownKEK", err)
	}
	// Same id, tampered ciphertext must fail authentication.
	wrapped[len(wrapped)-1] ^= 0xff
	if _, err := p1.Unwrap(t.Context(), id, wrapped); err == nil {
		t.Fatal("tampered wrap unwrapped cleanly")
	}
}

func TestRotationUnwrapsOldWrapsNew(t *testing.T) {
	oldKey := writeKey(t, 0o600)
	newKey := writeKey(t, 0o600)

	oldP, err := NewLocalFiles(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, oldID, err := oldP.Wrap(t.Context(), []byte("dek"))
	if err != nil {
		t.Fatal(err)
	}

	// Rotated provider: new active, old retained for unwrapping.
	rot, err := NewLocalFiles(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	if rot.ActiveID() == oldID {
		t.Fatal("active id did not change on rotation")
	}
	dek, err := rot.Unwrap(t.Context(), oldID, wrapped)
	if err != nil || string(dek) != "dek" {
		t.Fatalf("unwrap old under rotated provider: %q, %v", dek, err)
	}
	if _, id, err := rot.Wrap(t.Context(), dek); err != nil || id != rot.ActiveID() {
		t.Fatalf("rewrap: id=%q err=%v (want active)", id, err)
	}
}

func TestKeyFileValidation(t *testing.T) {
	if _, err := NewLocalFiles(writeKey(t, 0o644)); err == nil {
		t.Fatal("world-readable key file accepted")
	}
	short := filepath.Join(t.TempDir(), "short.bin")
	if err := os.WriteFile(short, []byte("too-short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLocalFiles(short); err == nil {
		t.Fatal("short key file accepted")
	}
	if _, err := NewLocalFiles(filepath.Join(t.TempDir(), "missing.bin")); err == nil {
		t.Fatal("missing key file accepted")
	}
}
