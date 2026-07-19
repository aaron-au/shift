package consign

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
)

func testManifest() Manifest {
	return Manifest{
		Name:    "http",
		Version: "1.2.0",
		OS:      "linux",
		Arch:    "arm64",
		Digest:  sha256.Sum256([]byte("artifact-bytes")),
	}
}

// TestMessageGolden freezes the canonical signing payload. If this test
// breaks, every existing signature in every registry breaks with it —
// bump the version tag instead of editing the format.
func TestMessageGolden(t *testing.T) {
	want := "shift-connector-artifact-v1\n" +
		"http\n" +
		"1.2.0\n" +
		"linux/arm64\n" +
		"sha256:6521df166eb07efaf36eba5b6bedefd9d6a252e9c80bab1c99653700ec71473c\n"
	got := string(testManifest().Message())
	if got != want {
		t.Fatalf("canonical message changed:\ngot  %q\nwant %q", got, want)
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := testManifest()
	sig := Sign(priv, m)
	if len(sig) != ed25519.SignatureSize {
		t.Fatalf("signature size = %d, want %d", len(sig), ed25519.SignatureSize)
	}
	if err := Verify(pub, m, sig); err != nil {
		t.Fatalf("Verify() on valid signature: %v", err)
	}
}

func TestVerifyTamper(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sig := Sign(priv, testManifest())

	tampered := map[string]func(*Manifest){
		"name":    func(m *Manifest) { m.Name = "http2" },
		"version": func(m *Manifest) { m.Version = "1.2.1" },
		"os":      func(m *Manifest) { m.OS = "darwin" },
		"arch":    func(m *Manifest) { m.Arch = "amd64" },
		"digest":  func(m *Manifest) { m.Digest[0] ^= 0xff },
	}
	for field, mutate := range tampered {
		m := testManifest()
		mutate(&m)
		if err := Verify(pub, m, sig); !errors.Is(err, ErrBadSignature) {
			t.Errorf("tampered %s: Verify() = %v, want ErrBadSignature", field, err)
		}
	}

	m := testManifest()
	badSig := append([]byte(nil), sig...)
	badSig[0] ^= 0xff
	if err := Verify(pub, m, badSig); !errors.Is(err, ErrBadSignature) {
		t.Errorf("tampered signature: Verify() = %v, want ErrBadSignature", err)
	}

	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(otherPub, m, sig); !errors.Is(err, ErrBadSignature) {
		t.Errorf("wrong key: Verify() = %v, want ErrBadSignature", err)
	}
}

func TestVerifyMalformedInputs(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := testManifest()
	sig := Sign(priv, m)

	if err := Verify(pub[:16], m, sig); !errors.Is(err, ErrBadSignature) {
		t.Errorf("short key: Verify() = %v, want ErrBadSignature", err)
	}
	if err := Verify(pub, m, sig[:32]); !errors.Is(err, ErrBadSignature) {
		t.Errorf("short signature: Verify() = %v, want ErrBadSignature", err)
	}
	if err := Verify(nil, m, nil); !errors.Is(err, ErrBadSignature) {
		t.Errorf("nil inputs: Verify() = %v, want ErrBadSignature", err)
	}
}

func TestHashReader(t *testing.T) {
	content := strings.Repeat("shift", 1000)
	digest, size, err := HashReader(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}
	want := sha256.Sum256([]byte(content))
	if !bytes.Equal(digest[:], want[:]) {
		t.Fatalf("digest mismatch")
	}
}
