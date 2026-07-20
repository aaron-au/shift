package consign

import (
	"crypto/ed25519"
	"testing"
)

// FuzzVerify asserts signature verification fails closed on arbitrary bytes
// — never panics, and never accepts garbage. Verify is the fail-closed gate
// runners trust for signed connector artifacts (ADR-0011/0018), so a panic
// or a false-accept here is a supply-chain hole.
func FuzzVerify(f *testing.F) {
	pub, _, _ := ed25519.GenerateKey(nil) // deterministic seed key is fine for a seed
	f.Add([]byte(pub), []byte("sig"), "http", "1.0.0", []byte("digest"))
	f.Add([]byte{}, []byte{}, "", "", []byte{})

	f.Fuzz(func(t *testing.T, pub, sig []byte, name, version string, digest []byte) {
		m := Manifest{Name: name, Version: version, OS: "linux", Arch: "amd64"}
		copy(m.Digest[:], digest)
		if len(digest) > 32 { // also exercise the descriptor (v2) message path
			copy(m.DescriptorDigest[:], digest[16:])
		}
		// Must not panic; garbage must not verify.
		if err := Verify(ed25519.PublicKey(pub), m, sig); err == nil {
			t.Fatalf("fuzzed garbage verified as valid: pub=%d sig=%d", len(pub), len(sig))
		}
	})
}
