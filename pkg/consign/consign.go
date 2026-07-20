// Package consign implements Ed25519 signing of connector artifacts.
//
// A signature covers a canonical Manifest — the artifact's identity
// (name, version, os/arch) bound to its SHA-256 content digest — never
// the raw bytes alone. Signing only the digest would let a valid
// artifact A be republished under artifact B's identity with the same
// key. Private keys belong to publishers and never reach the hub; the
// hub and runners hold public keys only.
package consign

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// ErrBadSignature is returned by Verify when the signature does not
// match the manifest under the given public key.
var ErrBadSignature = errors.New("consign: signature verification failed")

// Manifest binds a connector artifact's identity to its content digest.
type Manifest struct {
	Name    string   // connector name (runner connpool naming rules)
	Version string   // publisher-chosen version string
	OS      string   // GOOS
	Arch    string   // GOARCH
	Digest  [32]byte // SHA-256 of the artifact bytes

	// DescriptorDigest is the SHA-256 of the connector's opaque descriptor
	// blob (action config schemas — ADR-0018). Zero when the artifact
	// carries no descriptor, in which case the canonical message stays the
	// byte-identical v1 form and existing signatures remain valid. When
	// non-zero the message bumps to the v2 form so the descriptor is signed
	// alongside identity + artifact digest.
	DescriptorDigest [32]byte
}

// zeroDigest is the sentinel for "no descriptor" (v1 artifacts).
var zeroDigest [32]byte

// Message renders the canonical signing payload. The leading version
// tag makes the scheme evolvable: a format bump changes the tag and old
// signatures cannot be replayed against it. A manifest without a
// descriptor digest renders the original v1 form so pre-descriptor
// artifacts stay verifiable; one with a descriptor digest renders v2.
func (m Manifest) Message() []byte {
	if m.DescriptorDigest == zeroDigest {
		return fmt.Appendf(nil, "shift-connector-artifact-v1\n%s\n%s\n%s/%s\nsha256:%x\n",
			m.Name, m.Version, m.OS, m.Arch, m.Digest)
	}
	return fmt.Appendf(nil, "shift-connector-artifact-v2\n%s\n%s\n%s/%s\nsha256:%x\ndescriptor-sha256:%x\n",
		m.Name, m.Version, m.OS, m.Arch, m.Digest, m.DescriptorDigest)
}

// Sign returns a 64-byte detached Ed25519 signature over the manifest.
func Sign(priv ed25519.PrivateKey, m Manifest) []byte {
	return ed25519.Sign(priv, m.Message())
}

// Verify checks a detached signature over the manifest. It fails
// closed: any malformed key or signature is ErrBadSignature.
func Verify(pub ed25519.PublicKey, m Manifest, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return ErrBadSignature
	}
	if !ed25519.Verify(pub, m.Message(), sig) {
		return ErrBadSignature
	}
	return nil
}

// HashReader consumes r and returns the SHA-256 digest and byte count
// of its content.
func HashReader(r io.Reader) (digest [32]byte, size int64, err error) {
	h := sha256.New()
	size, err = io.Copy(h, r)
	if err != nil {
		return digest, 0, fmt.Errorf("consign: hashing artifact: %w", err)
	}
	copy(digest[:], h.Sum(nil))
	return digest, size, nil
}
