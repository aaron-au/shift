// Package kek abstracts the key-encryption-key used to wrap per-secret
// DEKs (envelope encryption). The interface is deliberately tiny so a
// cloud KMS provider can replace the local file provider without any
// schema or service change: Wrap always uses the active key, Unwrap
// selects by the kek_id recorded on the envelope — which is also the
// whole rotation mechanism (add a new active key, re-wrap, retire).
package kek

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// ErrUnknownKEK means an envelope names a KEK this provider does not
// hold — typically a retired key removed before rotation finished.
var ErrUnknownKEK = errors.New("kek: unknown key id")

// Provider wraps and unwraps data-encryption keys.
type Provider interface {
	// ActiveID identifies the key new wraps use.
	ActiveID() string
	// Wrap encrypts a DEK under the active KEK.
	Wrap(ctx context.Context, dek []byte) (wrapped []byte, kekID string, err error)
	// Unwrap decrypts a DEK wrapped by the identified KEK.
	Unwrap(ctx context.Context, kekID string, wrapped []byte) ([]byte, error)
}

// localFiles is the dev/self-hosted provider: 32-byte raw key files.
type localFiles struct {
	active string
	keys   map[string]cipher.AEAD
}

// NewLocalFiles loads the active KEK plus any number of old KEKs still
// needed to unwrap not-yet-rotated envelopes. Key files must be exactly
// 32 raw bytes with 0600 permissions.
func NewLocalFiles(activePath string, oldPaths ...string) (Provider, error) {
	p := &localFiles{keys: make(map[string]cipher.AEAD)}
	for i, path := range append([]string{activePath}, oldPaths...) {
		id, aead, err := loadKeyFile(path)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			p.active = id
		}
		p.keys[id] = aead
	}
	return p, nil
}

func loadKeyFile(path string) (string, cipher.AEAD, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, fmt.Errorf("kek: %w", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return "", nil, fmt.Errorf("kek: %s has mode %04o; refuse keys readable by group/other (chmod 600)", path, perm)
	}
	key, err := os.ReadFile(path) //nolint:gosec // G304: operator-configured KEK path (flag/env)
	if err != nil {
		return "", nil, fmt.Errorf("kek: %w", err)
	}
	if len(key) != 32 {
		return "", nil, fmt.Errorf("kek: %s is %d bytes; a KEK file is exactly 32 raw bytes", path, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(key)
	return "local-" + hex.EncodeToString(sum[:4]), aead, nil
}

func (p *localFiles) ActiveID() string { return p.active }

func (p *localFiles) Wrap(_ context.Context, dek []byte) ([]byte, string, error) {
	aead := p.keys[p.active]
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", err
	}
	return aead.Seal(nonce, nonce, dek, []byte(p.active)), p.active, nil
}

func (p *localFiles) Unwrap(_ context.Context, kekID string, wrapped []byte) ([]byte, error) {
	aead, ok := p.keys[kekID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKEK, kekID)
	}
	if len(wrapped) < aead.NonceSize() {
		return nil, errors.New("kek: wrapped DEK too short")
	}
	dek, err := aead.Open(nil, wrapped[:aead.NonceSize()], wrapped[aead.NonceSize():], []byte(kekID))
	if err != nil {
		return nil, fmt.Errorf("kek: unwrap under %s: %w", kekID, err)
	}
	return dek, nil
}
