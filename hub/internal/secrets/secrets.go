// Package secrets is the hub's envelope-encryption service: each secret
// value is sealed under its own fresh DEK (AES-256-GCM), and the DEK is
// wrapped by the kek.Provider. Plaintext exists only transiently in
// memory here and on the runner that resolves it — it is never stored,
// never listed, and never appears in errors (names only).
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"

	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/store"
)

// Service seals and opens secret envelopes over the store.
type Service struct {
	st  *store.Store
	kek kek.Provider
}

// New builds the service.
func New(st *store.Store, p kek.Provider) *Service { return &Service{st: st, kek: p} }

// MissingError lists requested secret names that do not exist. It
// carries names only — never values.
type MissingError struct{ Names []string }

func (e *MissingError) Error() string {
	return "secrets: not found: " + strings.Join(e.Names, ", ")
}

// Put seals value under a fresh DEK and stores the envelope. Replacing
// a secret bumps its version (the DEK rotates implicitly).
func (s *Service) Put(ctx context.Context, name string, value []byte, createdBy string) (version int, err error) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return 0, err
	}
	aead, err := newAEAD(dek)
	if err != nil {
		return 0, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return 0, err
	}
	// The AAD binds the ciphertext to this secret's identity at seal
	// time: swapping envelope rows in the DB fails authentication.
	// Store assigns id/version, so AAD is the (account-scoped) name.
	ciphertext := aead.Seal(nonce, nonce, value, []byte(name))

	wrapped, kekID, err := s.kek.Wrap(ctx, dek)
	if err != nil {
		return 0, err
	}
	_, version, err = s.st.UpsertSecret(ctx, name, ciphertext, wrapped, kekID, createdBy)
	if err != nil {
		return 0, fmt.Errorf("secrets: storing %q: %w", name, err)
	}
	return version, nil
}

// Resolve decrypts the named secrets. Any missing name is a
// *MissingError; any envelope that fails to open is an error naming the
// secret, not its content.
func (s *Service) Resolve(ctx context.Context, names []string) (map[string]string, error) {
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	envs, err := s.st.SecretEnvelopes(ctx, names)
	if err != nil {
		return nil, err
	}
	found := make(map[string]store.SecretEnvelope, len(envs))
	for _, e := range envs {
		found[e.Name] = e
	}
	var missing []string
	for _, n := range names {
		if _, ok := found[n]; !ok {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, &MissingError{Names: missing}
	}

	out := make(map[string]string, len(envs))
	for _, e := range envs {
		value, err := s.open(ctx, e)
		if err != nil {
			return nil, err
		}
		out[e.Name] = string(value)
	}
	return out, nil
}

// List returns metadata only.
func (s *Service) List(ctx context.Context) ([]store.SecretMeta, error) { return s.st.Secrets(ctx) }

// Delete removes a secret.
func (s *Service) Delete(ctx context.Context, name string) error { return s.st.DeleteSecret(ctx, name) }

// RotateKEK re-wraps every DEK not already under the active KEK. The
// ciphertext never moves — that is the point of envelopes.
func (s *Service) RotateKEK(ctx context.Context) (rewrapped int, err error) {
	stale, err := s.st.SecretEnvelopesNotWrappedBy(ctx, s.kek.ActiveID())
	if err != nil {
		return 0, err
	}
	for _, e := range stale {
		dek, err := s.kek.Unwrap(ctx, e.KEKID, e.WrappedDEK)
		if err != nil {
			return rewrapped, fmt.Errorf("secrets: rotating %q: %w", e.Name, err)
		}
		wrapped, kekID, err := s.kek.Wrap(ctx, dek)
		if err != nil {
			return rewrapped, err
		}
		if err := s.st.UpdateSecretWrap(ctx, e.ID, wrapped, kekID); err != nil {
			return rewrapped, err
		}
		rewrapped++
	}
	return rewrapped, nil
}

func (s *Service) open(ctx context.Context, e store.SecretEnvelope) ([]byte, error) {
	dek, err := s.kek.Unwrap(ctx, e.KEKID, e.WrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("secrets: opening %q: %w", e.Name, err)
	}
	aead, err := newAEAD(dek)
	if err != nil {
		return nil, err
	}
	if len(e.Ciphertext) < aead.NonceSize() {
		return nil, fmt.Errorf("secrets: envelope %q malformed", e.Name)
	}
	value, err := aead.Open(nil, e.Ciphertext[:aead.NonceSize()], e.Ciphertext[aead.NonceSize():], []byte(e.Name))
	if err != nil {
		return nil, fmt.Errorf("secrets: opening %q: authentication failed", e.Name)
	}
	return value, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
