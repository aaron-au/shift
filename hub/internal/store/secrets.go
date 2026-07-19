package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// SecretMeta is what admin reads see — never a value, never key
// material.
type SecretMeta struct {
	Name    string    `json:"name"`
	Version int       `json:"version"`
	Created time.Time `json:"created_at"`
	Updated time.Time `json:"updated_at"`
}

// SecretEnvelope is the stored encrypted form, consumed only by the
// secrets service (hub/internal/secrets).
type SecretEnvelope struct {
	ID         string
	Name       string
	Ciphertext []byte
	WrappedDEK []byte
	KEKID      string
	Version    int
}

// UpsertSecret stores a new envelope for the named secret, bumping the
// version on replace. createdBy may be empty (break-glass writes).
func (s *Store) UpsertSecret(ctx context.Context, name string, ciphertext, wrappedDEK []byte, kekID, createdBy string) (id string, version int, err error) {
	var by *string
	if createdBy != "" {
		by = &createdBy
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO secrets (id, account_id, name, ciphertext, wrapped_dek, kek_id, created_by)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (account_id, name)
		 DO UPDATE SET ciphertext = $4, wrapped_dek = $5, kek_id = $6,
		               version = secrets.version + 1, updated_at = now()
		 RETURNING id, version`,
		newUUID(), accountID(ctx), name, ciphertext, wrappedDEK, kekID, by).Scan(&id, &version)
	return id, version, err
}

// SecretEnvelopes fetches the named secrets' envelopes. Missing names
// are simply absent from the result — callers decide whether that is an
// error.
func (s *Store) SecretEnvelopes(ctx context.Context, names []string) ([]SecretEnvelope, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, ciphertext, wrapped_dek, kek_id, version FROM secrets
		 WHERE account_id = $1 AND name = ANY($2)`, accountID(ctx), names)
	if err != nil {
		return nil, err
	}
	return scanEnvelopes(rows)
}

// SecretEnvelopesNotWrappedBy lists envelopes still wrapped by a KEK
// other than kekID — the KEK-rotation work list.
func (s *Store) SecretEnvelopesNotWrappedBy(ctx context.Context, kekID string) ([]SecretEnvelope, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, ciphertext, wrapped_dek, kek_id, version FROM secrets
		 WHERE account_id = $1 AND kek_id <> $2`, accountID(ctx), kekID)
	if err != nil {
		return nil, err
	}
	return scanEnvelopes(rows)
}

// UpdateSecretWrap swaps a secret's wrapped DEK after KEK rotation.
// The ciphertext is untouched — that is the point of envelopes.
func (s *Store) UpdateSecretWrap(ctx context.Context, id string, wrappedDEK []byte, kekID string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE secrets SET wrapped_dek = $2, kek_id = $3, updated_at = now()
		 WHERE id = $1 AND account_id = $4`, id, wrappedDEK, kekID, accountID(ctx))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Secrets lists the account's secret metadata (names and versions only).
func (s *Store) Secrets(ctx context.Context) ([]SecretMeta, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, version, created_at, updated_at FROM secrets
		 WHERE account_id = $1 ORDER BY name`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SecretMeta
	for rows.Next() {
		var m SecretMeta
		if err := rows.Scan(&m.Name, &m.Version, &m.Created, &m.Updated); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteSecret removes the named secret.
func (s *Store) DeleteSecret(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM secrets WHERE account_id = $1 AND name = $2`, accountID(ctx), name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func scanEnvelopes(rows pgx.Rows) ([]SecretEnvelope, error) {
	defer rows.Close()
	var out []SecretEnvelope
	for rows.Next() {
		var e SecretEnvelope
		if err := rows.Scan(&e.ID, &e.Name, &e.Ciphertext, &e.WrappedDEK, &e.KEKID, &e.Version); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
