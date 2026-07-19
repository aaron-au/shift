package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PublisherKey is a trusted artifact-signing public key.
type PublisherKey struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	PublicKey []byte     `json:"public_key"`
	Created   time.Time  `json:"created_at"`
	Revoked   *time.Time `json:"revoked_at,omitempty"`
}

// ConnectorVersion is one published artifact (identity + signature; the
// blob itself is fetched separately by digest).
type ConnectorVersion struct {
	Name         string    `json:"name"`
	Version      string    `json:"version"`
	OS           string    `json:"os"`
	Arch         string    `json:"arch"`
	Digest       []byte    `json:"digest"`
	Signature    []byte    `json:"signature"`
	PublisherKey []byte    `json:"publisher_key"` // joined for runner verification
	SizeBytes    int64     `json:"size_bytes"`
	Created      time.Time `json:"created_at"`
}

// AddPublisherKey registers a trusted Ed25519 public key.
func (s *Store) AddPublisherKey(ctx context.Context, name string, pub []byte) (string, error) {
	if len(pub) != ed25519.PublicKeySize {
		return "", fmt.Errorf("store: publisher key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	id := newUUID()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO publisher_keys (id, account_id, name, public_key) VALUES ($1,$2,$3,$4)`,
		id, accountID(ctx), name, pub); err != nil {
		return "", err
	}
	return id, nil
}

// RevokePublisherKey marks a key untrusted. Existing artifact rows keep
// their reference for provenance, but resolve excludes them.
func (s *Store) RevokePublisherKey(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE publisher_keys SET revoked_at = now()
		 WHERE id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, accountID(ctx))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// TrustedKeys lists non-revoked publisher keys (runner trust bootstrap).
func (s *Store) TrustedKeys(ctx context.Context) ([]PublisherKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, public_key, created_at, revoked_at FROM publisher_keys
		 WHERE account_id = $1 AND revoked_at IS NULL ORDER BY created_at`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PublisherKey
	for rows.Next() {
		var k PublisherKey
		if err := rows.Scan(&k.ID, &k.Name, &k.PublicKey, &k.Created, &k.Revoked); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// publisherKeyByName resolves a non-revoked key for upload verification.
func (s *Store) PublisherKeyByName(ctx context.Context, name string) (PublisherKey, error) {
	var k PublisherKey
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, public_key, created_at, revoked_at FROM publisher_keys
		 WHERE account_id = $1 AND name = $2 AND revoked_at IS NULL`,
		accountID(ctx), name).Scan(&k.ID, &k.Name, &k.PublicKey, &k.Created, &k.Revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return PublisherKey{}, ErrNotFound
	}
	return k, err
}

// PutConnectorVersion stores a verified artifact: blob (deduped by
// digest) + version row, one transaction. The API layer verifies the
// signature BEFORE calling this — the store trusts its caller here and
// stays dumb SQL.
func (s *Store) PutConnectorVersion(ctx context.Context, name, version, osName, arch string, digest, signature []byte, publisherKeyID string, data []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO connector_blobs (digest, size_bytes, data) VALUES ($1,$2,$3)
		 ON CONFLICT (digest) DO NOTHING`, digest, len(data), data); err != nil {
		return err
	}
	var connectorID string
	if err := tx.QueryRow(ctx,
		`INSERT INTO connectors (id, account_id, name) VALUES ($1,$2,$3)
		 ON CONFLICT (account_id, name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, newUUID(), accountID(ctx), name).Scan(&connectorID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO connector_versions (connector_id, version, os, arch, digest, signature, publisher_key_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (connector_id, version, os, arch)
		 DO UPDATE SET digest = $5, signature = $6, publisher_key_id = $7, created_at = now(), yanked_at = NULL`,
		connectorID, version, osName, arch, digest, signature, publisherKeyID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ResolveConnector finds an artifact ("" or "latest" = newest publish,
// registry-latest not semver). Yanked versions and revoked keys are
// excluded — fail closed.
func (s *Store) ResolveConnector(ctx context.Context, name, version, osName, arch string) (ConnectorVersion, error) {
	q := `SELECT c.name, v.version, v.os, v.arch, v.digest, v.signature, k.public_key, b.size_bytes, v.created_at
	        FROM connector_versions v
	        JOIN connectors c ON c.id = v.connector_id
	        JOIN publisher_keys k ON k.id = v.publisher_key_id AND k.revoked_at IS NULL
	        JOIN connector_blobs b ON b.digest = v.digest
	       WHERE c.account_id = $1 AND c.name = $2 AND v.os = $3 AND v.arch = $4 AND v.yanked_at IS NULL`
	args := []any{accountID(ctx), name, osName, arch}
	if version != "" && version != "latest" {
		q += ` AND v.version = $5`
		args = append(args, version)
	}
	q += ` ORDER BY v.created_at DESC LIMIT 1`

	var cv ConnectorVersion
	err := s.pool.QueryRow(ctx, q, args...).Scan(
		&cv.Name, &cv.Version, &cv.OS, &cv.Arch, &cv.Digest, &cv.Signature,
		&cv.PublisherKey, &cv.SizeBytes, &cv.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return ConnectorVersion{}, ErrNotFound
	}
	return cv, err
}

// ConnectorBlob fetches artifact bytes by content digest (account-gated
// through connector_versions so blobs are only served to accounts that
// published them).
func (s *Store) ConnectorBlob(ctx context.Context, digest []byte) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx,
		`SELECT b.data FROM connector_blobs b
		 WHERE b.digest = $1 AND EXISTS (
		   SELECT 1 FROM connector_versions v
		   JOIN connectors c ON c.id = v.connector_id
		   WHERE v.digest = b.digest AND c.account_id = $2)`,
		digest, accountID(ctx)).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return data, err
}

// Connectors lists the newest version per connector (dashboard).
func (s *Store) Connectors(ctx context.Context) ([]ConnectorVersion, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT ON (c.name)
		        c.name, v.version, v.os, v.arch, v.digest, v.signature, k.public_key, b.size_bytes, v.created_at
		   FROM connector_versions v
		   JOIN connectors c ON c.id = v.connector_id
		   JOIN publisher_keys k ON k.id = v.publisher_key_id
		   JOIN connector_blobs b ON b.digest = v.digest
		  WHERE c.account_id = $1 AND v.yanked_at IS NULL
		  ORDER BY c.name, v.created_at DESC`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConnectorVersion
	for rows.Next() {
		var cv ConnectorVersion
		if err := rows.Scan(&cv.Name, &cv.Version, &cv.OS, &cv.Arch, &cv.Digest,
			&cv.Signature, &cv.PublisherKey, &cv.SizeBytes, &cv.Created); err != nil {
			return nil, err
		}
		out = append(out, cv)
	}
	return out, rows.Err()
}
