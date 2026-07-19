package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrTokenInvalid covers unknown, expired, and already-used registration
// tokens — callers must not distinguish (no oracle).
var ErrTokenInvalid = errors.New("store: registration token invalid")

// ErrUnauthorized is an unknown runner secret.
var ErrUnauthorized = errors.New("store: unauthorized")

// Runner is a registered runner's public record.
type Runner struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Registered time.Time  `json:"registered_at"`
	LastSeen   *time.Time `json:"last_seen_at,omitempty"`
}

// CreateRegistrationToken mints a single-use runner registration token.
// The plaintext is returned once; only its hash is stored.
func (s *Store) CreateRegistrationToken(ctx context.Context, ttl time.Duration) (token string, expires time.Time, err error) {
	if ttl <= 0 {
		ttl = time.Hour
	}
	plaintext, hash := newSecret("srt_")
	expires = time.Now().Add(ttl)
	_, err = s.pool.Exec(ctx,
		`INSERT INTO runner_registration_tokens (id, account_id, token_hash, expires_at)
		 VALUES ($1,$2,$3,$4)`,
		newUUID(), accountID(ctx), hash, expires)
	if err != nil {
		return "", time.Time{}, err
	}
	return plaintext, expires, nil
}

// RegisterRunner consumes a registration token and issues the runner's
// identity: its id and bearer secret (returned once, stored hashed).
func (s *Store) RegisterRunner(ctx context.Context, token, name string) (id, secret string, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var accountID string
	err = tx.QueryRow(ctx,
		`UPDATE runner_registration_tokens SET used_at = now()
		 WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now()
		 RETURNING account_id`,
		hashSecret(token)).Scan(&accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrTokenInvalid
	}
	if err != nil {
		return "", "", err
	}

	id = newUUID()
	plaintext, hash := newSecret("rs_")
	if _, err := tx.Exec(ctx,
		`INSERT INTO runners (id, account_id, name, secret_hash) VALUES ($1,$2,$3,$4)`,
		id, accountID, name, hash); err != nil {
		return "", "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return id, plaintext, nil
}

// AuthRunner resolves a bearer secret to a runner id and its account,
// updating last_seen_at. Lookup is by SHA-256 of the presented secret.
func (s *Store) AuthRunner(ctx context.Context, secret string) (id, accountID string, err error) {
	err = s.pool.QueryRow(ctx,
		`UPDATE runners SET last_seen_at = now() WHERE secret_hash = $1 RETURNING id, account_id`,
		hashSecret(secret)).Scan(&id, &accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrUnauthorized
	}
	if err != nil {
		return "", "", fmt.Errorf("store: auth runner: %w", err)
	}
	return id, accountID, nil
}

// Runners lists the account's registered runners, newest first.
func (s *Store) Runners(ctx context.Context) ([]Runner, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, registered_at, last_seen_at FROM runners
		 WHERE account_id = $1 ORDER BY registered_at DESC`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Runner
	for rows.Next() {
		var r Runner
		if err := rows.Scan(&r.ID, &r.Name, &r.Registered, &r.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
