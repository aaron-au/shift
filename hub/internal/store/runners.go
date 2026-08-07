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

	// Labels are what the hub ASSERTS this runner is (ADR-0041 §3) — the facts
	// route selectors match against. Served here because an administrator
	// editing them has to be able to see them; a manager that writes a value
	// it cannot read back is a guess with a button on it.
	Labels map[string]string `json:"labels,omitempty"`

	// Tier is what this runner is FOR (ADR-0048 §1): "production" or "test".
	// Asserted by the hub, never by the runner — one that could name its own
	// tier could escape metering, or receive work it should not see.
	Tier string `json:"tier"`

	// CertNotAfter is when this runner's client certificate stops being
	// honoured (ADR-0044). Surfaced because expiry is silent otherwise: the
	// runner simply stops leasing, and nothing says why.
	CertNotAfter *time.Time `json:"cert_not_after,omitempty"`
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

// RegisterRunnerCert consumes a registration token and creates a runner with
// NO bearer secret (ADR-0044 §1). Its credential is the certificate the caller
// signs next; until that is recorded the row cannot authenticate anything,
// which is the correct state for a registration that failed halfway.
//
// The token is spent either way. It is single-use by design, and a token that
// survived a failed registration would be a token that could be replayed.
func (s *Store) RegisterRunnerCert(ctx context.Context, token, name string) (id, account string, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	account, err = consumeRegistrationToken(ctx, tx, token)
	if err != nil {
		return "", "", err
	}
	id = newUUID()
	if _, err := tx.Exec(ctx,
		`INSERT INTO runners (id, account_id, name) VALUES ($1,$2,$3)`,
		id, account, name); err != nil {
		return "", "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return id, account, nil
}

// RecordRunnerCertificate stores the issued certificate's identity for
// operations — which certificate a runner is using, and when it expires.
// Nothing here is consulted to AUTHENTICATE: the subject is the runner id, and
// the chain is verified by TLS.
func (s *Store) RecordRunnerCertificate(ctx context.Context, id, serial string, notAfter time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE runners SET cert_serial = $2, cert_not_after = $3, cert_issued_at = now()
		 WHERE id = $1`, id, serial, notAfter)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AuthRunnerCert resolves a runner id proven by a client certificate to its
// account, updating last_seen_at.
//
// The account is a fact the hub HOLDS, not one the runner states — the same
// principle as the bearer path, and the reason a certificate carries only an
// id (ADR-0044 §5).
func (s *Store) AuthRunnerCert(ctx context.Context, id string) (accountID string, err error) {
	err = s.pool.QueryRow(ctx,
		`UPDATE runners SET last_seen_at = now() WHERE id = $1 RETURNING account_id`,
		id).Scan(&accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		// A certificate for a runner that has been deleted is a valid
		// signature over a name that means nothing any more. Refusing here is
		// what makes deleting a runner an effective revocation.
		return "", ErrUnauthorized
	}
	if err != nil {
		return "", err
	}
	return accountID, nil
}

// DeleteRunner removes a runner.
//
// Under mTLS this IS revocation (ADR-0044 §2): the certificate stays
// cryptographically valid until it expires, and the name it carries stops
// resolving, so the next request it makes is refused. With certificate
// lifetimes measured in a day, that is the whole revocation story — no CRL to
// publish, distribute, or fail to fetch.
func (s *Store) DeleteRunner(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM runners WHERE id = $1 AND account_id = $2`, id, accountID(ctx))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// consumeRegistrationToken spends a single-use token and returns its account.
func consumeRegistrationToken(ctx context.Context, tx pgx.Tx, token string) (string, error) {
	var accountID string
	err := tx.QueryRow(ctx,
		`UPDATE runner_registration_tokens SET used_at = now()
		 WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now()
		 RETURNING account_id`,
		hashSecret(token)).Scan(&accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrTokenInvalid
	}
	return accountID, err
}

// RegisterRunner consumes a registration token and issues the runner's
// identity: its id and bearer secret (returned once, stored hashed).
//
// Retained for deployments that terminate TLS before the hub, where a client
// certificate cannot survive the hop (ADR-0044 §4).
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
		`SELECT id, name, registered_at, last_seen_at, labels, cert_not_after, tier FROM runners
		 WHERE account_id = $1 ORDER BY registered_at DESC`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Runner
	for rows.Next() {
		var r Runner
		var labels []byte
		if err := rows.Scan(&r.ID, &r.Name, &r.Registered, &r.LastSeen, &labels, &r.CertNotAfter, &r.Tier); err != nil {
			return nil, err
		}
		if err := unmarshalMap(labels, &r.Labels); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Runner tiers (ADR-0048 §1).
const (
	TierProduction = "production"
	TierTest       = "test"
)

// ValidTier reports whether t is a runner tier.
func ValidTier(t string) bool { return t == TierProduction || t == TierTest }

// SetRunnerTier records what a runner is FOR (ADR-0048 §1).
//
// Hub-asserted, like labels and for the same reason: a runner that could name
// its own tier could claim production capacity to escape metering, or claim
// test capacity to be handed work it should not see. Nothing the runner sends
// is consulted.
func (s *Store) SetRunnerTier(ctx context.Context, id, tier string) error {
	if !ValidTier(tier) {
		return fmt.Errorf("store: unknown runner tier %q", tier)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE runners SET tier = $3 WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, tier)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RunnerTier reads one runner's tier. Used on the claim path, which must not
// trust anything the runner says about itself.
func (s *Store) RunnerTier(ctx context.Context, id string) (string, error) {
	var tier string
	err := s.pool.QueryRow(ctx,
		`SELECT tier FROM runners WHERE account_id = $1 AND id = $2`, accountID(ctx), id).Scan(&tier)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return tier, err
}
