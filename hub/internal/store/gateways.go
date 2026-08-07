package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrAlreadyAdopted is a second adoption attempt against a gateway that
// already has an owner (ADR-0049 §2). It is refused rather than overwritten:
// silently re-adopting would let anything that can reach the URL with a fresh
// key inherit a live gateway's routes and certificates.
var ErrAlreadyAdopted = errors.New("store: the gateway is already adopted")

// Gateway is a gateway's record.
//
// Fingerprint is the hex SHA-256 of the gateway's long-lived PUBLIC key, which
// makes it safe to return in an API response and to log: it identifies a key,
// it does not authorise use of one.
type Gateway struct {
	ID string `json:"id"`
	// AccountID is carried because the RECONCILE LOOP reads gateways across
	// every tenant — it runs outside any request, so there is no account on its
	// context to scope by. It re-scopes to this value before acting on the
	// gateway; without it the loop would silently only ever serve the default
	// account, which is the shape the scheduler avoids the same way (FireDue).
	AccountID   string `json:"-"`
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
	// InstallToken is deliberately NOT serialised. It is a bootstrap secret,
	// not a fact about the gateway, and an API response is exactly where a
	// secret leaks from — it is handed to the operator once, by the create
	// endpoint, and never served again.
	InstallToken string     `json:"-"`
	TokenExpires *time.Time `json:"token_expires_at,omitempty"`
	AdoptedAt    *time.Time `json:"adopted_at,omitempty"`

	CertSerial   string     `json:"cert_serial,omitempty"`
	CertNotAfter *time.Time `json:"cert_not_after,omitempty"`

	// ConfigVersion is what the hub intends this gateway to run;
	// PushedVersion is what it last acknowledged. They differ exactly while a
	// push is outstanding or failing, which is the drift an administrator
	// needs to see — from the hub, because that is where the administrator is.
	// TrustedProxies are the CIDRs whose X-Forwarded-* headers this gateway
	// believes. Per gateway because it describes where the box SITS.
	TrustedProxies []string `json:"trusted_proxies,omitempty"`

	ConfigVersion int64      `json:"config_version"`
	PushedVersion int64      `json:"pushed_version"`
	LastPushAt    *time.Time `json:"last_push_at,omitempty"`
	LastPushError string     `json:"last_push_error,omitempty"`

	Created time.Time `json:"created_at"`
}

const gatewayCols = `id, account_id, name, url, fingerprint, install_token, token_expires_at, adopted_at,
	cert_serial, cert_not_after, trusted_proxies,
	config_version, pushed_version, last_push_at, last_push_error, created_at`

func scanGateway(row pgx.Row) (Gateway, error) {
	var g Gateway
	var serial *string
	var pushErr *string
	err := row.Scan(&g.ID, &g.AccountID, &g.Name, &g.URL, &g.Fingerprint, &g.InstallToken, &g.TokenExpires,
		&g.AdoptedAt, &serial,
		&g.CertNotAfter, &g.TrustedProxies,
		&g.ConfigVersion, &g.PushedVersion, &g.LastPushAt, &pushErr, &g.Created)
	if err != nil {
		return Gateway{}, err
	}
	if serial != nil {
		g.CertSerial = *serial
	}
	if pushErr != nil {
		g.LastPushError = *pushErr
	}
	return g, nil
}

// CreateGateway records an intent to adopt: a name, the URL the hub will dial,
// and a freshly minted install token (ADR-0049 §1a).
//
// The fingerprint is EMPTY and stays empty until the hub learns it on the first
// dial. That is the whole point of the token: the hub cannot pin a key that
// does not exist yet, so it pairs on a secret it minted and pins what it finds.
//
// The token is returned once, in the created record. The operator pastes it
// into a deployment, and the hub keeps its copy only until adoption burns it.
func (s *Store) CreateGateway(ctx context.Context, name, url string, expires time.Time) (Gateway, error) {
	token, _ := newSecret("sgt_")
	row := s.pool.QueryRow(ctx,
		`INSERT INTO gateways (id, account_id, name, url, fingerprint, install_token, token_expires_at)
		 VALUES ($1,$2,$3,$4,'',$5,$6)
		 RETURNING `+gatewayCols,
		newUUID(), accountID(ctx), name, url, token, expires)
	return scanGateway(row)
}

// LearnGatewayFingerprint records the key the hub observed while pairing, and
// burns the install token in the same statement.
//
// The UPDATE requires the gateway to still be unadopted with the token unspent,
// so two concurrent pairings cannot both win and a token cannot be replayed
// after the first success. Checking in Go and writing after would leave that
// window open.
func (s *Store) LearnGatewayFingerprint(ctx context.Context, id, fingerprint string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateways SET fingerprint = $3, install_token = '', token_expires_at = NULL
		  WHERE account_id = $1 AND id = $2 AND adopted_at IS NULL AND install_token <> ''`,
		accountID(ctx), id, fingerprint)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.GetGateway(ctx, id); err != nil {
			return err
		}
		return ErrAlreadyAdopted
	}
	return nil
}

// ListGateways returns the account's gateways, newest first.
func (s *Store) ListGateways(ctx context.Context) ([]Gateway, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+gatewayCols+` FROM gateways WHERE account_id = $1 ORDER BY created_at DESC`,
		accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Gateway{}
	for rows.Next() {
		g, err := scanGateway(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GetGateway reads one gateway by id.
func (s *Store) GetGateway(ctx context.Context, id string) (Gateway, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+gatewayCols+` FROM gateways WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id)
	g, err := scanGateway(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Gateway{}, ErrNotFound
	}
	return g, err
}

// MarkGatewayAdopted records a completed adoption and the identity issued
// during it.
//
// The UPDATE requires adopted_at IS NULL, so two concurrent adoptions of the
// same record cannot both succeed — the second sees no row and gets
// ErrAlreadyAdopted. Checking in Go first and writing second would leave the
// window open.
func (s *Store) MarkGatewayAdopted(ctx context.Context, id, serial string, notAfter time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateways
		    SET adopted_at = now(), cert_serial = $3, cert_not_after = $4, cert_issued_at = now()
		  WHERE account_id = $1 AND id = $2 AND adopted_at IS NULL`,
		accountID(ctx), id, serial, notAfter)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Either the record is gone or it already has an owner. Distinguish,
		// because the two need different operator actions: delete-and-recreate
		// versus rotate-adoption.
		if _, err := s.GetGateway(ctx, id); err != nil {
			return err
		}
		return ErrAlreadyAdopted
	}
	return nil
}

// RecordGatewayCertificate updates the identity after a renewal.
func (s *Store) RecordGatewayCertificate(ctx context.Context, id, serial string, notAfter time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateways SET cert_serial = $3, cert_not_after = $4, cert_issued_at = now()
		  WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, serial, notAfter)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RotateGatewayAdoption points an existing record at a NEW key and returns it
// to the unadopted state, preserving its routes and certificates (ADR-0049 §4).
//
// This is the recovery path for a gateway redeployed without its state
// directory. It is deliberately an explicit, audited administrator action
// rather than something the hub infers: a gateway whose identity vanished must
// not be silently re-trusted, because "presents a fresh key at the right URL"
// is exactly what an impostor also does.
func (s *Store) RotateGatewayAdoption(ctx context.Context, id string, expires time.Time) (string, error) {
	token, _ := newSecret("sgt_")
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateways
		    SET fingerprint = '', install_token = $3, token_expires_at = $4, adopted_at = NULL,
		        cert_serial = NULL, cert_not_after = NULL, cert_issued_at = NULL,
		        pushed_version = 0, last_push_at = NULL, last_push_error = NULL
		  WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, token, expires)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return token, nil
}

// DeleteGateway removes a gateway record.
//
// Deletion is revocation, as it is for runners (ADR-0044 §6): the hub stops
// dialling and stops issuing, and the identity it last held expires on its own
// short TTL. There is no revocation list to publish or fail to publish.
func (s *Store) DeleteGateway(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM gateways WHERE account_id = $1 AND id = $2`, accountID(ctx), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// BumpGatewayConfig raises the intended configuration generation for every
// gateway in the account, which is what marks a push as due.
//
// Account-wide rather than per-gateway because configuration is derived from
// account-wide facts — the flow set, the runner roster, the routes. A change to
// any of them is a change every gateway needs.
//
// NOTHING IN PRODUCTION CALLS THIS YET, and that is deliberate rather than an
// oversight: the config builder is not built (gwsync.Options.ConfigFor is nil),
// so raising a generation now would mark every gateway permanently drifted
// against a push that can never be satisfied. Its caller lands with the
// builder. Until then it is exercised by the tests that prove the drift
// detection and push-accounting paths in gwsync actually work — those ARE
// production code, and this is how they are put into the state they handle.
func (s *Store) BumpGatewayConfig(ctx context.Context) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE gateways SET config_version = config_version + 1 WHERE account_id = $1`,
		accountID(ctx))
	return err
}

// RecordGatewayPush stores the outcome of a push attempt.
//
// A failure records the error and leaves pushed_version alone, so the drift
// stays visible and the reconcile loop keeps retrying. A success clears the
// error — a stale message beside a healthy gateway is worse than none.
func (s *Store) RecordGatewayPush(ctx context.Context, id string, version int64, pushErr error) error {
	if pushErr != nil {
		_, err := s.pool.Exec(ctx,
			`UPDATE gateways SET last_push_at = now(), last_push_error = $3
			  WHERE account_id = $1 AND id = $2`,
			accountID(ctx), id, pushErr.Error())
		return err
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE gateways SET pushed_version = $3, last_push_at = now(), last_push_error = NULL
		  WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, version)
	return err
}

// GatewaysPending lists gateways never yet adopted that still hold a live
// install token — the ones the reconcile loop should try to pair with.
//
// An expired token is left alone rather than retried forever. Re-issuing one is
// rotate, which is a deliberate act with an audit record behind it.
func (s *Store) GatewaysPending(ctx context.Context) ([]Gateway, error) {
	// NO account filter: this is read by the reconcile loop, which has no
	// account on its context and must see every tenant's gateways.
	return s.gatewayQuery(ctx,
		`SELECT `+gatewayCols+` FROM gateways
		  WHERE adopted_at IS NULL AND install_token <> ''
		    AND (token_expires_at IS NULL OR token_expires_at > now())
		  ORDER BY created_at`)
}

// GatewaysDue lists adopted gateways whose acknowledged generation is behind
// the intended one, or whose identity is approaching expiry.
//
// The second half is not an optimisation. A gateway cannot ask for a renewal —
// it never dials inward — so an identity that lapses before the hub pushes a
// new one would strand it. Renewal is therefore something the hub must notice,
// which means it belongs in the same query that notices config drift.
func (s *Store) GatewaysDue(ctx context.Context, renewWithin time.Duration) ([]Gateway, error) {
	// NO account filter, for the same reason as GatewaysPending.
	return s.gatewayQuery(ctx,
		`SELECT `+gatewayCols+` FROM gateways
		  WHERE adopted_at IS NOT NULL
		    AND (pushed_version < config_version
		         OR cert_not_after IS NULL
		         OR cert_not_after < now() + $1::interval)
		  ORDER BY created_at`,
		renewWithin.String())
}

func (s *Store) gatewayQuery(ctx context.Context, sql string, args ...any) ([]Gateway, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Gateway{}
	for rows.Next() {
		g, err := scanGateway(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}
