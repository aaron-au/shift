package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Route is one public path a gateway serves (ADR-0038 §5).
//
// The JSON tags mirror gateway/internal/config.Route exactly, because this is
// what the built configuration is marshalled from. The two types are separate
// on purpose — the gateway is its own module with no dependencies, an auditable
// property of the one component that may sit in a DMZ — and a golden fixture
// keeps them honest (see TestTheBuiltConfigMatchesTheGatewaysModel).
type Route struct {
	ID        string `json:"-"`
	GatewayID string `json:"-"` // empty = every gateway in the account

	Path   string `json:"path"`
	Method string `json:"method,omitempty"`
	Flow   string `json:"flow"`

	Selector map[string]string `json:"selector,omitempty"`

	// AuthTokenSHA256 is the hex SHA-256 of the caller's bearer token. The
	// plaintext is returned once, at creation, and never stored.
	AuthTokenSHA256 string `json:"auth_token_sha256,omitempty"`
	AuthPrincipal   string `json:"auth_principal,omitempty"`

	AllowCIDRs     []string          `json:"allow_cidrs,omitempty"`
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
	MaxBodyBytes   int64             `json:"max_body_bytes,omitempty"`

	Created time.Time `json:"-"`
	Updated time.Time `json:"-"`
}

// GatewayConfig is the document the hub pushes to one gateway.
//
// Mirrors gateway/internal/config.Config. Deliberately a whole document with no
// partial form: the gateway swaps it atomically, and a half-applied policy is
// worse than a stale one.
type GatewayConfig struct {
	Version        int64          `json:"version"`
	Routes         []Route        `json:"routes"`
	Runners        []RosterRunner `json:"runners,omitempty"`
	TrustedProxies []string       `json:"trusted_proxies,omitempty"`
}

// RosterRunner is one runner's hub-asserted identity and labels.
type RosterRunner struct {
	ID     string            `json:"id"`
	Labels map[string]string `json:"labels,omitempty"`
}

const routeCols = `id, COALESCE(gateway_id::text, ''), path, method, flow, selector,
	auth_token_sha256, auth_principal, allow_cidrs, require_headers, max_body_bytes,
	created_at, updated_at`

func scanRoute(row pgx.Row) (Route, error) {
	var r Route
	var selector, headers []byte
	err := row.Scan(&r.ID, &r.GatewayID, &r.Path, &r.Method, &r.Flow, &selector,
		&r.AuthTokenSHA256, &r.AuthPrincipal, &r.AllowCIDRs, &headers, &r.MaxBodyBytes,
		&r.Created, &r.Updated)
	if err != nil {
		return Route{}, err
	}
	if err := unmarshalMap(selector, &r.Selector); err != nil {
		return Route{}, err
	}
	if err := unmarshalMap(headers, &r.RequireHeaders); err != nil {
		return Route{}, err
	}
	return r, nil
}

// unmarshalMap decodes a JSONB column, leaving an empty map nil.
//
// Nil rather than empty matters: the JSON tags are omitempty, and an empty
// object serialised into the pushed configuration would differ byte-for-byte
// from one that never had the field — which is exactly the kind of difference
// that makes a config version churn for no reason.
func unmarshalMap(raw []byte, into *map[string]string) error {
	if len(raw) == 0 {
		return nil
	}
	m := map[string]string{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	if len(m) > 0 {
		*into = m
	}
	return nil
}

// CreateRoute records a route and mints the caller's bearer token.
//
// The token is minted here rather than supplied, for the same reason runner
// registration tokens are: an operator inventing their own credential invents a
// weak one, and one that has probably been in a shell history. It is returned
// once and stored only as a hash.
//
// An empty principal is allowed — a route may be deliberately anonymous — but
// it means the runner sees no caller identity, so the API layer decides whether
// that is acceptable rather than this one.
func (s *Store) CreateRoute(ctx context.Context, r Route, withToken bool) (Route, string, error) {
	var plaintext, hash string
	if withToken {
		tok, sum := newSecret("sgr_")
		plaintext, hash = tok, hexString(sum)
	}
	var gatewayID any
	if r.GatewayID != "" {
		gatewayID = r.GatewayID
	}
	row := s.pool.QueryRow(ctx,
		`INSERT INTO gateway_routes
		   (id, account_id, gateway_id, path, method, flow, selector,
		    auth_token_sha256, auth_principal, allow_cidrs, require_headers, max_body_bytes)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 RETURNING `+routeCols,
		newUUID(), accountID(ctx), gatewayID, r.Path, r.Method, r.Flow, mapOrEmpty(r.Selector),
		hash, r.AuthPrincipal, orEmptySlice(r.AllowCIDRs), mapOrEmpty(r.RequireHeaders), r.MaxBodyBytes)
	out, err := scanRoute(row)
	if err != nil {
		return Route{}, "", err
	}
	return out, plaintext, nil
}

// ListRoutes returns the account's routes, ordered so a built configuration is
// byte-stable across passes — an unstable order would churn the config version
// and re-push identical policy forever.
func (s *Store) ListRoutes(ctx context.Context) ([]Route, error) {
	return s.routeQuery(ctx,
		`SELECT `+routeCols+` FROM gateway_routes WHERE account_id = $1
		  ORDER BY path, method, id`, accountID(ctx))
}

// GetRoute reads one route.
func (s *Store) GetRoute(ctx context.Context, id string) (Route, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+routeCols+` FROM gateway_routes WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id)
	r, err := scanRoute(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Route{}, ErrNotFound
	}
	return r, err
}

// DeleteRoute removes a route. The next reconcile pass withdraws it from every
// gateway, which is what makes "take this endpoint down" one action.
func (s *Store) DeleteRoute(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM gateway_routes WHERE account_id = $1 AND id = $2`, accountID(ctx), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RotateRouteToken mints a new caller token for a route, returning it once.
func (s *Store) RotateRouteToken(ctx context.Context, id string) (string, error) {
	plaintext, sum := newSecret("sgr_")
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateway_routes SET auth_token_sha256 = $3, updated_at = now()
		  WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, hexString(sum))
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return plaintext, nil
}

func (s *Store) routeQuery(ctx context.Context, sql string, args ...any) ([]Route, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Route{}
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetRunnerLabels records what a runner IS (ADR-0041 §3).
//
// Asserted by the hub and never by the runner: a runner that could state its own
// labels could promote itself into a trust tier by claiming one, and placement
// would become a suggestion rather than a decision.
func (s *Store) SetRunnerLabels(ctx context.Context, id string, labels map[string]string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE runners SET labels = $3 WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, mapOrEmpty(labels))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetGatewayTrustedProxies records whose forwarded headers a gateway believes.
func (s *Store) SetGatewayTrustedProxies(ctx context.Context, id string, cidrs []string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateways SET trusted_proxies = $3 WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id, orEmptySlice(cidrs))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// BuildGatewayConfig assembles the document for one gateway.
//
// Everything is read in one place because the document is pushed whole: routes
// scoped to this gateway or to all of them, the runner roster the selectors are
// resolved against, and the proxy trust for where this box sits.
//
// The ROSTER is the half that is easy to underestimate. A gateway matches a
// route's selector against labels from here, never from a runner's poll — so a
// roster that omitted a runner would silently stop routing to it, and a roster
// that included one the hub no longer trusts would keep routing to it. It is
// therefore built from the live runner table on every pass rather than cached.
func (s *Store) BuildGatewayConfig(ctx context.Context, gatewayID string) (GatewayConfig, error) {
	gw, err := s.GetGateway(ctx, gatewayID)
	if err != nil {
		return GatewayConfig{}, err
	}
	routes, err := s.routeQuery(ctx,
		`SELECT `+routeCols+` FROM gateway_routes
		  WHERE account_id = $1 AND (gateway_id IS NULL OR gateway_id = $2)
		  ORDER BY path, method, id`,
		accountID(ctx), gatewayID)
	if err != nil {
		return GatewayConfig{}, err
	}
	roster, err := s.runnerRoster(ctx)
	if err != nil {
		return GatewayConfig{}, err
	}
	cfg := GatewayConfig{
		Version:        gw.ConfigVersion,
		Routes:         routes,
		Runners:        roster,
		TrustedProxies: gw.TrustedProxies,
	}
	if len(cfg.TrustedProxies) == 0 {
		cfg.TrustedProxies = nil
	}
	return cfg, nil
}

func (s *Store) runnerRoster(ctx context.Context) ([]RosterRunner, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, labels FROM runners WHERE account_id = $1 ORDER BY id`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RosterRunner{}
	for rows.Next() {
		var r RosterRunner
		var labels []byte
		if err := rows.Scan(&r.ID, &labels); err != nil {
			return nil, err
		}
		if err := unmarshalMap(labels, &r.Labels); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// hexString renders a hash the way the gateway compares it: lowercase hex.
func hexString(sum []byte) string { return hex.EncodeToString(sum) }

func mapOrEmpty(m map[string]string) []byte {
	if len(m) == 0 {
		return []byte("{}")
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return []byte("{}")
	}
	return raw
}

func orEmptySlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
