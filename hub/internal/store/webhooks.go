package store

import (
	"context"
	"encoding/json"
	"time"
)

// Webhook is a hub-authored webhook configuration (ADR-0016): a named hook
// bound to a deployed flow, optionally protected by a token (stored as a
// hash). Metadata only — no payload.
type Webhook struct {
	Name      string    `json:"name"`
	FlowName  string    `json:"flow_name"`
	TokenHash string    `json:"token_hash,omitempty"`
	Enabled   bool      `json:"enabled"`
	Created   time.Time `json:"created_at"`
	Updated   time.Time `json:"updated_at"`
}

// UpsertWebhook creates or replaces the named webhook for the account. The
// referenced flow must exist.
func (s *Store) UpsertWebhook(ctx context.Context, w Webhook) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO webhooks (id, account_id, name, flow_name, token_hash, enabled)
		 SELECT $1, $2, $3, f.name, NULLIF($5,''), $6
		   FROM flows f WHERE f.account_id = $2 AND f.name = $4
		 ON CONFLICT (account_id, name) DO UPDATE
		   SET flow_name = EXCLUDED.flow_name, token_hash = EXCLUDED.token_hash,
		       enabled = EXCLUDED.enabled, updated_at = now()`,
		newUUID(), accountID(ctx), w.Name, w.FlowName, w.TokenHash, w.Enabled)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound // unknown flow
	}
	return nil
}

// DeleteWebhook removes the named webhook.
func (s *Store) DeleteWebhook(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM webhooks WHERE account_id = $1 AND name = $2`, accountID(ctx), name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Webhooks lists the account's webhooks (admin view; metadata only).
func (s *Store) Webhooks(ctx context.Context) ([]Webhook, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, flow_name, COALESCE(token_hash,''), enabled, created_at, updated_at
		   FROM webhooks WHERE account_id = $1 ORDER BY name`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Webhook
	for rows.Next() {
		var w Webhook
		if err := rows.Scan(&w.Name, &w.FlowName, &w.TokenHash, &w.Enabled, &w.Created, &w.Updated); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// WebhookConfig is what a runner needs to serve a hook: the name, the token
// hash to check, and the published flow document to run.
type WebhookConfig struct {
	Name      string          `json:"name"`
	TokenHash string          `json:"token_hash,omitempty"`
	Document  json.RawMessage `json:"document"`
}

// EnabledWebhookConfigs returns the runnable webhooks for the account:
// enabled hooks whose flow has a published version, with that document.
// Hooks on unpublished flows are skipped (nothing to run yet).
func (s *Store) EnabledWebhookConfigs(ctx context.Context) ([]WebhookConfig, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT w.name, COALESCE(w.token_hash,''), fv.document
		   FROM webhooks w
		   JOIN flows f ON f.id = (SELECT id FROM flows WHERE account_id = w.account_id AND name = w.flow_name)
		   JOIN flow_versions fv ON fv.flow_id = f.id AND fv.version = f.published_version
		  WHERE w.account_id = $1 AND w.enabled AND f.published_version > 0`,
		accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WebhookConfig
	for rows.Next() {
		var c WebhookConfig
		if err := rows.Scan(&c.Name, &c.TokenHash, &c.Document); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
