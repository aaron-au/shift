package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is a missing flow or task.
var ErrNotFound = errors.New("store: not found")

// Flow is a deployed flow's public record.
type Flow struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	LatestVersion int       `json:"latest_version"`
	Created       time.Time `json:"created_at"`
}

// DeployFlow stores a new version of the named flow (creating the flow on
// first deploy) and returns the version number. The document must already
// be validated (flowdoc.Parse) by the caller.
func (s *Store) DeployFlow(ctx context.Context, name string, document json.RawMessage) (version int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var flowID string
	err = tx.QueryRow(ctx,
		`INSERT INTO flows (id, account_id, name, latest_version)
		 VALUES ($1,$2,$3,1)
		 ON CONFLICT (account_id, name)
		 DO UPDATE SET latest_version = flows.latest_version + 1
		 RETURNING id, latest_version`,
		newUUID(), DefaultAccountID, name).Scan(&flowID, &version)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO flow_versions (flow_id, version, document) VALUES ($1,$2,$3)`,
		flowID, version, document); err != nil {
		return 0, err
	}
	return version, tx.Commit(ctx)
}

// GetFlow returns the flow record and the requested version's document
// (version 0 = latest).
func (s *Store) GetFlow(ctx context.Context, name string, version int) (Flow, json.RawMessage, error) {
	var f Flow
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, latest_version, created_at FROM flows
		 WHERE account_id = $1 AND name = $2`,
		DefaultAccountID, name).Scan(&f.ID, &f.Name, &f.LatestVersion, &f.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Flow{}, nil, ErrNotFound
	}
	if err != nil {
		return Flow{}, nil, err
	}
	if version <= 0 {
		version = f.LatestVersion
	}
	var doc json.RawMessage
	err = s.pool.QueryRow(ctx,
		`SELECT document FROM flow_versions WHERE flow_id = $1 AND version = $2`,
		f.ID, version).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return Flow{}, nil, ErrNotFound
	}
	if err != nil {
		return Flow{}, nil, err
	}
	return f, doc, nil
}

// Flows lists deployed flows, newest first.
func (s *Store) Flows(ctx context.Context) ([]Flow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, latest_version, created_at FROM flows
		 WHERE account_id = $1 ORDER BY created_at DESC`, DefaultAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Flow
	for rows.Next() {
		var f Flow
		if err := rows.Scan(&f.ID, &f.Name, &f.LatestVersion, &f.Created); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
