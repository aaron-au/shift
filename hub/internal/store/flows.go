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

// ErrNotPublished means the flow exists but no version has been
// published, so the default (version 0) path has nothing to run.
var ErrNotPublished = errors.New("store: flow has no published version")

// Flow is a deployed flow's public record.
type Flow struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	LatestVersion    int       `json:"latest_version"`
	PublishedVersion int       `json:"published_version"`
	Created          time.Time `json:"created_at"`
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
		newUUID(), accountID(ctx), name).Scan(&flowID, &version)
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

// PublishFlow marks a version as the flow's published version — the one
// version-0 execution and the scheduler run. Publishing an older
// version is a rollback.
func (s *Store) PublishFlow(ctx context.Context, name string, version int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var flowID string
	err = tx.QueryRow(ctx,
		`UPDATE flow_versions v SET status = 'published'
		 FROM flows f
		 WHERE v.flow_id = f.id AND f.account_id = $1 AND f.name = $2 AND v.version = $3
		 RETURNING f.id`,
		accountID(ctx), name, version).Scan(&flowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE flows SET published_version = $2 WHERE id = $1`, flowID, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetFlow returns the flow record and the requested version's document.
// Version 0 resolves to the published version (ErrNotPublished when
// there is none); explicit versions fetch drafts too.
func (s *Store) GetFlow(ctx context.Context, name string, version int) (Flow, json.RawMessage, error) {
	var f Flow
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, latest_version, published_version, created_at FROM flows
		 WHERE account_id = $1 AND name = $2`,
		accountID(ctx), name).Scan(&f.ID, &f.Name, &f.LatestVersion, &f.PublishedVersion, &f.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Flow{}, nil, ErrNotFound
	}
	if err != nil {
		return Flow{}, nil, err
	}
	if version <= 0 {
		if f.PublishedVersion == 0 {
			return Flow{}, nil, ErrNotPublished
		}
		version = f.PublishedVersion
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
		`SELECT id, name, latest_version, published_version, created_at FROM flows
		 WHERE account_id = $1 ORDER BY created_at DESC`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Flow
	for rows.Next() {
		var f Flow
		if err := rows.Scan(&f.ID, &f.Name, &f.LatestVersion, &f.PublishedVersion, &f.Created); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
