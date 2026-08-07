package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aaron-au/shift/pkg/flowdoc"
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
//
// It also PINS the version's connector steps (ADR-0047 §1): every unpinned
// registry connector resolves to a concrete version, and the rewritten
// document is stored. A published flow runs the builds it was published
// against for as long as it exists, so publishing a connector can no longer
// change what a flow does on its next task.
//
// The rewrite happens in the SAME transaction as the status flip. Pinning and
// publishing separately would leave a window where a flow is published and
// unpinned — which is precisely the state this exists to make unreachable —
// and a crash in that window would leave it there permanently.
func (s *Store) PublishFlow(ctx context.Context, name string, version int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var flowID string
	var raw []byte
	err = tx.QueryRow(ctx,
		`UPDATE flow_versions v SET status = 'published'
		 FROM flows f
		 WHERE v.flow_id = f.id AND f.account_id = $1 AND f.name = $2 AND v.version = $3
		 RETURNING f.id, v.document`,
		accountID(ctx), name, version).Scan(&flowID, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	pinned, err := s.pinDocument(ctx, raw)
	if err != nil {
		return err
	}
	if pinned != nil {
		if _, err := tx.Exec(ctx,
			`UPDATE flow_versions SET document = $3 WHERE flow_id = $1 AND version = $2`,
			flowID, version, pinned); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE flows SET published_version = $2 WHERE id = $1`, flowID, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// pinDocument resolves every unpinned connector step to a concrete version,
// returning the rewritten document (nil when nothing needed pinning).
//
// A connector the REGISTRY does not know is left unpinned rather than refused.
// The registry is optional — a self-hosted deployment may provision connector
// binaries into the runner's directory and never publish an artifact — and a
// hub that refused to publish those flows would make the registry mandatory by
// accident. What it must not be is silent: `connector-pin` (ADR-0042 §7)
// raises a notice naming every step still resolving to "newest", and that is
// where an author sees a typo or a connector they forgot to publish.
func (s *Store) pinDocument(ctx context.Context, raw []byte) ([]byte, error) {
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		// A stored document that no longer parses — an older schema, a
		// hand-edited row — is left exactly as it is. Publishing was possible
		// before pinning existed and must stay possible, or adding this would
		// have quietly made a class of existing rows unpublishable. The
		// document is still readable for repair, and /graph already answers
		// 422 for it.
		//nolint:nilerr // deliberate: an unparseable legacy row publishes unpinned rather than becoming unpublishable
		return nil, nil
	}
	before := doc.ConnectorPins()
	if err := doc.PinConnectors(func(connector string) (string, error) {
		v, err := s.LatestConnectorVersion(ctx, connector)
		if errors.Is(err, ErrNotFound) {
			return "", nil // nothing published to pin; stays unpinned, reported by review
		}
		return v, err
	}); err != nil {
		return nil, err
	}
	after := doc.ConnectorPins()
	changed := false
	for i := range after {
		if before[i].Version != after[i].Version {
			changed = true
			break
		}
	}
	if !changed {
		return nil, nil
	}
	return json.Marshal(doc)
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

// FlowByName returns the flow record alone — its version numbers, without
// fetching a document. Design-time callers need it because their default
// version is the LATEST one (you review a draft in order to decide whether to
// publish it), which is a different question from GetFlow's default.
func (s *Store) FlowByName(ctx context.Context, name string) (Flow, error) {
	var f Flow
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, latest_version, published_version, created_at FROM flows
		 WHERE account_id = $1 AND name = $2`,
		accountID(ctx), name).Scan(&f.ID, &f.Name, &f.LatestVersion, &f.PublishedVersion, &f.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Flow{}, ErrNotFound
	}
	return f, err
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
