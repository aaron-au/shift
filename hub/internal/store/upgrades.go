package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bulk connector upgrade (ADR-0047 §9).
//
// Republishing flows one at a time after every connector release is the
// friction that makes people stop upgrading — and a platform whose users have
// stopped upgrading is one where every security fix ships to nobody. So there
// is a bulk path. It is staged rather than automatic, because a mass republish
// is a mass change against live data, and the two gates (a report you read, a
// test run that passed) are what separate it from a button that rewrites
// production.
//
// The three steps share one durable batch. That is the whole design: locate
// reports, stage records, publish verifies against what was recorded. Without
// it, "tested" would be a claim about a set of flows nobody wrote down.

// ErrUntested is publish-all refusing a batch whose flows have not all passed
// their test run. It names them, because "some flow failed" is not actionable.
var ErrUntested = errors.New("store: batch has flows that have not passed a test run")

// ErrAlreadyPublished is a second publish-all against the same batch.
var ErrAlreadyPublished = errors.New("store: batch is already published")

// PinnedFlow is one published flow version pinning a connector, with the build
// it pins. It is FlowRef plus the version, because bulk locate asks the
// question across every build at once rather than one at a time.
type PinnedFlow struct {
	Flow    string   `json:"flow"`
	Version int      `json:"version"`
	Steps   []string `json:"steps"`
	Current bool     `json:"current"`
	Pinned  string   `json:"pinned"`
}

// FlowsPinningConnector reports every published flow version that pins the
// named connector, at whatever build it pins.
//
// It deliberately does NOT filter by "older than the target". Which versions
// are older is a registry-ordering question, and ordering lives in one place
// (the API layer's orderedVersions) so that currency notices, the support
// window and bulk locate cannot disagree about what "behind" means. A second
// ordering implementation in SQL — string comparison on a version column —
// would be wrong the first time somebody published v0.10.0.
func (s *Store) FlowsPinningConnector(ctx context.Context, connector string) ([]PinnedFlow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT f.name, p.flow_version, array_agg(p.step_id ORDER BY p.step_id),
		        bool_or(f.published_version = p.flow_version), p.version
		   FROM flow_connector_pins p
		   JOIN flows f ON f.id = p.flow_id
		  WHERE p.account_id = $1 AND p.connector = $2
		  GROUP BY f.name, p.flow_version, p.version
		  ORDER BY f.name, p.flow_version`,
		accountID(ctx), connector)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PinnedFlow{}
	for rows.Next() {
		var p PinnedFlow
		if err := rows.Scan(&p.Flow, &p.Version, &p.Steps, &p.Current, &p.Pinned); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UpgradeBatch is a staged bulk upgrade.
type UpgradeBatch struct {
	ID        string        `json:"id"`
	Connector string        `json:"connector"`
	Target    string        `json:"target"`
	Created   time.Time     `json:"created_at"`
	CreatedBy string        `json:"created_by"`
	Published *time.Time    `json:"published_at,omitempty"`
	Flows     []UpgradeFlow `json:"flows"`
}

// UpgradeFlow is one flow's place in a batch.
type UpgradeFlow struct {
	Flow      string          `json:"flow"`
	From      string          `json:"from"`
	Draft     int             `json:"draft_version"`
	TaskID    string          `json:"task_id,omitempty"`
	TaskState string          `json:"task_state,omitempty"`
	Notices   json.RawMessage `json:"notices,omitempty"`
	Published *time.Time      `json:"published_at,omitempty"`
}

// StagedFlow is one flow the caller wants in a batch: the draft that was
// created for it and the task queued to test that draft.
type StagedFlow struct {
	Flow   string
	From   string
	Draft  int
	TaskID string
}

// CreateUpgradeBatch records a staged batch and its flows in one transaction.
//
// One transaction because a batch that lists half its flows is worse than no
// batch: publish-all would check the gate against a short list and report
// success having moved only some of them.
func (s *Store) CreateUpgradeBatch(ctx context.Context, connector, target, by string, flows []StagedFlow) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	id := newUUID()
	if _, err := tx.Exec(ctx,
		`INSERT INTO connector_upgrade_batches (id, account_id, connector, target, created_by)
		 VALUES ($1,$2,$3,$4,$5)`,
		id, accountID(ctx), connector, target, by); err != nil {
		return "", err
	}
	for _, f := range flows {
		var task *string
		if f.TaskID != "" {
			t := f.TaskID
			task = &t
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO connector_upgrade_flows
			   (batch_id, flow_id, flow_name, from_version, draft_version, task_id)
			 SELECT $1, id, name, $3, $4, $5 FROM flows WHERE account_id = $2 AND name = $6`,
			id, accountID(ctx), f.From, f.Draft, task, f.Flow); err != nil {
			return "", err
		}
	}
	return id, tx.Commit(ctx)
}

// GetUpgradeBatch reads a batch with its flows and each flow's test-task
// state. The task state is joined live rather than copied in, so the report
// reflects a run that finished after staging.
func (s *Store) GetUpgradeBatch(ctx context.Context, id string) (UpgradeBatch, error) {
	var b UpgradeBatch
	err := s.pool.QueryRow(ctx,
		`SELECT id, connector, target, created_at, created_by, published_at
		   FROM connector_upgrade_batches WHERE account_id = $1 AND id = $2`,
		accountID(ctx), id).Scan(&b.ID, &b.Connector, &b.Target, &b.Created, &b.CreatedBy, &b.Published)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT uf.flow_name, uf.from_version, uf.draft_version,
		        COALESCE(uf.task_id::text, ''), COALESCE(t.state, ''),
		        uf.notices, uf.published_at
		   FROM connector_upgrade_flows uf
		   LEFT JOIN tasks t ON t.id = uf.task_id
		  WHERE uf.batch_id = $1
		  ORDER BY uf.flow_name`, id)
	if err != nil {
		return b, err
	}
	defer rows.Close()
	b.Flows = []UpgradeFlow{}
	for rows.Next() {
		var f UpgradeFlow
		if err := rows.Scan(&f.Flow, &f.From, &f.Draft, &f.TaskID, &f.TaskState, &f.Notices, &f.Published); err != nil {
			return b, err
		}
		b.Flows = append(b.Flows, f)
	}
	return b, rows.Err()
}

// UpgradeBatches lists recent batches (without their flows) for the audit view.
func (s *Store) UpgradeBatches(ctx context.Context, limit int) ([]UpgradeBatch, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT b.id, b.connector, b.target, b.created_at, b.created_by, b.published_at
		   FROM connector_upgrade_batches b
		  WHERE b.account_id = $1
		  ORDER BY b.created_at DESC LIMIT $2`,
		accountID(ctx), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UpgradeBatch{}
	for rows.Next() {
		var b UpgradeBatch
		if err := rows.Scan(&b.ID, &b.Connector, &b.Target, &b.Created, &b.CreatedBy, &b.Published); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UntestedFlows names the flows in a batch whose test run did not pass.
//
// "Did not pass" covers failed, still running, and never queued. An absent
// result is not a good result — a batch whose tasks are all still leased has
// proven nothing, and letting it through would make step 2 decoration.
func (s *Store) UntestedFlows(ctx context.Context, batchID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT uf.flow_name
		   FROM connector_upgrade_flows uf
		   LEFT JOIN tasks t ON t.id = uf.task_id
		  WHERE uf.batch_id = $1 AND (t.state IS NULL OR t.state <> 'completed')
		  ORDER BY uf.flow_name`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// RecordBatchPublish stamps one flow's audit row with the notices its draft
// carried at the moment it went live.
func (s *Store) RecordBatchPublish(ctx context.Context, batchID, flow string, notices json.RawMessage) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE connector_upgrade_flows SET notices = $3, published_at = now()
		  WHERE batch_id = $1 AND flow_name = $2`,
		batchID, flow, notices)
	return err
}

// CloseUpgradeBatch marks the batch published. It returns ErrAlreadyPublished
// rather than silently succeeding, so a retried request cannot report a second
// publish as if it had done the work.
func (s *Store) CloseUpgradeBatch(ctx context.Context, batchID string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE connector_upgrade_batches SET published_at = now()
		  WHERE account_id = $1 AND id = $2 AND published_at IS NULL`,
		accountID(ctx), batchID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAlreadyPublished
	}
	return nil
}
