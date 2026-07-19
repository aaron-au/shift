package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrLeaseLost means the caller no longer holds the task's lease (it
// expired and was re-dispatched, or the task already finished).
var ErrLeaseLost = errors.New("store: lease lost")

// Task is a queue entry.
type Task struct {
	ID             string          `json:"id"`
	FlowName       string          `json:"flow_name"`
	FlowVersion    int             `json:"flow_version"`
	Document       json.RawMessage `json:"document,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	State          string          `json:"state"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	LeasedBy       string          `json:"leased_by,omitempty"`
	Enqueued       time.Time       `json:"enqueued_at"`
	Started        *time.Time      `json:"started_at,omitempty"`
	Finished       *time.Time      `json:"finished_at,omitempty"`
	Error          string          `json:"error,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
}

// TaskAttempt is one lease of a task.
type TaskAttempt struct {
	Attempt  int        `json:"attempt"`
	RunnerID string     `json:"runner_id,omitempty"`
	Started  time.Time  `json:"started_at"`
	Finished *time.Time `json:"finished_at,omitempty"`
	Outcome  string     `json:"outcome,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// Enqueue queues one execution of the named flow (version 0 = latest).
// With an idempotency key, re-enqueueing returns the existing task id
// instead of creating a duplicate.
func (s *Store) Enqueue(ctx context.Context, flowName string, version int, idempotencyKey string, maxAttempts int) (string, error) {
	f, doc, err := s.GetFlow(ctx, flowName, version)
	if err != nil {
		return "", err
	}
	if version <= 0 {
		version = f.LatestVersion
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	id := newUUID()
	var key *string
	if idempotencyKey != "" {
		key = &idempotencyKey
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO tasks (id, account_id, flow_id, flow_name, flow_version, document, idempotency_key, max_attempts)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (account_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		id, DefaultAccountID, f.ID, f.Name, version, doc, key, maxAttempts)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 1 {
		return id, nil
	}
	// Idempotent replay: hand back the original task.
	var existing string
	err = s.pool.QueryRow(ctx,
		`SELECT id FROM tasks WHERE account_id = $1 AND idempotency_key = $2`,
		DefaultAccountID, idempotencyKey).Scan(&existing)
	if err != nil {
		return "", fmt.Errorf("store: idempotent enqueue lookup: %w", err)
	}
	return existing, nil
}

// Claim leases the oldest runnable task for a runner. It first reaps
// expired leases (requeue, or fail permanently once attempts are
// exhausted), then claims with FOR UPDATE SKIP LOCKED so concurrent hubs
// and runners never double-dispatch. Returns nil when the queue is empty.
func (s *Store) Claim(ctx context.Context, runnerID string, leaseTTL time.Duration) (*Task, error) {
	if err := s.reapExpired(ctx); err != nil {
		return nil, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	t := &Task{State: "leased", LeasedBy: runnerID}
	err = tx.QueryRow(ctx,
		`UPDATE tasks SET state = 'leased', leased_by = $1, attempt = attempt + 1,
		        lease_expires_at = now() + make_interval(secs => $2), started_at = COALESCE(started_at, now())
		 WHERE id = (
		   SELECT id FROM tasks WHERE state = 'queued'
		   ORDER BY enqueued_at
		   FOR UPDATE SKIP LOCKED
		   LIMIT 1)
		 RETURNING id, flow_name, flow_version, document,
		           COALESCE(idempotency_key, ''), attempt, max_attempts, enqueued_at`,
		runnerID, leaseTTL.Seconds()).Scan(
		&t.ID, &t.FlowName, &t.FlowVersion, &t.Document,
		&t.IdempotencyKey, &t.Attempt, &t.MaxAttempts, &t.Enqueued)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO task_attempts (task_id, attempt, runner_id) VALUES ($1,$2,$3)`,
		t.ID, t.Attempt, runnerID); err != nil {
		return nil, err
	}
	return t, tx.Commit(ctx)
}

// reapExpired handles crashed runners: expired leases go back to the
// queue, or fail permanently once attempts are exhausted. Attempt history
// records the expiry either way.
func (s *Store) reapExpired(ctx context.Context) error {
	// Attempts exhausted → terminal failure.
	rows, err := s.pool.Query(ctx,
		`UPDATE tasks SET state = 'failed', finished_at = now(), leased_by = NULL, lease_expires_at = NULL,
		        error = 'lease expired after ' || attempt || ' attempt(s); runner presumed dead'
		 WHERE state = 'leased' AND lease_expires_at < now() AND attempt >= max_attempts
		 RETURNING id, attempt`)
	if err != nil {
		return fmt.Errorf("store: reap failed: %w", err)
	}
	expired, err := collectExpired(rows)
	if err != nil {
		return err
	}

	// Attempts remain → back to the queue for re-dispatch.
	rows, err = s.pool.Query(ctx,
		`UPDATE tasks SET state = 'queued', leased_by = NULL, lease_expires_at = NULL
		 WHERE state = 'leased' AND lease_expires_at < now()
		 RETURNING id, attempt`)
	if err != nil {
		return fmt.Errorf("store: reap requeue: %w", err)
	}
	requeued, err := collectExpired(rows)
	if err != nil {
		return err
	}

	for _, e := range append(expired, requeued...) {
		if _, err := s.pool.Exec(ctx,
			`UPDATE task_attempts SET finished_at = now(), outcome = 'lease-expired'
			 WHERE task_id = $1 AND attempt = $2 AND finished_at IS NULL`,
			e.id, e.attempt); err != nil {
			return err
		}
	}
	return nil
}

type expiredLease struct {
	id      string
	attempt int
}

func collectExpired(rows pgx.Rows) ([]expiredLease, error) {
	defer rows.Close()
	var out []expiredLease
	for rows.Next() {
		var e expiredLease
		if err := rows.Scan(&e.id, &e.attempt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Heartbeat extends a held lease. An expired or reassigned lease cannot
// be resurrected — the runner gets ErrLeaseLost and must abandon the task.
func (s *Store) Heartbeat(ctx context.Context, taskID, runnerID string, leaseTTL time.Duration) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET lease_expires_at = now() + make_interval(secs => $3)
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased' AND lease_expires_at > now()`,
		taskID, runnerID, leaseTTL.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete finishes a held task with its result document.
func (s *Store) Complete(ctx context.Context, taskID, runnerID string, result json.RawMessage) error {
	return s.finish(ctx, taskID, runnerID, "completed", "", result)
}

// Fail reports a failed attempt: the task is requeued while attempts
// remain, and fails permanently otherwise.
func (s *Store) Fail(ctx context.Context, taskID, runnerID, errMsg string) (requeued bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempt, maxAttempts int
	err = tx.QueryRow(ctx,
		`SELECT attempt, max_attempts FROM tasks
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased'
		 FOR UPDATE`,
		taskID, runnerID).Scan(&attempt, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrLeaseLost
	}
	if err != nil {
		return false, err
	}

	requeued = attempt < maxAttempts
	if requeued {
		_, err = tx.Exec(ctx,
			`UPDATE tasks SET state = 'queued', leased_by = NULL, lease_expires_at = NULL, error = $2
			 WHERE id = $1`, taskID, errMsg)
	} else {
		_, err = tx.Exec(ctx,
			`UPDATE tasks SET state = 'failed', finished_at = now(), leased_by = NULL, lease_expires_at = NULL, error = $2
			 WHERE id = $1`, taskID, errMsg)
	}
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE task_attempts SET finished_at = now(), outcome = 'failed', error = $3
		 WHERE task_id = $1 AND attempt = $2`,
		taskID, attempt, errMsg); err != nil {
		return false, err
	}
	return requeued, tx.Commit(ctx)
}

func (s *Store) finish(ctx context.Context, taskID, runnerID, state, errMsg string, result json.RawMessage) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempt int
	err = tx.QueryRow(ctx,
		`UPDATE tasks SET state = $3, finished_at = now(), result = $4, error = NULLIF($5,''),
		        leased_by = NULL, lease_expires_at = NULL
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased'
		 RETURNING attempt`,
		taskID, runnerID, state, result, errMsg).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE task_attempts SET finished_at = now(), outcome = $3, error = NULLIF($4,'')
		 WHERE task_id = $1 AND attempt = $2`,
		taskID, attempt, state, errMsg); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetTask fetches one task (without its document — use Claim for
// execution payloads).
func (s *Store) GetTask(ctx context.Context, id string) (Task, error) {
	var t Task
	err := s.pool.QueryRow(ctx,
		`SELECT id, flow_name, flow_version, COALESCE(idempotency_key,''), state, attempt, max_attempts,
		        COALESCE(leased_by::text,''), enqueued_at, started_at, finished_at, COALESCE(error,''), result
		 FROM tasks WHERE id = $1`, id).Scan(
		&t.ID, &t.FlowName, &t.FlowVersion, &t.IdempotencyKey, &t.State, &t.Attempt, &t.MaxAttempts,
		&t.LeasedBy, &t.Enqueued, &t.Started, &t.Finished, &t.Error, &t.Result)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	return t, err
}

// TaskAttempts lists a task's lease history.
func (s *Store) TaskAttempts(ctx context.Context, id string) ([]TaskAttempt, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT attempt, COALESCE(runner_id::text,''), started_at, finished_at, COALESCE(outcome,''), COALESCE(error,'')
		 FROM task_attempts WHERE task_id = $1 ORDER BY attempt`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskAttempt
	for rows.Next() {
		var a TaskAttempt
		if err := rows.Scan(&a.Attempt, &a.RunnerID, &a.Started, &a.Finished, &a.Outcome, &a.Error); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Tasks lists recent tasks, newest first.
func (s *Store) Tasks(ctx context.Context, limit int) ([]Task, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, flow_name, flow_version, COALESCE(idempotency_key,''), state, attempt, max_attempts,
		        COALESCE(leased_by::text,''), enqueued_at, started_at, finished_at, COALESCE(error,''), result
		 FROM tasks ORDER BY enqueued_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(
			&t.ID, &t.FlowName, &t.FlowVersion, &t.IdempotencyKey, &t.State, &t.Attempt, &t.MaxAttempts,
			&t.LeasedBy, &t.Enqueued, &t.Started, &t.Finished, &t.Error, &t.Result); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
