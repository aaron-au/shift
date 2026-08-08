package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	// Checkpoint is the resume position the previous attempt's sink confirmed
	// (ADR-0037), with the connector and version that produced it. Opaque to
	// the hub. Delivered on lease so a re-dispatched task -- typically on a
	// different runner -- restarts where the last attempt reached, and pinned
	// so it is never handed to a build that could read it as a different
	// position.
	// Test marks a task as a TEST execution (ADR-0048 §3): metered
	// separately, excluded from billing, and the only kind a test runner may
	// claim. Set only by the studio's run-now and an API execution that flags
	// itself — never by a schedule or a webhook.
	Test bool `json:"test,omitempty"`

	Checkpoint          []byte `json:"checkpoint,omitempty"`
	CheckpointConnector string `json:"checkpoint_connector,omitempty"`
	CheckpointVersion   string `json:"checkpoint_version,omitempty"`
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

// Enqueue queues one execution of the named flow (version 0 = the
// published version; explicit draft versions are allowed for
// smoke-testing). With an idempotency key, re-enqueueing returns the
// existing task id instead of creating a duplicate.
func (s *Store) Enqueue(ctx context.Context, flowName string, version int, idempotencyKey string, maxAttempts int) (string, error) {
	return s.enqueue(ctx, flowName, version, idempotencyKey, maxAttempts, false)
}

// EnqueueTest queues a TEST execution (ADR-0048 §3): metered separately,
// excluded from billing, and claimable by a test runner.
//
// A separate method rather than a boolean on Enqueue, deliberately. A trailing
// bool on a five-argument call is invisible at the call site and transposes
// silently; a named method cannot be passed by accident, and grepping for the
// two callers allowed to mark work as test — the studio's run-now and an
// explicitly-flagged API execution — is then a grep for one identifier.
func (s *Store) EnqueueTest(ctx context.Context, flowName string, version int, idempotencyKey string, maxAttempts int) (string, error) {
	return s.enqueue(ctx, flowName, version, idempotencyKey, maxAttempts, true)
}

func (s *Store) enqueue(ctx context.Context, flowName string, version int, idempotencyKey string, maxAttempts int, test bool) (string, error) {
	f, doc, err := s.GetFlow(ctx, flowName, version)
	if err != nil {
		return "", err
	}
	if version <= 0 {
		version = f.PublishedVersion
	}
	maxAttempts = effectiveMaxAttempts(doc, maxAttempts)

	return enqueueTx(ctx, s.pool, accountID(ctx), f.ID, f.Name, version, doc, idempotencyKey, maxAttempts, test)
}

// effectiveMaxAttempts resolves a task's attempt ceiling. A flow declared
// at_most_once caps at 1 and cannot be overridden by a trigger requesting more
// (the flow's non-idempotent safety intent wins — ADR-0002, issue #11);
// otherwise the trigger's request applies, defaulting when unset.
func effectiveMaxAttempts(doc json.RawMessage, requested int) int {
	if flowdoc.DeliveryFromDoc(doc) == flowdoc.DeliveryAtMostOnce {
		return 1
	}
	if requested > 0 {
		return requested
	}
	return flowdoc.DefaultMaxAttempts
}

// queryExecer is the slice of pgx both *pgxpool.Pool and pgx.Tx satisfy,
// so Enqueue (pool) and FireDue (tx) share one INSERT path.
type queryExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// enqueueTx inserts one task under an explicit account (FireDue crosses
// accounts; the context's account would be wrong there). With an
// idempotency key, a replay returns the existing task's id.
func enqueueTx(ctx context.Context, q queryExecer, account, flowID, flowName string, version int, doc json.RawMessage, idempotencyKey string, maxAttempts int, test bool) (string, error) {
	id := newUUID()
	var key *string
	if idempotencyKey != "" {
		key = &idempotencyKey
	}
	tag, err := q.Exec(ctx,
		`INSERT INTO tasks (id, account_id, flow_id, flow_name, flow_version, document, idempotency_key, max_attempts, test)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (account_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		id, account, flowID, flowName, version, doc, key, maxAttempts, test)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 1 {
		return id, nil
	}
	// Idempotent replay: hand back the original task.
	var existing string
	err = q.QueryRow(ctx,
		`SELECT id FROM tasks WHERE account_id = $1 AND idempotency_key = $2`,
		account, idempotencyKey).Scan(&existing)
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
	if err := s.ReapExpired(ctx); err != nil {
		return nil, err
	}
	// What this runner is FOR is read from the ROSTER, never from the claim
	// (ADR-0048 §1). A test runner takes test-marked work only.
	//
	// The converse is deliberately NOT true: test-marked work may also run on
	// a production runner. Forbidding that would mean run-now stops working
	// entirely in every deployment that has not registered a test runner —
	// turning an additive capability into a breaking change — and the tier's
	// commercial purpose is to keep test load OFF production capacity by
	// choice, not to make testing impossible without it. The metering
	// dimension travels with the task either way (§4).
	tier, err := s.RunnerTier(ctx, runnerID)
	if err != nil {
		return nil, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	t := &Task{State: "leased", LeasedBy: runnerID}
	// testOnly is the whole filter: true restricts a test runner to
	// test-marked work; false leaves a production runner able to take either.
	testOnly := tier == TierTest
	err = tx.QueryRow(ctx,
		`UPDATE tasks SET state = 'leased', leased_by = $1, attempt = attempt + 1,
		        lease_expires_at = now() + make_interval(secs => $2), started_at = COALESCE(started_at, now())
		 WHERE id = (
		   SELECT id FROM tasks WHERE state = 'queued' AND account_id = $3
		     AND (NOT $4 OR test)
		   ORDER BY enqueued_at
		   FOR UPDATE SKIP LOCKED
		   LIMIT 1)
		 RETURNING id, flow_name, flow_version, document,
		           COALESCE(idempotency_key, ''), attempt, max_attempts, enqueued_at,
		           checkpoint, COALESCE(checkpoint_connector, ''), COALESCE(checkpoint_version, ''), test`,
		runnerID, leaseTTL.Seconds(), accountID(ctx), testOnly).Scan(
		&t.ID, &t.FlowName, &t.FlowVersion, &t.Document,
		&t.IdempotencyKey, &t.Attempt, &t.MaxAttempts, &t.Enqueued,
		&t.Checkpoint, &t.CheckpointConnector, &t.CheckpointVersion, &t.Test)
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

// ReapExpired handles crashed runners: expired leases go back to the
// queue, or fail permanently once attempts are exhausted. Attempt history
// records the expiry either way. It runs at every claim and periodically
// from the scheduler loop (so expiries surface without claim traffic).
func (s *Store) ReapExpired(ctx context.Context) error {
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

	// Attempts remain → back to the queue for re-dispatch. The `attempt <
	// max_attempts` guard MUST be here too, not just on the terminal-fail
	// statement above: the two are separate round-trips with independent now(),
	// so a lease expiring in the window between them would otherwise be missed
	// by terminal-fail and requeued here, exceeding max_attempts by one.
	rows, err = s.pool.Query(ctx,
		`UPDATE tasks SET state = 'queued', leased_by = NULL, lease_expires_at = NULL
		 WHERE state = 'leased' AND lease_expires_at < now() AND attempt < max_attempts
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
	return s.HeartbeatWithCheckpoint(ctx, taskID, runnerID, leaseTTL, Checkpoint{})
}

// Checkpoint is a resume position and the connector build that produced it.
// A zero value means "no position to record" and leaves any stored one alone.
type Checkpoint struct {
	Cursor    []byte
	Connector string
	Version   string
}

// HeartbeatWithCheckpoint extends the lease and, when cur carries a position,
// records it (ADR-0037).
//
// It rides the heartbeat because that is the control-plane call that already
// exists at the right cadence, and because it must be gated by exactly the
// same lease check: a runner whose lease expired has been superseded, and
// letting it write a cursor would let a zombie rewind the attempt that
// replaced it. Same WHERE clause, same 409 -- deliberately not a separate
// endpoint that could drift from it.
func (s *Store) HeartbeatWithCheckpoint(ctx context.Context, taskID, runnerID string, leaseTTL time.Duration, cur Checkpoint) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET lease_expires_at = now() + make_interval(secs => $3),
		        checkpoint = COALESCE($4, checkpoint),
		        checkpoint_connector = COALESCE(NULLIF($5, ''), checkpoint_connector),
		        checkpoint_version = COALESCE(NULLIF($6, ''), checkpoint_version)
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased' AND lease_expires_at > now()`,
		taskID, runnerID, leaseTTL.Seconds(), nilIfEmpty(cur.Cursor), cur.Connector, cur.Version)
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
	var acct, flowName string
	var isTest bool
	var started, finished *time.Time
	err = tx.QueryRow(ctx,
		`SELECT attempt, max_attempts, account_id, flow_name, started_at, test FROM tasks
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased'
		 FOR UPDATE`,
		taskID, runnerID).Scan(&attempt, &maxAttempts, &acct, &flowName, &started, &isTest)
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
		err = tx.QueryRow(ctx,
			`UPDATE tasks SET state = 'failed', finished_at = now(), leased_by = NULL, lease_expires_at = NULL, error = $2
			 WHERE id = $1 RETURNING finished_at`, taskID, errMsg).Scan(&finished)
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
	// Meter only the terminal failure (M6d): a requeued attempt is not a
	// billable execution outcome — it will meter once it reaches a terminal
	// state. No result payload on failure, so record counts are zero.
	if !requeued {
		if err := recordUsage(ctx, tx, acct, UsageSourceTask, flowName, "failed", 0, 0, execSeconds(started, finished), isTest); err != nil {
			return false, err
		}
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
	var acct, flowName string
	var isTest bool
	var started, finished *time.Time
	err = tx.QueryRow(ctx,
		`UPDATE tasks SET state = $3, finished_at = now(), result = $4, error = NULLIF($5,''),
		        leased_by = NULL, lease_expires_at = NULL
		 WHERE id = $1 AND leased_by = $2 AND state = 'leased'
		 RETURNING attempt, account_id, flow_name, started_at, finished_at, test`,
		taskID, runnerID, state, result, errMsg).Scan(&attempt, &acct, &flowName, &started, &finished, &isTest)
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
	// Metering row in the same tx: a terminal task always has its usage record
	// (M6d). Counts come from the runner's result; the task's own account_id is
	// authoritative over the request context.
	in, out := parseResultMetrics(result)
	// The task's OWN test marker decides, not the request context: the flag
	// travelled with the work from the moment it was enqueued (ADR-0048 §3).
	if err := recordUsage(ctx, tx, acct, UsageSourceTask, flowName, state, in, out, execSeconds(started, finished), isTest); err != nil {
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
		        COALESCE(leased_by::text,''), enqueued_at, started_at, finished_at, COALESCE(error,''), result, test
		 FROM tasks WHERE id = $1 AND account_id = $2`, id, accountID(ctx)).Scan(
		&t.ID, &t.FlowName, &t.FlowVersion, &t.IdempotencyKey, &t.State, &t.Attempt, &t.MaxAttempts,
		&t.LeasedBy, &t.Enqueued, &t.Started, &t.Finished, &t.Error, &t.Result, &t.Test)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	return t, err
}

// TaskAttempts lists a task's lease history.
func (s *Store) TaskAttempts(ctx context.Context, id string) ([]TaskAttempt, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT a.attempt, COALESCE(a.runner_id::text,''), a.started_at, a.finished_at, COALESCE(a.outcome,''), COALESCE(a.error,'')
		 FROM task_attempts a JOIN tasks t ON t.id = a.task_id
		 WHERE a.task_id = $1 AND t.account_id = $2 ORDER BY a.attempt`, id, accountID(ctx))
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
		        COALESCE(leased_by::text,''), enqueued_at, started_at, finished_at, COALESCE(error,''), result, test
		 FROM tasks WHERE account_id = $2 ORDER BY enqueued_at DESC LIMIT $1`, limit, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(
			&t.ID, &t.FlowName, &t.FlowVersion, &t.IdempotencyKey, &t.State, &t.Attempt, &t.MaxAttempts,
			&t.LeasedBy, &t.Enqueued, &t.Started, &t.Finished, &t.Error, &t.Result, &t.Test); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// nilIfEmpty maps an empty cursor to SQL NULL so COALESCE keeps the stored
// value: a heartbeat with nothing new to say must not erase real progress.
func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
