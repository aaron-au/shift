package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Schedule is a flow's cron schedule.
type Schedule struct {
	ID          string     `json:"id"`
	FlowName    string     `json:"flow_name"`
	Cron        string     `json:"cron"`
	Enabled     bool       `json:"enabled"`
	NextFire    time.Time  `json:"next_fire_at"`
	LastFired   *time.Time `json:"last_fired_at,omitempty"`
	LastTaskID  string     `json:"last_task_id,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	MaxAttempts int        `json:"max_attempts"`
}

// UpsertSchedule creates or replaces the named flow's schedule. next is
// the first fire time — cron parsing lives in the scheduler package,
// the store stays SQL-only.
func (s *Store) UpsertSchedule(ctx context.Context, flowName, cronExpr string, enabled bool, maxAttempts int, next time.Time) (Schedule, error) {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	var sc Schedule
	sc.FlowName = flowName
	err := s.pool.QueryRow(ctx,
		`INSERT INTO schedules (id, account_id, flow_id, cron, enabled, next_fire_at, max_attempts)
		 SELECT $1, f.account_id, f.id, $4, $5, $6, $7
		   FROM flows f WHERE f.account_id = $2 AND f.name = $3
		 ON CONFLICT (flow_id)
		 DO UPDATE SET cron = $4, enabled = $5, next_fire_at = $6, max_attempts = $7, last_error = NULL
		 RETURNING id, cron, enabled, next_fire_at, max_attempts`,
		newUUID(), accountID(ctx), flowName, cronExpr, enabled, next, maxAttempts).Scan(
		&sc.ID, &sc.Cron, &sc.Enabled, &sc.NextFire, &sc.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotFound // unknown flow
	}
	return sc, err
}

// GetSchedule fetches the named flow's schedule.
func (s *Store) GetSchedule(ctx context.Context, flowName string) (Schedule, error) {
	sc, err := scanSchedule(s.pool.QueryRow(ctx,
		scheduleSelect+` WHERE f.account_id = $1 AND f.name = $2`, accountID(ctx), flowName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	return sc, err
}

// DeleteSchedule removes the named flow's schedule.
func (s *Store) DeleteSchedule(ctx context.Context, flowName string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM schedules sc USING flows f
		 WHERE sc.flow_id = f.id AND f.account_id = $1 AND f.name = $2`,
		accountID(ctx), flowName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Schedules lists the account's schedules by flow name.
func (s *Store) Schedules(ctx context.Context) ([]Schedule, error) {
	rows, err := s.pool.Query(ctx,
		scheduleSelect+` WHERE f.account_id = $1 ORDER BY f.name`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

const scheduleSelect = `
	SELECT sc.id, f.name, sc.cron, sc.enabled, sc.next_fire_at, sc.last_fired_at,
	       COALESCE(sc.last_task_id::text,''), COALESCE(sc.last_error,''), sc.max_attempts
	  FROM schedules sc JOIN flows f ON f.id = sc.flow_id`

func scanSchedule(row pgx.Row) (Schedule, error) {
	var sc Schedule
	err := row.Scan(&sc.ID, &sc.FlowName, &sc.Cron, &sc.Enabled, &sc.NextFire,
		&sc.LastFired, &sc.LastTaskID, &sc.LastError, &sc.MaxAttempts)
	return sc, err
}

// DueCount reports enabled schedules at or past their fire time
// (dashboard stat).
func (s *Store) DueCount(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM schedules WHERE enabled AND next_fire_at <= now()`).Scan(&n)
	return n, err
}

// FireDue enqueues one task for every enabled schedule whose
// next_fire_at has passed, and advances each schedule to its next tick.
// It is safe to run concurrently on every hub replica; the layers that
// make a tick fire exactly once:
//
//  1. pg_try_advisory_xact_lock(823401): at most one replica runs a
//     pass at a time — a liveness optimization, never load-bearing.
//  2. FOR UPDATE SKIP LOCKED: a schedule row is processed by at most
//     one transaction even without the advisory layer.
//  3. The task INSERT and the next_fire_at advance commit atomically:
//     a crash mid-pass rolls both back, and the next pass (any
//     replica) retries the SAME tick.
//  4. The idempotency key sched:<id>:<stored tick> rides the existing
//     unique index, so even an operator replay collapses to one task.
//
// Due-ness and the seed for the next tick both use Postgres now() —
// replica wall clocks never enter correctness. After downtime a
// schedule fires once and jumps forward (no catch-up storm).
//
// FireDue is a system pass across every account; each task is enqueued
// under its schedule's account, not the caller's.
func (s *Store) FireDue(ctx context.Context, nextFn func(cronExpr string, after time.Time) (time.Time, error), limit int) (fired int, err error) {
	if limit <= 0 {
		limit = 50
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var haveLock bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(823401)`).Scan(&haveLock); err != nil {
		return 0, err
	}
	if !haveLock {
		return 0, nil // another replica is mid-pass
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		return 0, err
	}

	rows, err := tx.Query(ctx,
		`SELECT sc.id, sc.account_id, sc.flow_id, sc.cron, sc.next_fire_at, sc.max_attempts,
		        f.name, f.published_version
		   FROM schedules sc JOIN flows f ON f.id = sc.flow_id
		  WHERE sc.enabled AND sc.next_fire_at <= now()
		  ORDER BY sc.next_fire_at
		  FOR UPDATE OF sc SKIP LOCKED
		  LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		id, account, flowID, cron, flowName string
		tick                                time.Time
		maxAttempts, published              int
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.account, &d.flowID, &d.cron, &d.tick,
			&d.maxAttempts, &d.flowName, &d.published); err != nil {
			rows.Close()
			return 0, err
		}
		dues = append(dues, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, d := range dues {
		next, nerr := nextFn(d.cron, dbNow)
		if nerr != nil {
			// Unparseable cron (edited under us?): park the schedule
			// instead of wedging the pass forever.
			if _, err := tx.Exec(ctx,
				`UPDATE schedules SET enabled = false, last_error = $2 WHERE id = $1`,
				d.id, "invalid cron: "+nerr.Error()); err != nil {
				return fired, err
			}
			continue
		}
		if d.published == 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE schedules SET next_fire_at = $2, last_error = $3 WHERE id = $1`,
				d.id, next, "flow has no published version"); err != nil {
				return fired, err
			}
			continue
		}

		var doc []byte
		if err := tx.QueryRow(ctx,
			`SELECT document FROM flow_versions WHERE flow_id = $1 AND version = $2`,
			d.flowID, d.published).Scan(&doc); err != nil {
			return fired, fmt.Errorf("store: schedule %s: version %d: %w", d.id, d.published, err)
		}

		// Full stored precision (µs after the Postgres round-trip), not
		// second-truncated: two distinct next_fire_at values are distinct
		// ticks and must get distinct keys. A re-dispatched *same* tick
		// re-reads the identical stored value, so dedup still holds.
		key := fmt.Sprintf("sched:%s:%s", d.id, d.tick.UTC().Format(time.RFC3339Nano))
		taskID, err := enqueueTx(ctx, tx, d.account, d.flowID, d.flowName, d.published, doc, key, d.maxAttempts)
		if err != nil {
			return fired, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE schedules SET next_fire_at = $2, last_fired_at = $3, last_task_id = $4, last_error = NULL
			 WHERE id = $1`, d.id, next, dbNow, taskID); err != nil {
			return fired, err
		}
		fired++
	}
	return fired, tx.Commit(ctx)
}
