package store

import (
	"context"
)

// Stats is the dashboard overview snapshot.
type Stats struct {
	Tasks           map[string]int `json:"tasks"`
	OldestQueuedSec float64        `json:"oldest_queued_seconds"`
	RunnersActive   int            `json:"runners_active"`
	RunnersTotal    int            `json:"runners_total"`
	SchedulesDue    int            `json:"schedules_due"`
	Schedules       int            `json:"schedules"`
	Flows           int            `json:"flows"`
}

// Stats aggregates the account's control-plane counters in one round
// trip per table.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Tasks: map[string]int{}}
	acct := accountID(ctx)

	// Grouped by state, so within the 'queued' group max(age) IS the
	// oldest queued task — no FILTER needed (and FILTER may only follow
	// an aggregate call, never EXTRACT(...)).
	rows, err := s.pool.Query(ctx,
		`SELECT state, count(*), COALESCE(EXTRACT(EPOCH FROM max(now() - enqueued_at)), 0)
		   FROM tasks WHERE account_id = $1 GROUP BY state`, acct)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var state string
		var n int
		var oldest float64
		if err := rows.Scan(&state, &n, &oldest); err != nil {
			rows.Close()
			return st, err
		}
		st.Tasks[state] = n
		if state == "queued" {
			st.OldestQueuedSec = oldest
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	if err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE last_seen_at > now() - interval '2 minutes')
		   FROM runners WHERE account_id = $1`, acct).Scan(&st.RunnersTotal, &st.RunnersActive); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE enabled AND next_fire_at <= now())
		   FROM schedules WHERE account_id = $1`, acct).Scan(&st.Schedules, &st.SchedulesDue); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM flows WHERE account_id = $1`, acct).Scan(&st.Flows); err != nil {
		return st, err
	}
	return st, nil
}

// PlatformStats aggregates the same counters as Stats but across ALL
// accounts — for the Prometheus scrape (/metrics), which has no auth/tenant
// context. Deliberately un-scoped: platform-wide operational metrics, not a
// per-tenant view (per-tenant metrics with a tenant label are later work).
func (s *Store) PlatformStats(ctx context.Context) (Stats, error) {
	st := Stats{Tasks: map[string]int{}}

	rows, err := s.pool.Query(ctx,
		`SELECT state, count(*), COALESCE(EXTRACT(EPOCH FROM max(now() - enqueued_at)), 0)
		   FROM tasks GROUP BY state`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var state string
		var n int
		var oldest float64
		if err := rows.Scan(&state, &n, &oldest); err != nil {
			rows.Close()
			return st, err
		}
		st.Tasks[state] = n
		if state == "queued" {
			st.OldestQueuedSec = oldest
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	if err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE last_seen_at > now() - interval '2 minutes')
		   FROM runners`).Scan(&st.RunnersTotal, &st.RunnersActive); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE enabled AND next_fire_at <= now())
		   FROM schedules`).Scan(&st.Schedules, &st.SchedulesDue); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM flows`).Scan(&st.Flows); err != nil {
		return st, err
	}
	return st, nil
}
