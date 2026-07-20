package store

import (
	"context"
	"time"
)

// DirectExecution is a runner-reported push execution (webhook / direct
// API, ADR-0016): metadata only — the hub never sees the payload. It is not
// a queued/leased task; it arrives already terminal.
type DirectExecution struct {
	ID         string     `json:"id"`
	RunnerID   string     `json:"runner_id,omitempty"`
	FlowName   string     `json:"flow_name"`
	Trigger    string     `json:"trigger"` // webhook | api
	State      string     `json:"state"`   // completed | failed
	RecordsIn  int64      `json:"records_in"`
	RecordsOut int64      `json:"records_out"`
	Error      string     `json:"error,omitempty"`
	Started    *time.Time `json:"started_at,omitempty"`
	Finished   *time.Time `json:"finished_at,omitempty"`
	Created    time.Time  `json:"created_at"`
}

// RecordDirectExecution stores a runner-reported direct execution under the
// caller's account. runnerID is the reporting runner ("" if unknown).
func (s *Store) RecordDirectExecution(ctx context.Context, runnerID string, e DirectExecution) (string, error) {
	id := newUUID()
	var runner any
	if runnerID != "" {
		runner = runnerID
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO direct_executions
		   (id, account_id, runner_id, flow_name, trigger, state, records_in, records_out, error, started_at, finished_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$11)`,
		id, accountID(ctx), runner, e.FlowName, e.Trigger, e.State,
		e.RecordsIn, e.RecordsOut, e.Error, e.Started, e.Finished)
	return id, err
}

// DirectExecutions lists the account's recent direct executions, newest first.
func (s *Store) DirectExecutions(ctx context.Context, limit int) ([]DirectExecution, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, COALESCE(runner_id::text,''), flow_name, trigger, state,
		        records_in, records_out, COALESCE(error,''), started_at, finished_at, created_at
		   FROM direct_executions WHERE account_id = $1
		  ORDER BY created_at DESC LIMIT $2`, accountID(ctx), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DirectExecution
	for rows.Next() {
		var e DirectExecution
		if err := rows.Scan(&e.ID, &e.RunnerID, &e.FlowName, &e.Trigger, &e.State,
			&e.RecordsIn, &e.RecordsOut, &e.Error, &e.Started, &e.Finished, &e.Created); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
