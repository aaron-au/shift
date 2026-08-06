package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Caller-facing status for asynchronous requests (ADR-0042 §3).
//
// This is not the task queue and not the history table. It exists from the
// moment a request is ACCEPTED so the 202's status URL resolves immediately,
// carries metadata only, and is pruned once the caller has read it.

// Execution status states.
const (
	StatusAccepted  = "accepted"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// DefaultStatusTTL bounds a row nobody reads. A caller that never polls must
// not keep a row forever.
const DefaultStatusTTL = 7 * 24 * time.Hour

// ConsumedGrace is how long a consumed row survives its first terminal read.
// Clients poll twice; deleting on the read itself makes the second poll look
// like a forgery.
const ConsumedGrace = time.Hour

// ExecutionStatus is one accepted request's caller-visible state.
type ExecutionStatus struct {
	ID       string `json:"task"`
	FlowName string `json:"flow"`
	State    string `json:"state"`

	RecordsIn  int64 `json:"records_in"`
	RecordsOut int64 `json:"records_out"`

	// ErrorStep/ErrorCode/Error are the canonical error shape (ADR-0031): the
	// step and class of failure, never record content — an internet caller
	// reads this.
	ErrorStep string `json:"error_step,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Error     string `json:"error,omitempty"`

	AcceptedAt time.Time  `json:"accepted_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`

	// Route and Principal are authorisation inputs, not caller-visible output.
	Route     string `json:"-"`
	Principal string `json:"-"`
}

// Terminal reports whether the execution has finished.
func (e *ExecutionStatus) Terminal() bool {
	return e.State == StatusCompleted || e.State == StatusFailed
}

// AcceptExecution records an accepted request under the caller's account.
//
// The id is supplied by the RUNNER (ADR-0042 §3a): it has already been quoted
// to the caller, or is about to be, so the hub takes it rather than inventing
// one. tokenSHA256 is set only for anonymous routes, where the status URL
// carries a capability token instead of relying on a principal.
func (s *Store) AcceptExecution(ctx context.Context, runnerID string, e ExecutionStatus, tokenSHA256 string, ttl time.Duration) error {
	if e.ID == "" || e.FlowName == "" {
		return errors.New("execution status: id and flow are required")
	}
	if ttl <= 0 {
		ttl = DefaultStatusTTL
	}
	var runner any
	if runnerID != "" {
		runner = runnerID
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO execution_status
		   (id, account_id, runner_id, flow_name, route, principal, token_sha256, state, expires_at)
		 VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,now()+$9::interval)`,
		e.ID, accountID(ctx), runner, e.FlowName, e.Route, e.Principal, tokenSHA256,
		StatusAccepted, fmt.Sprintf("%d seconds", int64(ttl.Seconds())))
	if err != nil {
		// A primary-key collision is a fresh id, never a silent overwrite: two
		// requests must not share one status row.
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "23505" {
			return ErrStatusIDTaken
		}
		return err
	}
	return nil
}

// ErrStatusIDTaken means the runner-minted id already exists. The caller mints
// another rather than overwriting.
var ErrStatusIDTaken = errors.New("execution status: id already exists")

// ErrStatusGone means the row existed and has been consumed and pruned.
var ErrStatusGone = errors.New("execution status: gone")

// FinishExecution moves a row to its terminal state.
//
// The account clause is load-bearing, not defensive habit: without it a buggy
// or compromised runner could finalise another tenant's row (ADR-0042 §3a).
func (s *Store) FinishExecution(ctx context.Context, e ExecutionStatus) error {
	if e.State != StatusCompleted && e.State != StatusFailed && e.State != StatusRunning {
		return fmt.Errorf("execution status: %q is not a reportable state", e.State)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE execution_status
		    SET state = $3, records_in = $4, records_out = $5,
		        error_step = NULLIF($6,''), error_code = NULLIF($7,''), error = NULLIF($8,''),
		        started_at = COALESCE($9, started_at),
		        finished_at = $10
		  WHERE id = $1 AND account_id = $2`,
		e.ID, accountID(ctx), e.State, e.RecordsIn, e.RecordsOut,
		e.ErrorStep, e.ErrorCode, e.Error, e.StartedAt, e.FinishedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Either the id is unknown or it belongs to another account, and from
		// here those are the same answer.
		return pgx.ErrNoRows
	}
	return nil
}

// ExecutionStatusByID reads a status row for a caller.
//
// Authorisation is the caller's business but the CHECKS live here, because
// this is the only place that holds what the row claims. All three failure
// modes return the same pgx.ErrNoRows to the handler, which turns it into a
// 404: a distinguishable response would confirm that someone else's task
// exists under that id.
//
//   - unknown id, or another account's id
//   - a route that does not match the row's
//   - a principal (or capability token) that does not match
func (s *Store) ExecutionStatusByID(ctx context.Context, id, route, principal, tokenSHA256 string) (*ExecutionStatus, error) {
	var e ExecutionStatus
	var dbRoute, dbPrincipal string
	var dbToken *string
	var consumed *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT id, flow_name, route, principal, token_sha256, state,
		        records_in, records_out,
		        COALESCE(error_step,''), COALESCE(error_code,''), COALESCE(error,''),
		        accepted_at, started_at, finished_at, consumed_at
		   FROM execution_status
		  WHERE id = $1 AND account_id = $2`, id, accountID(ctx)).
		Scan(&e.ID, &e.FlowName, &dbRoute, &dbPrincipal, &dbToken, &e.State,
			&e.RecordsIn, &e.RecordsOut,
			&e.ErrorStep, &e.ErrorCode, &e.Error,
			&e.AcceptedAt, &e.StartedAt, &e.FinishedAt, &consumed)
	if err != nil {
		return nil, err
	}

	// The route the status was read under must be the route that accepted the
	// work. A caller authorised for two routes still cannot read one's ids
	// through the other's path.
	if dbRoute != "" && route != "" && dbRoute != route {
		return nil, pgx.ErrNoRows
	}
	if dbToken != nil {
		// A capability URL: the token IS the authorisation, and a missing or
		// wrong one is indistinguishable from an unknown id.
		if tokenSHA256 == "" || *dbToken != tokenSHA256 {
			return nil, pgx.ErrNoRows
		}
	} else if dbPrincipal != principal {
		return nil, pgx.ErrNoRows
	}

	if consumed != nil {
		// Already read and awaiting the sweeper. Gone is kinder than 404 here
		// and leaks nothing: this caller has already proved the capability.
		return nil, ErrStatusGone
	}
	e.Route, e.Principal = dbRoute, dbPrincipal
	return &e, nil
}

// ConsumeExecutionStatus stamps the first terminal read. It is separate from
// the read so that a failed authorisation never marks a row consumed.
func (s *Store) ConsumeExecutionStatus(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE execution_status SET consumed_at = now()
		  WHERE id = $1 AND account_id = $2 AND consumed_at IS NULL
		    AND state IN ('completed','failed')`, id, accountID(ctx))
	return err
}

// SweepExecutionStatus deletes consumed rows past the grace period and unread
// rows past their TTL. It returns how many it removed.
//
// It is account-agnostic on purpose: this is a maintenance sweep over the whole
// table, run by the hub rather than on behalf of a caller.
func (s *Store) SweepExecutionStatus(ctx context.Context, grace time.Duration) (int64, error) {
	if grace <= 0 {
		grace = ConsumedGrace
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM execution_status
		  WHERE (consumed_at IS NOT NULL AND consumed_at < now() - $1::interval)
		     OR expires_at < now()`,
		fmt.Sprintf("%d seconds", int64(grace.Seconds())))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
