package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Usage source values (the execution plane that produced the metric).
const (
	UsageSourceTask = "task" // queued/leased task (this hub's queue)
	// webhook / api sources are the direct-execution triggers (ADR-0016);
	// they arrive already-typed as DirectExecution.Trigger.
)

// execer is satisfied by both *pgxpool.Pool and pgx.Tx, so a usage row can be
// written either standalone or inside the completion transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// recordUsage appends one metering row. Metadata only — counts + seconds, never
// payload. accountID is passed explicitly (the task's own account, authoritative
// over the request context). Best-effort at the call site: a metering failure
// must never fail the execution it measures.
func recordUsage(ctx context.Context, q execer, accountID, source, flowName, outcome string, recordsIn, recordsOut int64, execSeconds float64, test bool) error {
	_, err := q.Exec(ctx,
		`INSERT INTO usage_events (account_id, source, flow_name, outcome, records_in, records_out, exec_seconds, test)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		accountID, source, flowName, outcome, recordsIn, recordsOut, execSeconds, test)
	return err
}

// parseResultMetrics best-effort-extracts the record counts from a runner's
// completion result (hubclient.Result JSON). A nil/malformed blob yields zeros —
// metering degrades to a bare count, never fails.
func parseResultMetrics(result json.RawMessage) (recordsIn, recordsOut int64) {
	if len(result) == 0 {
		return 0, 0
	}
	var m struct {
		RecordsIn  int64 `json:"records_in"`
		RecordsOut int64 `json:"records_out"`
	}
	_ = json.Unmarshal(result, &m)
	return m.RecordsIn, m.RecordsOut
}

// execSeconds is the wall duration of an execution, guarding nil timestamps and
// clock skew (never negative).
func execSeconds(started, finished *time.Time) float64 {
	if started == nil || finished == nil {
		return 0
	}
	sec := finished.Sub(*started).Seconds()
	if sec < 0 {
		return 0
	}
	return sec
}

// UsageTotals is the account-wide rollup over a time range.
type UsageTotals struct {
	Executions  int64   `json:"executions"`
	Completed   int64   `json:"completed"`
	Failed      int64   `json:"failed"`
	RecordsIn   int64   `json:"records_in"`
	RecordsOut  int64   `json:"records_out"`
	ExecSeconds float64 `json:"exec_seconds"`
}

// UsageByFlow is per-flow usage within the range. Executions and the record
// counts are BILLABLE only; TestExecutions is the same flow's test runs.
//
// Carried per flow rather than only in the totals because "who is hammering
// test mode" is the question §4's visibility exists to answer, and an account
// total cannot answer it.
type UsageByFlow struct {
	FlowName       string  `json:"flow_name"`
	Executions     int64   `json:"executions"`
	TestExecutions int64   `json:"test_executions,omitempty"`
	RecordsIn      int64   `json:"records_in"`
	RecordsOut     int64   `json:"records_out"`
	ExecSeconds    float64 `json:"exec_seconds"`
}

// UsageBucket is a daily time-series point.
type UsageBucket struct {
	Day         time.Time `json:"day"`
	Executions  int64     `json:"executions"`
	RecordsIn   int64     `json:"records_in"`
	RecordsOut  int64     `json:"records_out"`
	ExecSeconds float64   `json:"exec_seconds"`
}

// UsageReport is the aggregated view the Usage window renders.
//
// Totals, ByFlow and Series are BILLABLE usage — test executions are excluded
// from all three, because a number that mixes them is a number nobody can
// invoice from. Test is the same rollup over test-marked executions, reported
// beside it rather than hidden: §4 keeps test usage uncapped and makes it
// visible instead, which only works if somebody can see it.
type UsageReport struct {
	Since  time.Time     `json:"since"`
	Until  time.Time     `json:"until"`
	Totals UsageTotals   `json:"totals"`
	Test   UsageTotals   `json:"test"`
	ByFlow []UsageByFlow `json:"by_flow"`
	Series []UsageBucket `json:"series"`
}

// Usage aggregates the account's metering over [since, until). All three
// rollups (totals, per-flow, daily series) are account-scoped.
func (s *Store) Usage(ctx context.Context, since, until time.Time) (UsageReport, error) {
	acct := accountID(ctx)
	rep := UsageReport{Since: since, Until: until}

	// One pass, two rollups: billable and test. Splitting with FILTER rather
	// than two queries keeps them consistent — a row cannot land in neither or
	// both because a second statement saw a different snapshot.
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE NOT test),
		        count(*) FILTER (WHERE NOT test AND outcome='completed'),
		        count(*) FILTER (WHERE NOT test AND outcome='failed'),
		        COALESCE(sum(records_in)  FILTER (WHERE NOT test),0),
		        COALESCE(sum(records_out) FILTER (WHERE NOT test),0),
		        COALESCE(sum(exec_seconds)FILTER (WHERE NOT test),0),
		        count(*) FILTER (WHERE test),
		        count(*) FILTER (WHERE test AND outcome='completed'),
		        count(*) FILTER (WHERE test AND outcome='failed'),
		        COALESCE(sum(records_in)  FILTER (WHERE test),0),
		        COALESCE(sum(records_out) FILTER (WHERE test),0),
		        COALESCE(sum(exec_seconds)FILTER (WHERE test),0)
		   FROM usage_events
		  WHERE account_id=$1 AND at >= $2 AND at < $3`,
		acct, since, until).Scan(
		&rep.Totals.Executions, &rep.Totals.Completed, &rep.Totals.Failed,
		&rep.Totals.RecordsIn, &rep.Totals.RecordsOut, &rep.Totals.ExecSeconds,
		&rep.Test.Executions, &rep.Test.Completed, &rep.Test.Failed,
		&rep.Test.RecordsIn, &rep.Test.RecordsOut, &rep.Test.ExecSeconds); err != nil {
		return UsageReport{}, err
	}

	flows, err := s.pool.Query(ctx,
		`SELECT flow_name,
		        count(*) FILTER (WHERE NOT test),
		        count(*) FILTER (WHERE test),
		        COALESCE(sum(records_in)  FILTER (WHERE NOT test),0),
		        COALESCE(sum(records_out) FILTER (WHERE NOT test),0),
		        COALESCE(sum(exec_seconds)FILTER (WHERE NOT test),0)
		   FROM usage_events
		  WHERE account_id=$1 AND at >= $2 AND at < $3
		  GROUP BY flow_name ORDER BY count(*) DESC, flow_name`,
		acct, since, until)
	if err != nil {
		return UsageReport{}, err
	}
	defer flows.Close()
	for flows.Next() {
		var f UsageByFlow
		if err := flows.Scan(&f.FlowName, &f.Executions, &f.TestExecutions,
			&f.RecordsIn, &f.RecordsOut, &f.ExecSeconds); err != nil {
			return UsageReport{}, err
		}
		rep.ByFlow = append(rep.ByFlow, f)
	}
	if err := flows.Err(); err != nil {
		return UsageReport{}, err
	}

	series, err := s.pool.Query(ctx,
		`SELECT date_trunc('day', at) AS day, count(*), COALESCE(sum(records_in),0),
		        COALESCE(sum(records_out),0), COALESCE(sum(exec_seconds),0)
		   FROM usage_events
		  WHERE account_id=$1 AND at >= $2 AND at < $3 AND NOT test
		  GROUP BY day ORDER BY day`,
		acct, since, until)
	if err != nil {
		return UsageReport{}, err
	}
	defer series.Close()
	for series.Next() {
		var b UsageBucket
		if err := series.Scan(&b.Day, &b.Executions, &b.RecordsIn, &b.RecordsOut, &b.ExecSeconds); err != nil {
			return UsageReport{}, err
		}
		rep.Series = append(rep.Series, b)
	}
	return rep, series.Err()
}

// UsageEvent is one exported metering row (cursor-based pull).
type UsageEvent struct {
	ID          int64     `json:"id"`
	At          time.Time `json:"at"`
	Source      string    `json:"source"`
	FlowName    string    `json:"flow_name"`
	Outcome     string    `json:"outcome"`
	RecordsIn   int64     `json:"records_in"`
	RecordsOut  int64     `json:"records_out"`
	ExecSeconds float64   `json:"exec_seconds"`
	// Test excludes this row from billing (ADR-0048 §4). Exported per ROW
	// rather than filtered out here: the billing platform ingests a cursor
	// stream and must be able to classify what it receives without asking, and
	// dropping test rows from the export would also hide the usage §4 exists
	// to make visible.
	Test bool `json:"test,omitempty"`
}

// UsageEventsSince returns the account's metering rows with id > afterID, in id
// order — the incremental pull the external billing platform ingests. Bounded
// by limit (1..1000). A global/cross-tenant pull is future work (needs a
// system-scoped credential; the hub is not the account master).
func (s *Store) UsageEventsSince(ctx context.Context, afterID int64, limit int) ([]UsageEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, at, source, flow_name, outcome, records_in, records_out, exec_seconds, test
		   FROM usage_events
		  WHERE account_id=$1 AND id > $2
		  ORDER BY id LIMIT $3`,
		accountID(ctx), afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageEvent
	for rows.Next() {
		var e UsageEvent
		if err := rows.Scan(&e.ID, &e.At, &e.Source, &e.FlowName, &e.Outcome,
			&e.RecordsIn, &e.RecordsOut, &e.ExecSeconds, &e.Test); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
