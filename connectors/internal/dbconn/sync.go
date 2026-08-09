package dbconn

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Incremental read — the DB half of "sync/CDC" as a first-class workload
// (ADR-0004), built on the resumable-source contract (ADR-0037).
//
// The cursor here is a WATERMARK, not a record ordinal, and that difference is
// the whole point. `fs get` resumes a single interrupted read by counting
// records; this resumes across RUNS. A scheduled sync fires hourly, and each
// run must start where the last one finished — so the cursor is the highest
// value of a monotonic column the previous run actually delivered, and the
// query is re-executed with it bound.
//
// Consequences worth being explicit about:
//
//   - The query MUST carry exactly one placeholder, $1, bound to the cursor,
//     and MUST order by the cursor column ascending. Without the ordering the
//     "highest value seen" is not the "highest value delivered", and any
//     interruption silently skips the rows in between. It is checked, not
//     documented-and-hoped.
//   - Rows sharing a watermark value can straddle a run boundary. The
//     comparison is therefore the caller's to write: `> $1` risks losing ties,
//     `>= $1` risks repeating them. `>=` plus an idempotent sink is the safe
//     pairing, and at-least-once delivery is the platform's contract anyway
//     (an idempotency key is injected for exactly this reason).
//   - A cursor is bound to the query that produced it. Editing the query
//     between runs and keeping the watermark would resume a new shape at an
//     old position, so the cursor records a fingerprint and refuses.
//
// This does NOT attempt log-based CDC (replication slots, WAL decoding). That
// captures deletes and needs privileges a read-only integration user should
// not have; a watermark read captures inserts and updates against a column the
// schema already maintains. Log-based CDC is its own connector when it comes.
const syncCursorVersion = 1

type syncCursor struct {
	V      int             `json:"v"`
	Column string          `json:"column"`
	Query  string          `json:"query_fp"`
	Value  json.RawMessage `json:"value"`
}

// syncSource streams rows newer than a watermark and reports the highest
// watermark it emitted.
type syncSource struct {
	cfg    config
	closer func() error
	rows   *sql.Rows
	q      querySource // the row→record mapping, reused wholesale

	cursorIdx int             // index of the cursor column in the result set
	high      json.RawMessage // highest watermark emitted so far
	started   bool
	resumed   json.RawMessage
}

func (s *syncSource) Open(ctx context.Context, cfgBytes []byte) error {
	if err := json.Unmarshal(cfgBytes, &s.cfg); err != nil {
		return fmt.Errorf("db: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if err := s.cfg.validateSync(); err != nil {
		return err
	}

	// The bound value: the resumed watermark, or the configured starting
	// point on a first run.
	bound := s.resumed
	if len(bound) == 0 {
		bound = s.cfg.CursorInitial
	}
	arg, err := cursorArg(bound)
	if err != nil {
		return err
	}

	db, closer, err := openDB(ctx, &s.cfg)
	if err != nil {
		return err
	}
	s.closer = closer
	rows, err := db.QueryContext(ctx, s.cfg.Query, arg) //nolint:sqlclosecheck // streaming cursor held across Next calls; closed in Close()
	if err != nil {
		_ = closer()
		return fmt.Errorf("db: sync query: %w", err)
	}
	if err := s.start(rows); err != nil {
		_ = rows.Close() //nolint:sqlclosecheck // error path; cursor otherwise held across Next and closed in Close()
		_ = closer()
		return err
	}
	return nil
}

// start wires the cursor and locates the watermark column in the result set.
// Split out from Open for the same reason querySource.start is: the mapping is
// testable against any *sql.Rows without a live database.
func (s *syncSource) start(rows *sql.Rows) error {
	if err := s.q.start(rows); err != nil {
		return err
	}
	s.rows = rows
	s.cursorIdx = -1
	for i, c := range s.q.cols {
		if strings.EqualFold(c, s.cfg.CursorColumn) {
			s.cursorIdx = i
			break
		}
	}
	if s.cursorIdx < 0 {
		// Fail here rather than emit rows whose watermark cannot be read. A
		// sync that returned data and no cursor would restart from the same
		// place every run and re-deliver the same rows forever.
		return fmt.Errorf("db: the query does not select the cursor column %q (selected: %s)",
			s.cfg.CursorColumn, strings.Join(s.q.cols, ", "))
	}
	s.started = true
	return nil
}

func (s *syncSource) Next(ctx context.Context) (*record.Batch, error) {
	b, err := s.q.Next(ctx)
	if err != nil {
		return nil, err
	}
	// Track the highest watermark in this batch. Taken from the SCAN buffer of
	// each row as it is mapped would be cheaper, but reading it back off the
	// built record keeps this independent of querySource's internals.
	for _, rec := range b.Records() {
		v, ok := rec.Field(s.q.cols[s.cursorIdx])
		if !ok {
			continue
		}
		raw, err := watermarkJSON(v)
		if err != nil {
			return nil, fmt.Errorf("db: cursor column %q: %w", s.cfg.CursorColumn, err)
		}
		s.high = raw
	}
	return b, nil
}

func (s *syncSource) Close() error {
	var first error
	if s.rows != nil {
		if err := s.rows.Close(); err != nil {
			first = err
		}
	}
	if s.closer != nil {
		if err := s.closer(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Resume binds the watermark the previous run reached.
func (s *syncSource) Resume(_ context.Context, cur []byte) error {
	if len(cur) == 0 {
		return nil // first run: start from cursor_initial
	}
	var c syncCursor
	if err := json.Unmarshal(cur, &c); err != nil {
		return fmt.Errorf("db: malformed resume cursor: %w", err)
	}
	if c.V != syncCursorVersion {
		return fmt.Errorf("db: resume cursor version %d, want %d", c.V, syncCursorVersion)
	}
	if !strings.EqualFold(c.Column, s.cfg.CursorColumn) {
		return fmt.Errorf("db: resume cursor tracks %q but this action syncs on %q", c.Column, s.cfg.CursorColumn)
	}
	if c.Query != queryFingerprint(s.cfg.Query) {
		// The node's query can be edited between runs. A watermark taken
		// against one query resumed into another starts a different result set
		// at an arbitrary point, and every row before it is silently lost.
		return errors.New("db: the query changed since this cursor was taken; " +
			"clear the cursor to re-sync from cursor_initial, which is the only safe option")
	}
	s.resumed = c.Value
	return nil
}

// Checkpoint reports the highest watermark emitted.
//
// Returning nil until a row has been seen is deliberate: an empty run must
// leave the stored cursor untouched, not overwrite it with "nothing".
func (s *syncSource) Checkpoint() []byte {
	if !s.started || len(s.high) == 0 {
		return nil
	}
	cur, err := json.Marshal(syncCursor{
		V: syncCursorVersion, Column: s.cfg.CursorColumn,
		Query: queryFingerprint(s.cfg.Query), Value: s.high,
	})
	if err != nil {
		return nil
	}
	return cur
}

// watermarkJSON renders a record value as the JSON form stored in the cursor.
//
// Only ordered scalars qualify. A watermark that cannot be compared is not a
// watermark, and silently accepting one would produce a cursor that never
// advances or advances wrongly.
func watermarkJSON(v record.Value) (json.RawMessage, error) {
	switch v.Kind() {
	case record.KindInt, record.KindFloat, record.KindString, record.KindBytes,
		record.KindDecimal, record.KindTimestamp, record.KindDate, record.KindTime:
		return json.Marshal(scalarOf(v))
	case record.KindNull:
		return nil, errors.New("value is NULL; a NULL watermark cannot order rows")
	default:
		return nil, fmt.Errorf("value is %v; a watermark must be a number, string or timestamp", v.Kind())
	}
}

func scalarOf(v record.Value) any {
	switch v.Kind() {
	case record.KindInt:
		return v.Int()
	case record.KindFloat:
		return v.Float()
	case record.KindDecimal:
		// json.Marshal of a float64 would round the cursor, and a rounded
		// cursor either re-reads rows or skips them. json.Number carries the
		// exact digits through Marshal as a bare number.
		return json.Number(v.DecimalText())
	case record.KindTimestamp, record.KindDate, record.KindTime:
		// RFC 3339 text, which is what cursorArg parses back — the same
		// spelling the row mapper used to emit directly.
		return v.Text()
	default:
		return v.String()
	}
}

// cursorArg converts the stored JSON watermark into a query argument.
//
// A timestamp arrives as an RFC 3339 string and is passed back as time.Time so
// the driver binds it as a timestamp rather than as text — comparing a
// timestamp column against a string is a type error in PostgreSQL, not a silent
// coercion.
//
// Native timestamps (ADR-0051) do not remove this step, which is worth stating
// because it looks like they should: the cursor is PERSISTED as JSON between
// runs, and JSON has no temporal type, so the value arrives here as a string
// however the row mapper produced it. What changed is only that the string is
// now written from a real instant rather than from a stringified one.
func cursorArg(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, errors.New("db: no cursor value; set cursor_initial for the first run")
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("db: malformed cursor value: %w", err)
	}
	if s, ok := v.(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t, nil
		}
	}
	return v, nil
}

// queryFingerprint identifies a query for cursor binding, ignoring whitespace
// so reformatting the SQL does not invalidate a perfectly good watermark.
func queryFingerprint(q string) string {
	return strings.Join(strings.Fields(q), " ")
}

// validateSync checks the sync config, including the two properties the
// correctness of a watermark read actually depends on.
func (c *config) validateSync() error {
	if strings.TrimSpace(c.Query) == "" {
		return errors.New("db: query is required")
	}
	if strings.TrimSpace(c.CursorColumn) == "" {
		return errors.New("db: cursor_column is required")
	}
	if len(c.Params) > 0 {
		return errors.New("db: sync binds $1 to the cursor itself; use literals in the query for anything else")
	}
	q := strings.ToLower(c.Query)
	if !strings.Contains(c.Query, "$1") {
		return errors.New("db: the query must reference the cursor as $1 " +
			`(e.g. "SELECT * FROM t WHERE updated_at >= $1 ORDER BY updated_at")`)
	}
	if strings.Contains(c.Query, "$2") {
		return errors.New("db: sync takes exactly one placeholder, $1, bound to the cursor")
	}
	// ORDER BY on the cursor column is not a style preference. Without it the
	// highest value SEEN is not the highest value DELIVERED, so an interrupted
	// run checkpoints past rows it never emitted and they are lost with no
	// error anywhere.
	if !strings.Contains(q, "order by") {
		return fmt.Errorf("db: the query must ORDER BY %s ascending, or an interrupted run "+
			"checkpoints past rows it never emitted", c.CursorColumn)
	}
	order := q[strings.LastIndex(q, "order by")+len("order by"):]
	if !strings.Contains(order, strings.ToLower(c.CursorColumn)) {
		return fmt.Errorf("db: the query orders by something other than the cursor column %q; "+
			"the watermark only advances correctly when rows arrive in cursor order", c.CursorColumn)
	}
	if strings.Contains(order, " desc") {
		return errors.New("db: the query orders descending; a watermark read must ascend")
	}
	return nil
}
