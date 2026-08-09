package dbconn

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// queryBatch caps how many rows one Next emits before yielding a batch.
const queryBatch = 256

// querySource runs a SELECT and streams each row as a typed record. Rows are
// pulled from the driver cursor incrementally — the result set is never
// buffered whole (doctrine: no whole-payload buffering). Column name → typed
// value, with NULL/int/float/bool/text/timestamp/json handled by appendValue.
type querySource struct {
	cfg     config
	closer  func() error
	rows    *sql.Rows
	cols    []string
	colKeys [][]byte // stable per-connector key bytes (KeyNoCopy)
	colHint []colHint
	scan    []any // per-row scan destinations (reused)
	holders []any // pointers into scan (reused)
	batch   *record.Batch
	done    bool
}

func (s *querySource) Open(ctx context.Context, cfgBytes []byte) error {
	if err := json.Unmarshal(cfgBytes, &s.cfg); err != nil {
		return fmt.Errorf("db: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if strings.TrimSpace(s.cfg.Query) == "" {
		return errors.New("db: query is required")
	}
	db, closer, err := openDB(ctx, &s.cfg)
	if err != nil {
		return err
	}
	s.closer = closer
	rows, err := db.QueryContext(ctx, s.cfg.Query, s.cfg.Params...) //nolint:sqlclosecheck // streaming cursor held across Next calls; closed in Close()
	if err != nil {
		_ = closer()
		return fmt.Errorf("db: query: %w", err)
	}
	if err := s.start(rows); err != nil {
		_ = rows.Close() //nolint:sqlclosecheck // error path; cursor otherwise held across Next and closed in Close()
		_ = closer()
		return err
	}
	return nil
}

// start wires the source to an open cursor: column names, per-column type
// hints, and reusable scan buffers. Split out from Open so the row→record
// mapping is testable against any *sql.Rows (e.g. go-sqlmock) without a live
// database.
func (s *querySource) start(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("db: columns: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("db: column types: %w", err)
	}
	s.rows = rows
	s.cols = cols
	s.colKeys = make([][]byte, len(cols))
	s.colHint = make([]colHint, len(cols))
	s.scan = make([]any, len(cols))
	s.holders = make([]any, len(cols))
	for i, c := range cols {
		s.colKeys[i] = []byte(c)
		s.holders[i] = &s.scan[i]
		if i < len(colTypes) {
			s.colHint[i] = hintFor(colTypes[i].DatabaseTypeName())
		}
	}
	s.batch = record.NewBatch()
	return nil
}

func (s *querySource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.batch.Reset()
	for s.batch.Len() < queryBatch {
		if !s.rows.Next() {
			s.done = true
			if err := s.rows.Err(); err != nil {
				return nil, fmt.Errorf("db: read: %w", err)
			}
			break
		}
		if err := s.rows.Scan(s.holders...); err != nil {
			return nil, fmt.Errorf("db: scan: %w", err)
		}
		s.mapRow(ctx)
	}
	if s.batch.Len() == 0 {
		return nil, io.EOF
	}
	return s.batch, nil
}

// mapRow builds one record ({column: value}) from the current scan buffer into
// the batch. Keys use KeyNoCopy since colKeys are stable for the source's whole
// lifetime (they survive batch Reset — they live outside the arena).
func (s *querySource) mapRow(ctx context.Context) {
	bld := s.batch.Builder()
	bld.BeginMap()
	for i := range s.cols {
		bld.KeyNoCopy(s.colKeys[i])
		appendValue(ctx, s.batch, bld, s.scan[i], s.colHint[i])
	}
	bld.EndMap()
	s.batch.Append(bld.Finish())
}

func (s *querySource) Close() error {
	var errs []error
	if s.rows != nil {
		if err := s.rows.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.closer != nil {
		if err := s.closer(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
