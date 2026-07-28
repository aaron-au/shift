package dbconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// execSource runs one non-returning statement (INSERT/UPDATE/DELETE/DDL) and
// emits a single status record {statement, rows_affected, ok}. It is a
// config-driven source so a one-verb flow is runnable on its own (ADR-0024).
//
// At-least-once caveat: a bare exec is NOT inherently idempotent (a redispatched
// DELETE/UPDATE re-runs). For idempotent writes prefer the upsert sink, or make
// the statement idempotent (e.g. guarded by a WHERE / ON CONFLICT clause).
type execSource struct {
	cfg   config
	done  bool
	batch *record.Batch
}

func (s *execSource) Open(_ context.Context, cfgBytes []byte) error {
	if err := json.Unmarshal(cfgBytes, &s.cfg); err != nil {
		return fmt.Errorf("db: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if strings.TrimSpace(s.statement()) == "" {
		return errors.New("db: statement is required")
	}
	return nil
}

// statement returns the exec SQL, accepting either "statement" or "query".
func (s *execSource) statement() string {
	if s.cfg.Statement != "" {
		return s.cfg.Statement
	}
	return s.cfg.Query
}

func (s *execSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	db, closer, err := openDB(ctx, &s.cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer() }()

	res, err := db.ExecContext(ctx, s.statement(), s.cfg.Params...)
	if err != nil {
		return nil, fmt.Errorf("db: exec: %w", err)
	}
	// RowsAffected is unsupported for some statements (e.g. DDL); treat an error
	// as "not reported" (0) rather than failing the op.
	affected, _ := res.RowsAffected()

	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("rows_affected")
	bld.Int(affected)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *execSource) Close() error { return nil }
