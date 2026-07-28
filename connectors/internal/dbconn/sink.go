package dbconn

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// upsertSink writes record batches to a table via
// INSERT ... ON CONFLICT (conflict_columns) DO UPDATE. This is naturally
// idempotent under at-least-once redelivery (ADR-0002): re-writing the same
// keyed record converges to the same row, so a re-dispatched task is safe.
//
// The column list is either fixed (config.columns) or inferred per record from
// its map keys. Value data is always bound as positional parameters; only the
// table and column identifiers are interpolated, each validated + quoted by
// quoteIdent so a name can never carry an injection.
type upsertSink struct {
	cfg            config
	db             *sql.DB
	closer         func() error
	table          string          // pre-quoted
	conflictQuoted []string        // pre-quoted conflict columns
	conflictSet    map[string]bool // raw conflict column names
	stmts          map[string]*sql.Stmt
}

func (s *upsertSink) Open(ctx context.Context, cfgBytes []byte) error {
	if err := json.Unmarshal(cfgBytes, &s.cfg); err != nil {
		return fmt.Errorf("db: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if s.cfg.Table == "" {
		return errors.New("db: upsert: table is required")
	}
	if len(s.cfg.ConflictColumns) == 0 {
		return errors.New("db: upsert: conflict_columns is required")
	}
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return err
	}
	s.table = tbl
	s.conflictSet = make(map[string]bool, len(s.cfg.ConflictColumns))
	s.conflictQuoted = make([]string, len(s.cfg.ConflictColumns))
	for i, c := range s.cfg.ConflictColumns {
		q, err := quoteIdent(c)
		if err != nil {
			return err
		}
		s.conflictQuoted[i] = q
		s.conflictSet[c] = true
	}
	// Fail closed early if fixed columns are named but invalid.
	for _, c := range s.cfg.Columns {
		if _, err := quoteIdent(c); err != nil {
			return err
		}
	}
	s.stmts = make(map[string]*sql.Stmt)

	db, closer, err := openDB(ctx, &s.cfg)
	if err != nil {
		return err
	}
	s.db, s.closer = db, closer
	return nil
}

func (s *upsertSink) Write(ctx context.Context, b *record.Batch) error {
	for _, rec := range b.Records() {
		if rec.Kind() != record.KindMap {
			return errors.New("db: upsert expects map records")
		}
		cols := s.cfg.Columns
		if len(cols) == 0 {
			cols = recordKeys(rec)
		}
		if len(cols) == 0 {
			continue // nothing to write for an empty record
		}
		stmt, err := s.stmtFor(ctx, cols) //nolint:sqlclosecheck // prepared stmts are cached in s.stmts and closed in Close()
		if err != nil {
			return err
		}
		args := make([]any, len(cols))
		for i, c := range cols {
			fv, _ := rec.Field(c) // absent field → null Value → NULL arg
			args[i] = valueToArg(fv)
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("db: upsert into %s: %w", s.cfg.Table, err)
		}
	}
	return nil
}

// stmtFor returns a prepared upsert statement for the given column set, caching
// by column signature so a batch with a stable shape prepares once.
func (s *upsertSink) stmtFor(ctx context.Context, cols []string) (*sql.Stmt, error) {
	sig := strings.Join(cols, "\x00")
	if st, ok := s.stmts[sig]; ok {
		return st, nil
	}
	query, err := s.buildSQL(cols)
	if err != nil {
		return nil, err
	}
	st, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("db: prepare upsert: %w", err)
	}
	s.stmts[sig] = st
	return st, nil
}

// buildSQL assembles the parameterized upsert. Every value is a $n placeholder;
// only validated+quoted identifiers are interpolated. When all written columns
// are conflict columns there is nothing to update, so it degrades to
// DO NOTHING.
func (s *upsertSink) buildSQL(cols []string) (string, error) {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		q, err := quoteIdent(c)
		if err != nil {
			return "", err
		}
		quoted[i] = q
	}
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(s.table)
	b.WriteString(" (")
	b.WriteString(strings.Join(quoted, ", "))
	b.WriteString(") VALUES (")
	for i := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(i + 1))
	}
	b.WriteString(") ON CONFLICT (")
	b.WriteString(strings.Join(s.conflictQuoted, ", "))
	b.WriteString(") DO ")

	var sets []string
	for i, c := range cols {
		if s.conflictSet[c] {
			continue
		}
		sets = append(sets, quoted[i]+" = EXCLUDED."+quoted[i])
	}
	if len(sets) == 0 {
		b.WriteString("NOTHING")
	} else {
		b.WriteString("UPDATE SET ")
		b.WriteString(strings.Join(sets, ", "))
	}
	return b.String(), nil
}

// Close closes cached statements and the pool.
func (s *upsertSink) Close() error {
	var errs []error
	for _, st := range s.stmts {
		if err := st.Close(); err != nil {
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
