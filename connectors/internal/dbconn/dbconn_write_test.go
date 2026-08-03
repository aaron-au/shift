package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aaron-au/shift/engine/record"
)

// withMockDB redirects the openDB seam at a sqlmock-backed pool so the
// query/exec/upsert paths run without a live database. Not parallel-safe (it
// mutates the package var), matching the non-parallel style of these tests.
func withMockDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := openDB
	openDB = func(_ context.Context, _ *config) (*sql.DB, func() error, error) { return db, db.Close, nil }
	t.Cleanup(func() {
		openDB = prev
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})
	return mock
}

// mapBatch builds a one-record batch {k:v,...} preserving key order.
func mapBatch(pairs ...any) *record.Batch {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	for i := 0; i+1 < len(pairs); i += 2 {
		bld.KeyLiteral(pairs[i].(string))
		switch v := pairs[i+1].(type) {
		case int:
			bld.Int(int64(v))
		case string:
			bld.StringLiteral(v)
		case bool:
			bld.Bool(v)
		case nil:
			bld.Null()
		}
	}
	bld.EndMap()
	b.Append(bld.Finish())
	return b
}

const connCfg = `"host":"db.example.com","database":"d","user":"u","password":"p","sslmode":"disable"`

func TestUpsertWriteInferredColumns(t *testing.T) {
	mock := withMockDB(t)
	s := &upsertSink{}
	cfg := `{` + connCfg + `,"table":"users","conflict_columns":["id"]}`
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	// id is the conflict column → not in the UPDATE SET; name is.
	mock.ExpectPrepare(`INSERT INTO "users"`).
		ExpectExec().WithArgs(int64(1), "alice").WillReturnResult(sqlmock.NewResult(0, 1))
	// Second record with the same column shape reuses the prepared stmt.
	mock.ExpectExec(`INSERT INTO "users"`).WithArgs(int64(2), "bob").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.Write(context.Background(), mapBatch("id", 1, "name", "alice")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := s.Write(context.Background(), mapBatch("id", 2, "name", "bob")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if len(s.stmts) != 1 {
		t.Errorf("expected 1 cached stmt (stable shape), got %d", len(s.stmts))
	}
}

func TestUpsertWriteAllConflictColumnsDegradesToDoNothing(t *testing.T) {
	mock := withMockDB(t)
	s := &upsertSink{}
	cfg := `{` + connCfg + `,"table":"t","conflict_columns":["id"],"columns":["id"]}`
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	// Only column is the conflict key → DO NOTHING.
	mock.ExpectPrepare(`ON CONFLICT .* DO NOTHING`).
		ExpectExec().WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := s.Write(context.Background(), mapBatch("id", 7)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestUpsertWriteExecError(t *testing.T) {
	mock := withMockDB(t)
	s := &upsertSink{}
	if err := s.Open(context.Background(), []byte(`{`+connCfg+`,"table":"t","conflict_columns":["id"]}`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	mock.ExpectPrepare(`INSERT INTO`).ExpectExec().WillReturnError(errors.New("boom"))
	if err := s.Write(context.Background(), mapBatch("id", 1)); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestUpsertWriteRejectsNonMapRecord(t *testing.T) {
	withMockDB(t)
	s := &upsertSink{}
	if err := s.Open(context.Background(), []byte(`{`+connCfg+`,"table":"t","conflict_columns":["id"]}`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	b := record.NewBatch()
	bld := b.Builder()
	bld.Int(5) // a scalar record, not a map
	b.Append(bld.Finish())
	if err := s.Write(context.Background(), b); err == nil {
		t.Fatal("expected non-map rejection")
	}
}

func TestExecSourceReportsRowsAffected(t *testing.T) {
	mock := withMockDB(t)
	s := &execSource{}
	cfg := `{` + connCfg + `,"statement":"DELETE FROM t WHERE age < $1","params":[18]}`
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	mock.ExpectExec(`DELETE FROM t`).WithArgs(float64(18)).WillReturnResult(sqlmock.NewResult(0, 4))

	batch, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	rec := batch.Record(0)
	got, _ := rec.Field("rows_affected")
	if got.Int() != 4 {
		t.Errorf("rows_affected = %d, want 4", got.Int())
	}
	ok, _ := rec.Field("ok")
	if !ok.Bool() {
		t.Error("ok should be true")
	}
	// Second Next is EOF.
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Errorf("second Next = %v, want EOF", err)
	}
}

func TestExecSourceError(t *testing.T) {
	mock := withMockDB(t)
	s := &execSource{}
	if err := s.Open(context.Background(), []byte(`{`+connCfg+`,"statement":"UPDATE t SET x=1"}`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	mock.ExpectExec(`UPDATE t`).WillReturnError(errors.New("denied"))
	if _, err := s.Next(context.Background()); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestQuerySourceOpenToNext(t *testing.T) {
	mock := withMockDB(t)
	s := &querySource{}
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a").AddRow(int64(2), "b")
	mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

	cfg := `{` + connCfg + `,"query":"SELECT id, name FROM t"}`
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	batch, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if batch.Len() != 2 {
		t.Fatalf("rows = %d, want 2", batch.Len())
	}
	id, _ := batch.Record(0).Field("id")
	if id.Int() != 1 {
		t.Errorf("row0 id = %d", id.Int())
	}
}
