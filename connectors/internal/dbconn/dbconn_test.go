package dbconn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aaron-au/shift/engine/record"
)

// TestQueryRowMapping drives querySource against a go-sqlmock result set (no
// database) and asserts the column→typed-value mapping for every supported SQL
// type, including NULL, timestamp, and jsonb (parsed into a nested record).
func TestQueryRowMapping(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ts := time.Date(2026, 7, 25, 10, 30, 0, 0, time.UTC)
	cols := []*sqlmock.Column{
		sqlmock.NewColumn("id").OfType("INT8", int64(0)),
		sqlmock.NewColumn("name").OfType("TEXT", ""),
		sqlmock.NewColumn("score").OfType("FLOAT8", float64(0)),
		sqlmock.NewColumn("active").OfType("BOOL", false),
		sqlmock.NewColumn("created").OfType("TIMESTAMPTZ", time.Time{}),
		sqlmock.NewColumn("note").OfType("TEXT", ""),
		sqlmock.NewColumn("meta").OfType("JSONB", []byte("{}")),
		sqlmock.NewColumn("tags").OfType("JSONB", []byte("[]")),
	}
	rows := sqlmock.NewRowsWithColumnDefinition(cols...).
		AddRow(int64(7), "alice", 4.5, true, ts, nil, []byte(`{"k":"v","n":2}`), []byte(`[1,2,3]`))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	r, err := db.QueryContext(context.Background(), "SELECT id, name, score, active, created, note, meta, tags FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	s := &querySource{}
	if err := s.start(r); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("rows = %d, want 1", b.Len())
	}
	rec := b.Record(0)

	if v, _ := rec.Field("id"); v.Kind() != record.KindInt || v.Int() != 7 {
		t.Fatalf("id = %v", v)
	}
	if v, _ := rec.Field("name"); v.String() != "alice" {
		t.Fatalf("name = %q", v.String())
	}
	if v, _ := rec.Field("score"); v.Kind() != record.KindFloat || v.Float() != 4.5 {
		t.Fatalf("score = %v", v)
	}
	if v, _ := rec.Field("active"); v.Kind() != record.KindBool || !v.Bool() {
		t.Fatalf("active = %v", v)
	}
	// A timestamp column is a native instant now (ADR-0051), not the RFC 3339
	// string it used to be stringified into. The rendered text is unchanged —
	// still UTC — so no flow's output moves; only the kind does.
	if v, _ := rec.Field("created"); v.Kind() != record.KindTimestamp ||
		!strings.HasPrefix(v.Text(), "2026-07-25T10:30:00") {
		t.Fatalf("created = %v %q", v.Kind(), v.Text())
	}
	if v, _ := rec.Field("note"); v.Kind() != record.KindNull {
		t.Fatalf("note = %v, want null", v)
	}
	// jsonb object → nested map
	meta, _ := rec.Field("meta")
	if meta.Kind() != record.KindMap {
		t.Fatalf("meta kind = %v, want map", meta.Kind())
	}
	if kv, _ := meta.Field("k"); kv.String() != "v" {
		t.Fatalf("meta.k = %q", kv.String())
	}
	if nv, _ := meta.Field("n"); nv.Int() != 2 {
		t.Fatalf("meta.n = %v", nv)
	}
	// jsonb array → nested list
	tags, _ := rec.Field("tags")
	if tags.Kind() != record.KindList || tags.Len() != 3 || tags.Index(0).Int() != 1 {
		t.Fatalf("tags = %v", tags)
	}

	if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestQuoteIdent(t *testing.T) {
	ok := map[string]string{
		"users":        `"users"`,
		"public.users": `"public"."users"`,
		"_x1":          `"_x1"`,
		"col$name":     `"col$name"`,
	}
	for in, want := range ok {
		got, err := quoteIdent(in)
		if err != nil {
			t.Fatalf("quoteIdent(%q) error: %v", in, err)
		}
		if got != want {
			t.Fatalf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
	bad := []string{
		"",
		`users"; DROP TABLE x; --`,
		"a b",
		"1col",
		"tbl;",
		"a.b.c.", // trailing empty part
		"col-name",
	}
	for _, in := range bad {
		if _, err := quoteIdent(in); err == nil {
			t.Fatalf("quoteIdent(%q) accepted an unsafe identifier", in)
		}
	}
}

func TestUpsertBuildSQL(t *testing.T) {
	s := &upsertSink{}
	// Simulate Open's identifier setup.
	s.table = `"t"`
	s.conflictQuoted = []string{`"id"`}
	s.conflictSet = map[string]bool{"id": true}

	got, err := s.buildSQL([]string{"id", "name", "score"})
	if err != nil {
		t.Fatalf("buildSQL: %v", err)
	}
	want := `INSERT INTO "t" ("id", "name", "score") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "score" = EXCLUDED."score"`
	if got != want {
		t.Fatalf("buildSQL =\n  %s\nwant\n  %s", got, want)
	}

	// All columns are conflict columns → DO NOTHING.
	nothing, err := s.buildSQL([]string{"id"})
	if err != nil {
		t.Fatalf("buildSQL(nothing): %v", err)
	}
	if !strings.HasSuffix(nothing, "DO NOTHING") {
		t.Fatalf("expected DO NOTHING, got %q", nothing)
	}

	// An unsafe column name is rejected (no injection into SQL).
	if _, err := s.buildSQL([]string{`x") ; DROP TABLE t; --`}); err == nil {
		t.Fatal("buildSQL accepted an unsafe column name")
	}
}

func TestValueToArg(t *testing.T) {
	if got := valueToArg(record.Null()); got != nil {
		t.Fatalf("null → %v", got)
	}
	if got := valueToArg(record.Int(5)); got != int64(5) {
		t.Fatalf("int → %v", got)
	}
	if got := valueToArg(record.Bool(true)); got != true {
		t.Fatalf("bool → %v", got)
	}

	// A nested map/list argument is encoded as JSON text (for a json/jsonb column).
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.Int(1)
	bld.KeyLiteral("b")
	bld.BeginList()
	bld.Bool(true)
	bld.StringLiteral("x\"y")
	bld.EndList()
	bld.EndMap()
	v := bld.Finish()

	got, ok := valueToArg(v).(string)
	if !ok {
		t.Fatalf("container arg is %T, want string", valueToArg(v))
	}
	want := `{"a":1,"b":[true,"x\"y"]}`
	if got != want {
		t.Fatalf("json arg = %s, want %s", got, want)
	}
}

func TestDiscoverSchema(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"table_schema", "table_name", "column_name", "data_type", "is_nullable"}).
		AddRow("public", "users", "id", "bigint", "NO").
		AddRow("public", "users", "email", "text", "YES").
		AddRow("public", "orders", "id", "bigint", "NO")
	mock.ExpectQuery("information_schema").WillReturnRows(rows)

	tables, err := discoverSchema(context.Background(), db)
	if err != nil {
		t.Fatalf("discoverSchema: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(tables))
	}
	users := tables[0]
	if users.Name != "users" || len(users.Columns) != 2 {
		t.Fatalf("users = %+v", users)
	}
	if users.Columns[0].Name != "id" || users.Columns[0].Nullable {
		t.Fatalf("users.id column = %+v, want non-nullable", users.Columns[0])
	}
	if !users.Columns[1].Nullable {
		t.Fatalf("users.email should be nullable")
	}
	if tables[1].Name != "orders" || len(tables[1].Columns) != 1 {
		t.Fatalf("orders = %+v", tables[1])
	}
}

func TestConfigDSNAndValidation(t *testing.T) {
	// Discrete fields → assembled DSN with escaped credentials.
	c := config{Host: "db.internal", Port: 0, Database: "app", User: "u", Password: "p@ss word", SSLMode: "require"}
	if err := c.validateConn(); err != nil {
		t.Fatalf("validateConn: %v", err)
	}
	if c.Port != 5432 {
		t.Fatalf("default port = %d, want 5432", c.Port)
	}
	dsn, err := c.dsn()
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	for _, want := range []string{"postgres://", "db.internal:5432", "/app", "sslmode=require", "p%40ss%20word"} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("dsn %q missing %q", dsn, want)
		}
	}

	// A verbatim DSN passes through untouched.
	c2 := config{DSN: "postgres://x/y"}
	if err := c2.validateConn(); err != nil {
		t.Fatalf("validateConn(dsn): %v", err)
	}
	if got, _ := c2.dsn(); got != "postgres://x/y" {
		t.Fatalf("verbatim dsn = %q", got)
	}

	// Neither dsn nor host+database → error.
	empty := config{}
	if err := empty.validateConn(); err == nil {
		t.Fatal("expected validateConn error for empty config")
	}
}

func TestNetworkGuard(t *testing.T) {
	// Fail closed: loopback and private targets refused unless allow_local.
	for _, addr := range []string{"127.0.0.1:5432", "10.1.2.3:5432", "100.64.0.1:5432"} {
		if err := guard(false)("tcp", addr, nil); err == nil {
			t.Fatalf("guard(false) allowed internal target %s", addr)
		}
		if err := guard(true)("tcp", addr, nil); err != nil {
			t.Fatalf("guard(true) refused %s: %v", addr, err)
		}
	}
	// A public target is always allowed.
	if err := guard(false)("tcp", "8.8.8.8:5432", nil); err != nil {
		t.Fatalf("guard refused public target: %v", err)
	}
}

// TestIntegrationPostgres exercises the real driver end-to-end. It is skipped
// unless SHIFT_TEST_PG is set to a reachable DSN (the platform's existing
// convention), so the default `go test` run needs no database or network.
func TestIntegrationPostgres(t *testing.T) {
	dsn := os.Getenv("SHIFT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set SHIFT_TEST_PG_DSN=<postgres dsn> to run the live integration test")
	}
	ctx := context.Background()
	cfg := config{DSN: dsn, AllowLocal: true}
	db, closer, err := cfg.open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = closer() }()

	const tbl = "shift_dbconn_it"
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE `+tbl+` (id bigint primary key, name text, meta jsonb)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// upsert two records, then upsert one again (idempotent update).
	sink := &upsertSink{}
	scfg := mustJSON(t, map[string]any{"dsn": dsn, "allow_local": true, "table": tbl, "conflict_columns": []string{"id"}})
	if err := sink.Open(ctx, scfg); err != nil {
		t.Fatalf("sink open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	for _, r := range []struct {
		id   int64
		name string
	}{{1, "a"}, {2, "b"}, {1, "a2"}} {
		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(r.id)
		bld.KeyLiteral("name")
		bld.StringLiteral(r.name)
		bld.EndMap()
		batch.Append(bld.Finish())
	}
	if err := sink.Write(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink close: %v", err)
	}

	// query back.
	src := &querySource{}
	qcfg := mustJSON(t, map[string]any{"dsn": dsn, "allow_local": true, "query": "SELECT id, name FROM " + tbl + " ORDER BY id"})
	if err := src.Open(ctx, qcfg); err != nil {
		t.Fatalf("source open: %v", err)
	}
	defer func() { _ = src.Close() }()
	got := map[int64]string{}
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			idv, _ := rec.Field("id")
			nv, _ := rec.Field("name")
			got[idv.Int()] = nv.String()
		}
	}
	if got[1] != "a2" || got[2] != "b" || len(got) != 2 {
		t.Fatalf("query result = %v, want {1:a2, 2:b}", got)
	}

	// discoverSchema sees the temp table.
	tables, err := discoverSchema(ctx, db)
	if err != nil {
		t.Fatalf("discoverSchema: %v", err)
	}
	_ = tables // presence depends on temp-schema visibility; the call must succeed
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
