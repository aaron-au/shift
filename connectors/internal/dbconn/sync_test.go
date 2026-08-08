package dbconn

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/aaron-au/shift/engine/record"
)

// syncOn builds a sync source over a mocked result set.
func syncOn(t *testing.T, cfg config, rows *sqlmock.Rows, query string) (*syncSource, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	r, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	s := &syncSource{cfg: cfg}
	if err := s.start(r); err != nil {
		_ = db.Close()
		t.Fatalf("start: %v", err)
	}
	return s, func() { _ = s.Close(); _ = db.Close() }
}

// The point of a watermark: the NEXT run starts where this one finished. A
// cursor that did not advance would re-deliver the same rows every fire, which
// is the failure a scheduled sync exists to avoid.
func TestTheCursorAdvancesToTheHighestRowDelivered(t *testing.T) {
	cols := []*sqlmock.Column{
		sqlmock.NewColumn("id").OfType("INT8", int64(0)),
		sqlmock.NewColumn("updated_at").OfType("INT8", int64(0)),
	}
	rows := sqlmock.NewRowsWithColumnDefinition(cols...).
		AddRow(int64(1), int64(100)).
		AddRow(int64(2), int64(200)).
		AddRow(int64(3), int64(300))

	cfg := config{CursorColumn: "updated_at", Query: "SELECT id, updated_at FROM t WHERE updated_at >= $1 ORDER BY updated_at"}
	s, done := syncOn(t, cfg, rows, cfg.Query)
	defer done()

	if cur := s.Checkpoint(); cur != nil {
		t.Fatal("a run that has emitted nothing reported a cursor; it would overwrite a good one with an empty result")
	}

	b, err := s.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 3 {
		t.Fatalf("rows = %d, want 3", b.Len())
	}

	var c syncCursor
	if err := json.Unmarshal(s.Checkpoint(), &c); err != nil {
		t.Fatal(err)
	}
	if string(c.Value) != "300" {
		t.Errorf("cursor = %s, want the highest delivered watermark 300", c.Value)
	}
	if c.Column != "updated_at" {
		t.Errorf("cursor column = %q", c.Column)
	}
}

// An empty run must leave the stored cursor ALONE. Reporting "nothing" would
// reset the sync to its initial position and re-deliver the whole table.
func TestAnEmptyRunReportsNoCursor(t *testing.T) {
	cols := []*sqlmock.Column{sqlmock.NewColumn("updated_at").OfType("INT8", int64(0))}
	cfg := config{CursorColumn: "updated_at", Query: "SELECT updated_at FROM t WHERE updated_at >= $1 ORDER BY updated_at"}
	s, done := syncOn(t, cfg, sqlmock.NewRowsWithColumnDefinition(cols...), cfg.Query)
	defer done()

	if _, err := s.Next(t.Context()); err == nil {
		t.Fatal("expected io.EOF on an empty result")
	}
	if cur := s.Checkpoint(); cur != nil {
		t.Fatalf("an empty run reported a cursor (%s); the stored one would be overwritten", cur)
	}
}

// A query the cursor column is not selected by cannot produce a watermark. It
// must fail at open, not stream rows and checkpoint nothing — that combination
// re-delivers the same rows on every run, forever, with no error.
func TestAQueryMustSelectItsCursorColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	cols := []*sqlmock.Column{sqlmock.NewColumn("id").OfType("INT8", int64(0))}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(cols...))
	r, err := db.QueryContext(t.Context(), "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	s := &syncSource{cfg: config{CursorColumn: "updated_at"}}
	if err := s.start(r); err == nil {
		t.Fatal("a query that does not select the cursor column was accepted")
	} else if !strings.Contains(err.Error(), "updated_at") {
		t.Errorf("the error does not name the missing column: %v", err)
	}
}

// Resume binds the previous watermark; a cursor for a different column or a
// different query must be refused rather than applied to the wrong result set.
func TestResumeRefusesACursorThatDoesNotBelong(t *testing.T) {
	const q = "SELECT id, updated_at FROM t WHERE updated_at >= $1 ORDER BY updated_at"
	cfg := config{CursorColumn: "updated_at", Query: q}

	good, _ := json.Marshal(syncCursor{V: syncCursorVersion, Column: "updated_at", Query: queryFingerprint(q), Value: json.RawMessage("42")})
	s := &syncSource{cfg: cfg}
	if err := s.Resume(t.Context(), good); err != nil {
		t.Fatalf("a matching cursor was refused: %v", err)
	}
	if string(s.resumed) != "42" {
		t.Errorf("resumed = %s, want 42", s.resumed)
	}

	// Whitespace-only reformatting of the SQL must NOT invalidate a cursor —
	// that would silently re-sync the whole table after a cosmetic edit.
	reformatted, _ := json.Marshal(syncCursor{V: syncCursorVersion, Column: "updated_at",
		Query: queryFingerprint("SELECT id, updated_at\n  FROM t\n  WHERE updated_at >= $1\n  ORDER BY updated_at"),
		Value: json.RawMessage("42")})
	if err := (&syncSource{cfg: cfg}).Resume(t.Context(), reformatted); err != nil {
		t.Errorf("reformatting the query invalidated its cursor: %v", err)
	}

	cases := map[string]syncCursor{
		"a different column": {V: syncCursorVersion, Column: "created_at", Query: queryFingerprint(q), Value: json.RawMessage("42")},
		"a different query":  {V: syncCursorVersion, Column: "updated_at", Query: "SELECT 1", Value: json.RawMessage("42")},
		"a future version":   {V: 99, Column: "updated_at", Query: queryFingerprint(q), Value: json.RawMessage("42")},
	}
	for name, c := range cases {
		raw, _ := json.Marshal(c)
		if err := (&syncSource{cfg: cfg}).Resume(t.Context(), raw); err == nil {
			t.Errorf("%s: the cursor was accepted", name)
		}
	}

	// No cursor at all is a first run, not an error.
	if err := (&syncSource{cfg: cfg}).Resume(t.Context(), nil); err != nil {
		t.Errorf("an absent cursor was treated as an error: %v", err)
	}
}

// The ordering rule is the one that silently loses data if unenforced: without
// ORDER BY on the cursor column ascending, the highest value SEEN is not the
// highest value DELIVERED, so an interrupted run checkpoints past rows it
// never emitted.
func TestSyncRefusesAQueryThatCannotCheckpointSafely(t *testing.T) {
	base := config{CursorColumn: "updated_at"}
	cases := map[string]struct {
		query  string
		params []any
		want   string
	}{
		"no cursor placeholder": {query: "SELECT * FROM t ORDER BY updated_at", want: "$1"},
		"no ordering":           {query: "SELECT * FROM t WHERE updated_at >= $1", want: "ORDER BY"},
		"ordered by something else": {
			query: "SELECT * FROM t WHERE updated_at >= $1 ORDER BY id", want: "cursor column"},
		"ordered descending": {
			query: "SELECT * FROM t WHERE updated_at >= $1 ORDER BY updated_at DESC", want: "descending"},
		"extra placeholder": {
			query: "SELECT * FROM t WHERE updated_at >= $1 AND x = $2 ORDER BY updated_at", want: "exactly one"},
		"extra params": {
			query:  "SELECT * FROM t WHERE updated_at >= $1 ORDER BY updated_at",
			params: []any{1}, want: "cursor itself"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := base
			c.Query, c.Params = tc.query, tc.params
			err := c.validateSync()
			if err == nil {
				t.Fatalf("accepted a query that cannot checkpoint safely: %s", tc.query)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not explain %q", err, tc.want)
			}
		})
	}

	ok := base
	ok.Query = "SELECT id, updated_at FROM t WHERE updated_at >= $1 ORDER BY updated_at ASC"
	if err := ok.validateSync(); err != nil {
		t.Errorf("a correct query was refused: %v", err)
	}
}

// A timestamp cursor has to go back to the driver as a TIME, not as the string
// it was stored as: PostgreSQL compares a timestamp column against text as a
// type error, not a coercion.
func TestATimestampCursorBindsAsATimestamp(t *testing.T) {
	arg, err := cursorArg(json.RawMessage(`"2026-07-25T10:30:00Z"`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := arg.(time.Time); !ok {
		t.Fatalf("bound %T, want time.Time — a timestamp column cannot be compared against text", arg)
	}

	num, err := cursorArg(json.RawMessage(`42`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := num.(float64); !ok {
		t.Fatalf("bound %T, want a number", num)
	}

	// A plain string stays a string.
	s, err := cursorArg(json.RawMessage(`"abc"`))
	if err != nil {
		t.Fatal(err)
	}
	if s != "abc" {
		t.Fatalf("bound %v, want abc", s)
	}

	if _, err := cursorArg(nil); err == nil {
		t.Error("an absent cursor value was accepted; the first run needs cursor_initial")
	}
}

// A NULL or container watermark cannot order rows. Accepting one yields a
// cursor that never advances or advances wrongly.
func TestAWatermarkMustBeAnOrderedScalar(t *testing.T) {
	cols := []*sqlmock.Column{
		sqlmock.NewColumn("id").OfType("INT8", int64(0)),
		sqlmock.NewColumn("updated_at").OfType("INT8", int64(0)),
	}
	rows := sqlmock.NewRowsWithColumnDefinition(cols...).AddRow(int64(1), nil)
	cfg := config{CursorColumn: "updated_at", Query: "SELECT id, updated_at FROM t WHERE updated_at >= $1 ORDER BY updated_at"}
	s, done := syncOn(t, cfg, rows, cfg.Query)
	defer done()

	if _, err := s.Next(context.Background()); err == nil {
		t.Fatal("a NULL watermark was accepted")
	}
}

// Open must reject a bad config BEFORE it dials. Validating after connecting
// would mean a misconfigured node opens a database session, and on a
// scheduled flow that is a connection attempt per fire against a server that
// can do nothing useful with it.
func TestOpenValidatesBeforeItDials(t *testing.T) {
	cases := map[string]string{
		"not json":         `{`,
		"no connection":    `{"query":"SELECT a FROM t WHERE a >= $1 ORDER BY a","cursor_column":"a"}`,
		"no cursor column": `{"dsn":"postgres://u@h/db","query":"SELECT a FROM t WHERE a >= $1 ORDER BY a"}`,
		"unsafe query":     `{"dsn":"postgres://u@h/db","query":"SELECT a FROM t","cursor_column":"a"}`,
		"no initial cursor": `{"dsn":"postgres://u@h/db","cursor_column":"a",` +
			`"query":"SELECT a FROM t WHERE a >= $1 ORDER BY a"}`,
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			s := &syncSource{}
			if err := s.Open(t.Context(), []byte(cfg)); err == nil {
				t.Fatal("opened with a config that cannot work")
			}
		})
	}
}

// Close must be safe on a source that never opened — the runner closes
// whatever it built, including a source whose Open failed.
func TestClosingAnUnopenedSyncIsSafe(t *testing.T) {
	if err := (&syncSource{}).Close(); err != nil {
		t.Fatalf("closing an unopened source: %v", err)
	}
}

// Every ordered scalar kind must render into the cursor. A float or string
// watermark that silently failed would leave the cursor stuck at its initial
// value and re-deliver the whole table on every run.
func TestWatermarksOfEveryOrderedKind(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(7)
	bld.KeyLiteral("f")
	bld.Float(1.5)
	bld.KeyLiteral("s")
	bld.StringLiteral("2026-08-01T00:00:00Z")
	bld.KeyLiteral("b")
	bld.Bool(true)
	bld.EndMap()
	b.Append(bld.Finish())
	rec := b.Record(0)

	for _, tc := range []struct{ field, want string }{
		{"i", "7"}, {"f", "1.5"}, {"s", `"2026-08-01T00:00:00Z"`},
	} {
		v, _ := rec.Field(tc.field)
		got, err := watermarkJSON(v)
		if err != nil {
			t.Errorf("%s: %v", tc.field, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s rendered as %s, want %s", tc.field, got, tc.want)
		}
	}

	// A boolean cannot order rows.
	v, _ := rec.Field("b")
	if _, err := watermarkJSON(v); err == nil {
		t.Error("a boolean was accepted as a watermark")
	}
}
