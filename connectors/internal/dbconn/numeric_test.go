package dbconn

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/aaron-au/shift/engine/record"
)

// TestANumericColumnBecomesAnExactDecimal is the database half of ADR-0051:
// a NUMERIC(12,2) is a declared type, so it opts the column out of float64
// without the flow author having to add a coerce step. The driver hands the
// cell over as text, and the column type is the only thing that says what the
// text means.
func TestANumericColumnBecomesAnExactDecimal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	cols := []*sqlmock.Column{
		sqlmock.NewColumn("amount").OfType("NUMERIC", ""),
		sqlmock.NewColumn("price").OfType("DECIMAL(12,2)", ""),
		sqlmock.NewColumn("owed").OfType("MONEY", ""),
		sqlmock.NewColumn("ratio").OfType("FLOAT8", float64(0)),
		sqlmock.NewColumn("weird").OfType("NUMERIC", ""),
	}
	rows := sqlmock.NewRowsWithColumnDefinition(cols...).
		AddRow("10.10", "1234.50", "-0.05", 4.5, "NaN")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	r, err := db.QueryContext(context.Background(), "SELECT amount, price, owed, ratio, weird FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	s := &querySource{}
	if err := s.start(r); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close() }()

	b, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	rec := b.Record(0)

	exact := []struct {
		field string
		text  string
	}{
		{"amount", "10.10"}, // the trailing zero the column declared
		{"price", "1234.50"},
		{"owed", "-0.05"},
	}
	for _, c := range exact {
		v, _ := rec.Field(c.field)
		if v.Kind() != record.KindDecimal || v.Text() != c.text {
			t.Errorf("%s = %v %q, want decimal %q", c.field, v.Kind(), v.Text(), c.text)
		}
	}
	// A float column is untouched: only a declared exact type opts in.
	if v, _ := rec.Field("ratio"); v.Kind() != record.KindFloat {
		t.Errorf("ratio = %v, want float", v.Kind())
	}
	// PostgreSQL NUMERIC also admits 'NaN', which has no decimal form. Keeping
	// the text beats dropping the value or failing the row.
	if v, _ := rec.Field("weird"); v.Kind() != record.KindString || v.String() != "NaN" {
		t.Errorf("weird = %v %q, want the string NaN", v.Kind(), v.String())
	}
}

func TestHintForRecognisesDialectSpellings(t *testing.T) {
	decimal := []string{"NUMERIC", "numeric", "NUMERIC(12,2)", "DECIMAL", "decimal(5,0)", "MONEY"}
	for _, s := range decimal {
		if got := hintFor(s); got != hintDecimal {
			t.Errorf("hintFor(%q) = %v, want hintDecimal", s, got)
		}
	}
	for _, s := range []string{"JSON", "JSONB", "jsonb"} {
		if got := hintFor(s); got != hintJSON {
			t.Errorf("hintFor(%q) = %v, want hintJSON", s, got)
		}
	}
	for _, s := range []string{"TEXT", "INT8", "TIMESTAMPTZ", "BOOL", ""} {
		if got := hintFor(s); got != hintNone {
			t.Errorf("hintFor(%q) = %v, want hintNone", s, got)
		}
	}
}

// TestExactValuesBindAsQueryArguments: a decimal must reach the database as
// exact text, not as a float64 that rounds on the way in and defeats the point
// of having read it exactly.
func TestExactValuesBindAsQueryArguments(t *testing.T) {
	if got := valueToArg(record.Decimal(1010, 2)); got != "10.10" {
		t.Errorf("decimal arg = %#v, want the exact text 10.10", got)
	}
	ts := time.Date(2026, 8, 8, 9, 30, 0, 0, time.UTC)
	got := valueToArg(record.TimestampAt(ts))
	bound, ok := got.(time.Time)
	if !ok {
		t.Fatalf("timestamp arg = %#v, want a time.Time so the driver binds a timestamp", got)
	}
	if !bound.Equal(ts) {
		t.Errorf("timestamp arg = %v, want %v", bound, ts)
	}
	if _, ok := valueToArg(record.Date(20673)).(time.Time); !ok {
		t.Error("date arg should bind as a time.Time")
	}
	if got := valueToArg(record.TimeOfDay(int64(14 * time.Hour))); got != "14:00:00" {
		t.Errorf("time-of-day arg = %#v, want clock text", got)
	}
}

// TestAWatermarkKeepsItsExactValue: the cursor is persisted as JSON between
// runs, and a rounded cursor either re-reads rows or skips them.
func TestAWatermarkKeepsItsExactValue(t *testing.T) {
	raw, err := watermarkJSON(record.Decimal(900719925474099301, 2))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "9007199254740993.01" {
		t.Errorf("watermark = %s, want the exact digits (a float64 cannot hold this)", raw)
	}

	raw, err = watermarkJSON(record.TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `"2026-08-08T09:30:00Z"` {
		t.Errorf("watermark = %s, want RFC 3339 text", raw)
	}
	// And it parses back into a bindable argument — the round trip cursorArg
	// performs on the next run.
	arg, err := cursorArg(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := arg.(time.Time); !ok {
		t.Errorf("cursorArg = %#v, want a time.Time", arg)
	}
}
