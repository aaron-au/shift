package ndjson

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

func writeOne(t *testing.T, build func(*record.Builder)) string {
	t.Helper()
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	build(bld)
	bld.EndMap()
	b.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out)
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// TestADecimalIsWrittenAsABareNumberWithEveryDigit: quoting it would change the
// JSON type of the field and break schema-validating consumers, so it stays a
// number — but with the exact digits the scale claims, not a float's rendering.
func TestADecimalIsWrittenAsABareNumberWithEveryDigit(t *testing.T) {
	got := writeOne(t, func(bld *record.Builder) {
		bld.KeyLiteral("amount")
		bld.Decimal(1010, 2)
		bld.KeyLiteral("tiny")
		bld.Decimal(5, 3)
		bld.KeyLiteral("owed")
		bld.Decimal(-1234, 2)
		bld.KeyLiteral("thousands")
		bld.Decimal(101, -3)
	})
	want := `{"amount":10.10,"tiny":0.005,"owed":-12.34,"thousands":101000}` + "\n"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestTemporalValuesAreWrittenAsJSONStrings(t *testing.T) {
	melb := time.FixedZone("AEST", 10*60*60)
	got := writeOne(t, func(bld *record.Builder) {
		bld.KeyLiteral("at")
		bld.TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 0, melb))
		bld.KeyLiteral("on")
		bld.Date(20673)
		bld.KeyLiteral("tod")
		bld.TimeOfDay(int64(14*time.Hour + 30*time.Minute))
	})
	want := `{"at":"2026-08-08T09:30:00+10:00","on":"2026-08-08","tod":"14:30:00"}` + "\n"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// TestTheWrittenDecimalIsValidJSON checks the output parses as JSON and, read
// back by our own reader, is the same number — while documenting the limit:
// JSON has no decimal type, so it returns as a float unless the field is
// declared (ADR-0051 §5).
func TestTheWrittenDecimalIsValidJSON(t *testing.T) {
	line := writeOne(t, func(bld *record.Builder) {
		bld.KeyLiteral("amount")
		bld.Decimal(1010, 2)
	})
	r := NewReader(bytes.NewReader([]byte(line)), ReaderOptions{})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := b.Record(0).Field("amount")
	if !ok {
		t.Fatal("amount missing")
	}
	if v.Kind() != record.KindFloat {
		t.Errorf("kind = %v, want float: JSON carries no decimal type, so a "+
			"round trip through it is lossy unless the field is declared", v.Kind())
	}
	if v.Float() != 10.10 {
		t.Errorf("amount = %v, want 10.10", v.Float())
	}
}

// TestJSONNumbersAreStillFloats guards the compatibility promise in ADR-0051
// §5: reading is unchanged, so no existing flow's output moves.
func TestJSONNumbersAreStillFloats(t *testing.T) {
	r := NewReader(bytes.NewReader([]byte(`{"a":10.10,"b":7}`+"\n")), ReaderOptions{})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rec := b.Record(0)
	if v, _ := rec.Field("a"); v.Kind() != record.KindFloat {
		t.Errorf("a = %v, want float (a bare JSON number must not become a decimal)", v.Kind())
	}
	if v, _ := rec.Field("b"); v.Kind() != record.KindInt {
		t.Errorf("b = %v, want int", v.Kind())
	}
}
