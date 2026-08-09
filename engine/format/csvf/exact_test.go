package csvf

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// TestAMoneyColumnRoundTripsExactly is the end-to-end claim the decimal kind
// exists for: a CSV of amounts read and written back must be the same file.
// With TypeFloat it is not — 10.10 comes back as 10.1, and a 3-decimal amount
// comes back with a rounding tail.
func TestAMoneyColumnRoundTripsExactly(t *testing.T) {
	const in = "id,amount\n1,10.10\n2,0.005\n3,-12.30\n4,1000000.00\n5,\n"

	r := NewReader(strings.NewReader(in), ReaderOptions{
		Types: map[string]ColumnType{"amount": TypeDecimal},
	})
	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{})

	for {
		b, err := r.Next(context.Background())
		if err != nil {
			break
		}
		if err := w.Write(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != in {
		t.Errorf("round trip changed the file:\n got %q\nwant %q", got, in)
	}
}

// TestTheSameColumnAsAFloatDoesNotRoundTrip pins why the column type is needed
// at all, rather than asserting it in a comment.
func TestTheSameColumnAsAFloatDoesNotRoundTrip(t *testing.T) {
	const in = "amount\n10.10\n"
	r := NewReader(strings.NewReader(in), ReaderOptions{
		Types: map[string]ColumnType{"amount": TypeFloat},
	})
	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if out.String() == in {
		t.Error("a float column unexpectedly round-tripped 10.10; TypeDecimal may no longer be needed")
	}
}

func TestTypedColumnsParseTheExactKinds(t *testing.T) {
	const in = "amount,at,on,tod\n10.10,2026-08-08T09:30:00+10:00,2026-08-08,14:30:05\n"
	r := NewReader(strings.NewReader(in), ReaderOptions{
		Types: map[string]ColumnType{
			"amount": TypeDecimal, "at": TypeTimestamp, "on": TypeDate, "tod": TypeTime,
		},
	})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rec := b.Record(0)
	cases := []struct {
		field string
		kind  record.Kind
		text  string
	}{
		{"amount", record.KindDecimal, "10.10"},
		{"at", record.KindTimestamp, "2026-08-08T09:30:00+10:00"},
		{"on", record.KindDate, "2026-08-08"},
		{"tod", record.KindTime, "14:30:05"},
	}
	for _, c := range cases {
		v, ok := rec.Field(c.field)
		if !ok {
			t.Errorf("%s missing", c.field)
			continue
		}
		if v.Kind() != c.kind || v.Text() != c.text {
			t.Errorf("%s = %v %q, want %v %q", c.field, v.Kind(), v.Text(), c.kind, c.text)
		}
	}
}

// TestAPaddedCellStillParses: CSV exported from a fixed-width system carries
// padding, and " 10.10 " is the same amount as "10.10".
func TestAPaddedCellStillParses(t *testing.T) {
	r := NewReader(strings.NewReader("amount\n  10.10  \n"), ReaderOptions{
		Types: map[string]ColumnType{"amount": TypeDecimal},
	})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	v, _ := b.Record(0).Field("amount")
	if v.Kind() != record.KindDecimal || v.Text() != "10.10" {
		t.Errorf("amount = %v %q", v.Kind(), v.Text())
	}
}

func TestAnEmptyTypedCellIsNullNotZero(t *testing.T) {
	r := NewReader(strings.NewReader("amount,at\n,\n"), ReaderOptions{
		Types: map[string]ColumnType{"amount": TypeDecimal, "at": TypeTimestamp},
	})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"amount", "at"} {
		if v, _ := b.Record(0).Field(f); !v.IsNull() {
			t.Errorf("%s = %v, want null (an absent amount is not zero)", f, v.Kind())
		}
	}
}

func TestABadTypedCellNamesTheColumnAndRow(t *testing.T) {
	r := NewReader(strings.NewReader("amount\nnot-a-number\n"), ReaderOptions{
		Types: map[string]ColumnType{"amount": TypeDecimal},
	})
	_, err := r.Next(context.Background())
	if err == nil {
		t.Fatal("expected a parse error")
	}
	for _, want := range []string{"row 2", "amount"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
