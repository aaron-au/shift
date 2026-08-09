package fixedw

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// invoice is the layout used across these tests: an id, a name, a zoned signed
// amount with two implied places, a packed date and a flag, with a filler gap
// where the source system keeps something we do not read.
func invoice() []Column {
	return []Column{
		{Name: "id", Width: 6, Type: TypeInt, Pad: '0'},
		{Name: "name", Width: 10, Type: TypeString},
		{Width: 2}, // filler
		{Name: "amount", Width: 8, Type: TypeZoned, Scale: 2, Pad: '0'},
		{Name: "due", Width: 8, Type: TypeDate},
		{Name: "paid", Width: 1, Type: TypeBool},
	}
}

func readAll(t *testing.T, in string, opts ReaderOptions) []record.Value {
	t.Helper()
	r := NewReader(strings.NewReader(in), opts)
	var out []record.Value
	keep := record.NewBatch()
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// Copy out: the reader reuses its batch (the lifetime contract).
		for _, rec := range b.Records() {
			out = append(out, record.CopyValue(keep, rec))
		}
	}
	return out
}

func field(t *testing.T, rec record.Value, name string) record.Value {
	t.Helper()
	v, ok := rec.Field(name)
	if !ok {
		t.Fatalf("field %q missing", name)
	}
	return v
}

func TestReadingALayoutProducesTypedFields(t *testing.T) {
	//        id    name       fill  amount    due      paid
	in := "000042ACME Ltd  " + "XX" + "0001010{" + "20260808" + "Y\n"
	recs := readAll(t, in, ReaderOptions{Columns: invoice()})
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	rec := recs[0]

	if v := field(t, rec, "id"); v.Kind() != record.KindInt || v.Int() != 42 {
		t.Errorf("id = %v %v", v.Kind(), v.Int())
	}
	if v := field(t, rec, "name"); v.String() != "ACME Ltd" {
		t.Errorf("name = %q, want ACME Ltd (padding trimmed)", v.String())
	}
	// The zoned amount: "0001010{" is +10100 at scale 2.
	if v := field(t, rec, "amount"); v.Kind() != record.KindDecimal || v.Text() != "101.00" {
		t.Errorf("amount = %v %q, want decimal 101.00", v.Kind(), v.Text())
	}
	if v := field(t, rec, "due"); v.Kind() != record.KindDate || v.Text() != "2026-08-08" {
		t.Errorf("due = %v %q", v.Kind(), v.Text())
	}
	if v := field(t, rec, "paid"); v.Kind() != record.KindBool || !v.Bool() {
		t.Errorf("paid = %v", v.Kind())
	}
	// The filler is not a field.
	if _, ok := rec.Field(""); ok {
		t.Error("filler leaked into the record")
	}
	if rec.Len() != 5 {
		t.Errorf("record has %d fields, want 5", rec.Len())
	}
}

// TestALayoutRoundTrips is the property that matters: read a file, write it
// back, get the same bytes.
func TestALayoutRoundTrips(t *testing.T) {
	in := "000042ACME Ltd  " + "  " + "0001010{" + "20260808" + "Y\n" +
		"000043Beta Pty  " + "  " + "0000050}" + "20261231" + "N\n"

	r := NewReader(strings.NewReader(in), ReaderOptions{Columns: invoice()})
	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: invoice()})
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Write(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != in {
		t.Errorf("round trip differs:\n got %q\nwant %q", got, in)
	}
}

// TestANegativeZonedAmountKeepsItsSign — the overpunch is the only place the
// sign lives, so losing it turns a credit into a debit.
//
// "0000050}" is eight digit positions: 0,0,0,0,0,5,0 and a final '}', which is
// digit 0 carrying the negative sign. That is -00000500, or -5.00 at scale 2 —
// the last position is a DIGIT, not a sign appended to the number.
func TestANegativeZonedAmountKeepsItsSign(t *testing.T) {
	in := "000043Beta Pty    " + "0000050}" + "20261231" + "N\n"
	recs := readAll(t, in, ReaderOptions{Columns: invoice()})
	if v := field(t, recs[0], "amount"); v.Text() != "-5.00" {
		t.Errorf("amount = %q, want -5.00", v.Text())
	}
	// And a genuine -0.50, to show the position of the point is the scale's
	// business and not the sign's.
	in2 := "000043Beta Pty    " + "0000005}" + "20261231" + "N\n"
	recs2 := readAll(t, in2, ReaderOptions{Columns: invoice()})
	if v := field(t, recs2[0], "amount"); v.Text() != "-0.50" {
		t.Errorf("amount = %q, want -0.50", v.Text())
	}
}

// TestAZeroPaddedNumberKeepsItsTrailingZeros guards the trim rule: stripping a
// zero pad from both ends would read "000100" as 1.
func TestAZeroPaddedNumberKeepsItsTrailingZeros(t *testing.T) {
	cols := []Column{{Name: "n", Width: 6, Type: TypeInt, Pad: '0'}}
	recs := readAll(t, "000100\n", ReaderOptions{Columns: cols})
	if v := field(t, recs[0], "n"); v.Int() != 100 {
		t.Errorf("n = %d, want 100", v.Int())
	}
	// A space-padded column still trims both ends, where that is safe.
	cols2 := []Column{{Name: "s", Width: 8, Type: TypeString}}
	recs2 := readAll(t, "  ACME  \n", ReaderOptions{Columns: cols2})
	if v := field(t, recs2[0], "s"); v.String() != "ACME" {
		t.Errorf("s = %q, want ACME", v.String())
	}
}

func TestZonedOverpunchCoversTheWholeAlphabet(t *testing.T) {
	cols := []Column{{Name: "n", Width: 3, Type: TypeZoned, Pad: '0'}}
	cases := []struct {
		cell string
		want string
	}{
		{"01{", "10"}, {"01A", "11"}, {"01I", "19"},
		{"01}", "-10"}, {"01J", "-11"}, {"01R", "-19"},
		{"012", "12"}, // a plain digit is unsigned/positive
		{"01a", "11"}, // down-cased on export
		{"01j", "-11"},
	}
	for _, c := range cases {
		recs := readAll(t, c.cell+"\n", ReaderOptions{Columns: cols})
		if len(recs) != 1 {
			t.Fatalf("%q: got %d records", c.cell, len(recs))
		}
		if got := field(t, recs[0], "n").Text(); got != c.want {
			t.Errorf("%q = %s, want %s", c.cell, got, c.want)
		}
	}
	// And every digit round-trips through the encoder.
	for d := range 10 {
		for _, neg := range []bool{false, true} {
			b, err := encodeOverpunch(byte('0'+d), neg)
			if err != nil {
				t.Fatal(err)
			}
			gotDigit, gotNeg, ok := decodeOverpunch(b)
			if !ok || gotDigit != byte('0'+d) || gotNeg != neg {
				t.Errorf("digit %d neg=%v encoded %q, decoded %q/%v/%v", d, neg, b, gotDigit, gotNeg, ok)
			}
		}
	}
}

func TestAnInvalidOverpunchIsReported(t *testing.T) {
	cols := []Column{{Name: "n", Width: 3, Type: TypeZoned}}
	r := NewReader(strings.NewReader("01*\n"), ReaderOptions{Columns: cols})
	_, err := r.Next(context.Background())
	if err == nil {
		t.Fatal("expected an error for an invalid sign character")
	}
	if !strings.Contains(err.Error(), "zoned") {
		t.Errorf("error = %v, want it to name the zoned column", err)
	}
}

// TestAValueTooWideForItsColumnIsRefused is the rule the whole writer exists
// to enforce: a truncated account number is indistinguishable from a real one.
func TestAValueTooWideForItsColumnIsRefused(t *testing.T) {
	cols := []Column{{Name: "name", Width: 4, Type: TypeString}}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("name")
	bld.StringLiteral("TOO LONG")
	bld.EndMap()
	b.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols})
	err := w.Write(context.Background(), b)
	if err == nil {
		t.Fatal("a value wider than its column must be refused, not trimmed")
	}
	for _, want := range []string{"name", "truncating"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if out.Len() != 0 {
		t.Errorf("refused but wrote %q", out.String())
	}
}

// TestRoundingAtTheColumnScaleIsRefusedToo — the same principle applied to
// precision rather than length.
func TestRoundingAtTheColumnScaleIsRefusedToo(t *testing.T) {
	cols := []Column{{Name: "amount", Width: 8, Type: TypeDecimal, Scale: 2, Pad: '0'}}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(10105, 3) // 10.105 — three places into a two-place column
	bld.EndMap()
	b.Append(bld.Finish())

	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: cols})
	err := w.Write(context.Background(), b)
	if err == nil {
		t.Fatal("silently rounding 10.105 into a 2-place column must be refused")
	}
	if !strings.Contains(err.Error(), "decimal places") {
		t.Errorf("error = %v", err)
	}

	// Trailing zeros are not a loss, so those rescale silently.
	b2 := record.NewBatch()
	bld2 := b2.Builder()
	bld2.BeginMap()
	bld2.KeyLiteral("amount")
	bld2.Decimal(101000, 4) // 10.1000 → 10.10
	bld2.EndMap()
	b2.Append(bld2.Finish())

	var out bytes.Buffer
	w2 := NewWriter(&out, WriterOptions{Columns: cols})
	if err := w2.Write(context.Background(), b2); err != nil {
		t.Fatalf("10.1000 fits a 2-place column exactly: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "00001010\n" {
		t.Errorf("got %q, want 00001010 (implied point, zero-padded)", got)
	}
}

// TestAShortRecordIsAnError: in a positional format every field still parses
// when the layout is wrong, just from the wrong bytes, so length is checked.
func TestAShortRecordIsAnError(t *testing.T) {
	r := NewReader(strings.NewReader("000042ACME\n"), ReaderOptions{Columns: invoice()})
	_, err := r.Next(context.Background())
	if err == nil {
		t.Fatal("expected a length error")
	}
	if !strings.Contains(err.Error(), "disagree") {
		t.Errorf("error = %v, want it to say the file and layout disagree", err)
	}
}

func TestTrailingContentThatIsNotPaddingIsAnError(t *testing.T) {
	line := "000042ACME Ltd  " + "XX" + "0001010{" + "20260808" + "Y" + "junk\n"
	r := NewReader(strings.NewReader(line), ReaderOptions{Columns: invoice()})
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("unaccounted trailing bytes must be reported, not dropped")
	}
	// Trailing spaces are ordinary and must be tolerated.
	ok := "000042ACME Ltd  " + "XX" + "0001010{" + "20260808" + "Y" + "   \n"
	if recs := readAll(t, ok, ReaderOptions{Columns: invoice()}); len(recs) != 1 {
		t.Fatalf("trailing spaces should be tolerated, got %d records", len(recs))
	}
}

func TestABlankColumnIsNullNotZero(t *testing.T) {
	in := "      ACME Ltd  " + "XX" + "        " + "20260808" + "Y\n"
	recs := readAll(t, in, ReaderOptions{Columns: invoice()})
	for _, name := range []string{"id", "amount"} {
		if v := field(t, recs[0], name); !v.IsNull() {
			t.Errorf("%s = %v %q, want null (a fixed-width file has no other way to say absent)",
				name, v.Kind(), v.Text())
		}
	}
}

func TestUnseparatedRecordsAreReadByLength(t *testing.T) {
	cols := []Column{
		{Name: "a", Width: 3, Type: TypeInt},
		{Name: "b", Width: 2, Type: TypeString},
	}
	recs := readAll(t, "001XY002ZW", ReaderOptions{Columns: cols, Unseparated: true})
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if v := field(t, recs[1], "a"); v.Int() != 2 {
		t.Errorf("second a = %d", v.Int())
	}
	if v := field(t, recs[1], "b"); v.String() != "ZW" {
		t.Errorf("second b = %q", v.String())
	}
}

func TestATrailingPartialRecordIsAnError(t *testing.T) {
	cols := []Column{{Name: "a", Width: 3, Type: TypeInt}}
	r := NewReader(strings.NewReader("001002XX"), ReaderOptions{Columns: cols, Unseparated: true})
	var err error
	for err == nil {
		_, err = r.Next(context.Background())
	}
	if errors.Is(err, io.EOF) {
		t.Fatal("a trailing partial record must be an error, not a clean EOF")
	}
	if !strings.Contains(err.Error(), "partial record") {
		t.Errorf("error = %v", err)
	}
}

func TestSkipLinesDropsABanner(t *testing.T) {
	cols := []Column{{Name: "a", Width: 3, Type: TypeInt}}
	recs := readAll(t, "HEADER\n001\n002\n", ReaderOptions{Columns: cols, SkipLines: 1})
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestLayoutValidation(t *testing.T) {
	cases := []struct {
		name string
		cols []Column
		want string
	}{
		{"empty", nil, "no columns"},
		{"zero width", []Column{{Name: "a"}}, "positive width"},
		{"negative width", []Column{{Name: "a", Width: -1}}, "positive width"},
		{"duplicate name", []Column{{Name: "a", Width: 1}, {Name: "a", Width: 1}}, "two fields of the same name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Length(c.cols)
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
	n, err := Length(invoice())
	if err != nil {
		t.Fatal(err)
	}
	if n != 35 {
		t.Errorf("record length = %d, want 35", n)
	}
}

func TestTemporalColumnsUsePackedLayouts(t *testing.T) {
	cols := []Column{
		{Name: "at", Width: 14, Type: TypeTimestamp},
		{Name: "on", Width: 8, Type: TypeDate},
		{Name: "tod", Width: 6, Type: TypeTime},
	}
	recs := readAll(t, "20260808093000"+"20260808"+"143005\n", ReaderOptions{Columns: cols})
	rec := recs[0]
	if v := field(t, rec, "at"); v.Kind() != record.KindTimestamp || v.Text() != "2026-08-08T09:30:00Z" {
		t.Errorf("at = %v %q", v.Kind(), v.Text())
	}
	if v := field(t, rec, "on"); v.Text() != "2026-08-08" {
		t.Errorf("on = %q", v.Text())
	}
	if v := field(t, rec, "tod"); v.Text() != "14:30:05" {
		t.Errorf("tod = %q", v.Text())
	}
}

// TestATimestampColumnHonoursItsDeclaredZone: a fixed-width timestamp carries
// no offset, so assuming one silently is how a date lands a day out.
func TestATimestampColumnHonoursItsDeclaredZone(t *testing.T) {
	cols := []Column{{Name: "at", Width: 14, Type: TypeTimestamp, Location: "Australia/Melbourne"}}
	recs := readAll(t, "20260808093000\n", ReaderOptions{Columns: cols})
	v := field(t, recs[0], "at")
	want := time.Date(2026, 8, 8, 9, 30, 0, 0, time.FixedZone("AEST", 10*60*60))
	if v.UnixNano() != want.UnixNano() {
		t.Errorf("at = %s, want the same instant as %s", v.Text(), want.Format(time.RFC3339))
	}
}

func TestAMissingFieldWritesABlankColumn(t *testing.T) {
	cols := []Column{
		{Name: "a", Width: 3, Type: TypeString},
		{Name: "b", Width: 3, Type: TypeString},
	}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.StringLiteral("xy")
	bld.EndMap()
	b.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "xy    \n" {
		t.Errorf("got %q, want %q", got, "xy    \n")
	}
}

func TestWriterRejectsAMismatchedKind(t *testing.T) {
	cols := []Column{{Name: "n", Width: 4, Type: TypeInt}}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("n")
	bld.StringLiteral("abc")
	bld.EndMap()
	b.Append(bld.Finish())

	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), b); err == nil {
		t.Fatal("a string in an int column must be refused")
	}
}

func TestAlignmentAndPaddingDefaults(t *testing.T) {
	cols := []Column{
		{Name: "s", Width: 5, Type: TypeString},        // left, space
		{Name: "n", Width: 5, Type: TypeInt},           // right, space
		{Name: "z", Width: 5, Type: TypeInt, Pad: '0'}, // right, zero
		{Name: "l", Width: 5, Type: TypeInt, Align: AlignLeft},
	}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("s")
	bld.StringLiteral("ab")
	bld.KeyLiteral("n")
	bld.Int(42)
	bld.KeyLiteral("z")
	bld.Int(42)
	bld.KeyLiteral("l")
	bld.Int(42)
	bld.EndMap()
	b.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "ab   "+"   42"+"00042"+"42   "+"\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadsFloatAndDecimalColumns(t *testing.T) {
	cols := []Column{
		{Name: "f", Width: 8, Type: TypeFloat},
		{Name: "d", Width: 8, Type: TypeDecimal},                     // point written in the data
		{Name: "i", Width: 6, Type: TypeDecimal, Scale: 3, Pad: '0'}, // point implied
	}
	recs := readAll(t, "  1.5   "+"  10.10 "+"001234\n", ReaderOptions{Columns: cols})
	rec := recs[0]
	if v := field(t, rec, "f"); v.Kind() != record.KindFloat || v.Float() != 1.5 {
		t.Errorf("f = %v %v", v.Kind(), v.Float())
	}
	if v := field(t, rec, "d"); v.Text() != "10.10" {
		t.Errorf("d = %q, want 10.10", v.Text())
	}
	if v := field(t, rec, "i"); v.Text() != "1.234" {
		t.Errorf("i = %q, want 1.234 (implied scale)", v.Text())
	}
}

// TestAnExplicitPointBeatsTheImpliedScale: if the file wrote the point, the
// layout's implied scale must not shift it again.
func TestAnExplicitPointBeatsTheImpliedScale(t *testing.T) {
	cols := []Column{{Name: "d", Width: 8, Type: TypeDecimal, Scale: 3}}
	recs := readAll(t, "   10.10\n", ReaderOptions{Columns: cols})
	if v := field(t, recs[0], "d"); v.Text() != "10.10" {
		t.Errorf("d = %q, want 10.10 unshifted", v.Text())
	}
}

func TestBadCellsAreReportedPerType(t *testing.T) {
	cases := []struct {
		col  Column
		cell string
		want string
	}{
		{Column{Name: "n", Width: 4, Type: TypeInt}, "abcd", "not an int"},
		{Column{Name: "f", Width: 4, Type: TypeFloat}, "abcd", "not a float"},
		{Column{Name: "b", Width: 4, Type: TypeBool}, "abcd", "not a bool"},
		{Column{Name: "d", Width: 4, Type: TypeDecimal}, "abcd", "decimal"},
		{Column{Name: "t", Width: 8, Type: TypeDate}, "notadate", "not a date"},
	}
	for _, c := range cases {
		r := NewReader(strings.NewReader(c.cell+"\n"), ReaderOptions{Columns: []Column{c.col}})
		_, err := r.Next(context.Background())
		if err == nil {
			t.Errorf("%s: %q was accepted", c.col.Type, c.cell)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not contain %q", c.col.Type, err, c.want)
		}
	}
}

func TestBoolSpellingsBothWays(t *testing.T) {
	cols := []Column{{Name: "b", Width: 5, Type: TypeBool}}
	for _, s := range []string{"Y", "y", "T", "t", "1", "true", "TRUE"} {
		recs := readAll(t, s+strings.Repeat(" ", 5-len(s))+"\n", ReaderOptions{Columns: cols})
		if v := field(t, recs[0], "b"); !v.Bool() {
			t.Errorf("%q should read true", s)
		}
	}
	for _, s := range []string{"N", "n", "F", "f", "0", "false", "FALSE"} {
		recs := readAll(t, s+strings.Repeat(" ", 5-len(s))+"\n", ReaderOptions{Columns: cols})
		if v := field(t, recs[0], "b"); v.Bool() {
			t.Errorf("%q should read false", s)
		}
	}
}

func TestAnUnknownLocationIsReported(t *testing.T) {
	cols := []Column{{Name: "at", Width: 8, Type: TypeDate, Location: "Mars/Olympus"}}
	r := NewReader(strings.NewReader("20260808\n"), ReaderOptions{Columns: cols})
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("an unknown zone was accepted")
	}
	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), record.NewBatch()); err == nil {
		t.Fatal("an unknown zone was accepted by the writer")
	}
}

// TestTheWriterStaysFailedAfterAnError: a half-written fixed-width file is
// worse than none, so the first failure sticks rather than letting later
// records append onto a broken record.
func TestTheWriterStaysFailedAfterAnError(t *testing.T) {
	cols := []Column{{Name: "n", Width: 2, Type: TypeInt}}
	bad := record.NewBatch()
	bld := bad.Builder()
	bld.BeginMap()
	bld.KeyLiteral("n")
	bld.Int(12345) // too wide
	bld.EndMap()
	bad.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), bad); err == nil {
		t.Fatal("expected an overflow error")
	}
	good := record.NewBatch()
	gb := good.Builder()
	gb.BeginMap()
	gb.KeyLiteral("n")
	gb.Int(7)
	gb.EndMap()
	good.Append(gb.Finish())
	if err := w.Write(context.Background(), good); err == nil {
		t.Error("the writer accepted more records after failing")
	}
	if err := w.Close(); err == nil {
		t.Error("Close reported success after a failed write")
	}
}

func TestTheWriterRejectsANonMapRecord(t *testing.T) {
	b := record.NewBatch()
	b.Append(record.Int(1))
	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: []Column{{Name: "n", Width: 2}}})
	if err := w.Write(context.Background(), b); err == nil {
		t.Fatal("a non-map record was accepted")
	}
}

func TestAStringColumnRendersOtherKindsCanonically(t *testing.T) {
	cols := []Column{
		{Name: "id", Width: 4, Type: TypeString},
		{Name: "on", Width: 10, Type: TypeString},
	}
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(42) // an id that happens to be an int is ordinary
	bld.KeyLiteral("on")
	bld.Date(20673)
	bld.EndMap()
	b.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols})
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "42  2026-08-08\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestWritingUnseparatedOmitsTheTerminator(t *testing.T) {
	cols := []Column{{Name: "n", Width: 3, Type: TypeInt, Pad: '0'}}
	b := record.NewBatch()
	bld := b.Builder()
	for _, n := range []int64{1, 2} {
		bld.BeginMap()
		bld.KeyLiteral("n")
		bld.Int(n)
		bld.EndMap()
		b.Append(bld.Finish())
	}
	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: cols, Unseparated: true})
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "001002" {
		t.Errorf("got %q, want 001002", got)
	}
}

func TestALayoutErrorSurfacesOnFirstUse(t *testing.T) {
	r := NewReader(strings.NewReader("x\n"), ReaderOptions{Columns: []Column{{Name: "a", Width: 0}}})
	if _, err := r.Next(context.Background()); err == nil {
		t.Error("an invalid layout was accepted by the reader")
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: []Column{{Name: "a", Width: 0}}})
	if err := w.Write(context.Background(), record.NewBatch()); err == nil {
		t.Error("an invalid layout was accepted by the writer")
	}
}

func TestColumnTypeNames(t *testing.T) {
	want := map[ColumnType]string{
		TypeString: "string", TypeInt: "int", TypeFloat: "float", TypeBool: "bool",
		TypeDecimal: "decimal", TypeZoned: "zoned", TypeTimestamp: "timestamp",
		TypeDate: "date", TypeTime: "time",
	}
	for k, s := range want {
		if got := k.String(); got != s {
			t.Errorf("ColumnType(%d) = %q, want %q", k, got, s)
		}
	}
	if got := ColumnType(200).String(); got != "invalid" {
		t.Errorf("unknown type = %q", got)
	}
}

// TestALineLongerThanTheReadBufferStillParses exercises the ReadSlice
// buffer-full fallback: rare, but a wide layout can exceed bufio's default.
func TestALineLongerThanTheReadBufferStillParses(t *testing.T) {
	const width = 8192
	cols := []Column{{Name: "wide", Width: width, Type: TypeString}}
	line := strings.Repeat("x", width) + "\n"
	recs := readAll(t, line, ReaderOptions{Columns: cols})
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if v := field(t, recs[0], "wide"); len(v.String()) != width {
		t.Errorf("wide is %d bytes, want %d", len(v.String()), width)
	}
}

func TestContextCancellationStopsTheReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewReader(strings.NewReader("000042ACME Ltd    0001010{20260808Y\n"),
		ReaderOptions{Columns: invoice()})
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Next = %v, want context.Canceled", err)
	}
	w := NewWriter(&bytes.Buffer{}, WriterOptions{Columns: invoice()})
	if err := w.Write(ctx, record.NewBatch()); !errors.Is(err, context.Canceled) {
		t.Errorf("Write = %v, want context.Canceled", err)
	}
}
