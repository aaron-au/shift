package record

import (
	"testing"
	"time"
)

func TestATimestampKeepsItsInstantAndItsOffset(t *testing.T) {
	melb := time.FixedZone("AEST", 10*60*60)
	src := time.Date(2026, 8, 8, 9, 30, 0, 0, melb)

	v := TimestampAt(src)
	if got := v.UnixNano(); got != src.UnixNano() {
		t.Errorf("UnixNano() = %d, want %d", got, src.UnixNano())
	}
	if got := v.ZoneOffset(); got != 10*time.Hour {
		t.Errorf("ZoneOffset() = %v, want 10h", got)
	}
	// The offset is kept so the value renders the way it arrived, rather than
	// being normalised to UTC behind the author's back.
	if got := v.Text(); got != "2026-08-08T09:30:00+10:00" {
		t.Errorf("Text() = %q, want 2026-08-08T09:30:00+10:00", got)
	}
}

// TestTimestampsFromDifferentZonesOrderAsInstants is the property that makes
// the stored offset safe: it is presentation, never part of the comparison.
func TestTimestampsFromDifferentZonesOrderAsInstants(t *testing.T) {
	utc := TimestampAt(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
	melb := TimestampAt(time.Date(2026, 8, 8, 10, 0, 0, 0, time.FixedZone("AEST", 10*60*60)))
	// Same instant, written two ways.
	if c, ok := Compare(utc, melb); !ok || c != 0 {
		t.Errorf("Compare = %d,%v; the two spellings are the same instant", c, ok)
	}
	if !utc.EqualScalar(melb) {
		t.Error("EqualScalar said two spellings of one instant differ")
	}
	later := TimestampAt(time.Date(2026, 8, 8, 0, 0, 1, 0, time.UTC))
	if c, _ := Compare(utc, later); c != -1 {
		t.Errorf("Compare(t, t+1s) = %d, want -1", c)
	}
}

// TestAnOddZoneOffsetRoundsForDisplayAndLeavesTheInstantAlone pins the one
// documented loss in ADR-0051 §2.
func TestAnOddZoneOffsetRoundsForDisplayAndLeavesTheInstantAlone(t *testing.T) {
	// Liberia ran at -00:44:30 until 1972 — not a multiple of 15 minutes.
	odd := time.FixedZone("LRT", -(44*60 + 30))
	src := time.Date(1970, 3, 1, 12, 0, 0, 0, odd)
	v := TimestampAt(src)

	if got := v.UnixNano(); got != src.UnixNano() {
		t.Errorf("the instant must be exact: UnixNano() = %d, want %d", got, src.UnixNano())
	}
	if got := v.ZoneOffset(); got != -45*time.Minute {
		t.Errorf("ZoneOffset() = %v, want -45m (nearest 15-minute unit)", got)
	}
}

func TestZoneOffsetsAreClampedRatherThanWrapped(t *testing.T) {
	// No real zone is this far out, but a corrupt or synthetic input must not
	// wrap into a plausible offset of the opposite sign.
	if got := offsetUnits(40 * time.Hour); got != 127 {
		t.Errorf("offsetUnits(40h) = %d, want 127", got)
	}
	if got := offsetUnits(-40 * time.Hour); got != -128 {
		t.Errorf("offsetUnits(-40h) = %d, want -128", got)
	}
	// Rounding is symmetric about zero: 8 minutes each way rounds away.
	if got := offsetUnits(8 * time.Minute); got != 1 {
		t.Errorf("offsetUnits(8m) = %d, want 1", got)
	}
	if got := offsetUnits(-8 * time.Minute); got != -1 {
		t.Errorf("offsetUnits(-8m) = %d, want -1", got)
	}
}

// TestADateIsTheDayTheSourceSaw, not the day it happens to be in UTC.
func TestADateIsTheDayTheSourceSaw(t *testing.T) {
	melb := time.FixedZone("AEST", 10*60*60)
	// 9am on the 8th in Melbourne is still the 7th in UTC. The source wrote
	// the 8th, so the date is the 8th.
	v := DateAt(time.Date(2026, 8, 8, 9, 0, 0, 0, melb))
	if got := v.Text(); got != "2026-08-08" {
		t.Errorf("Text() = %q, want 2026-08-08", got)
	}
	if got := DateOf(2026, time.August, 8); !got.EqualScalar(v) {
		t.Errorf("DateOf disagreed with DateAt: %q vs %q", got.Text(), v.Text())
	}
}

func TestDatesBeforeTheEpochFloorRatherThanRoundTowardIt(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
		days int64
	}{
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), "1970-01-01", 0},
		{time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), "1970-01-02", 1},
		{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), "1969-12-31", -1},
		// Midday on the last day before the epoch: truncating toward zero
		// would give day 0 and report 1970-01-01.
		{time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC), "1969-12-31", -1},
		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), "1900-01-01", -25567},
	}
	for _, c := range cases {
		v := DateAt(c.in)
		if got := v.DateDays(); got != c.days {
			t.Errorf("DateAt(%s).DateDays() = %d, want %d", c.in.Format(time.RFC3339), got, c.days)
		}
		if got := v.Text(); got != c.want {
			t.Errorf("DateAt(%s).Text() = %q, want %q", c.in.Format(time.RFC3339), got, c.want)
		}
	}
}

func TestATimeOfDayCarriesNoDate(t *testing.T) {
	v := TimeOfDay(int64(14*time.Hour + 30*time.Minute + 5*time.Second))
	if got := v.Text(); got != "14:30:05" {
		t.Errorf("Text() = %q, want 14:30:05", got)
	}
	// Sub-second precision is only printed when there is some.
	frac := TimeOfDay(int64(14*time.Hour + 30*time.Minute + 5*time.Second + 250*time.Millisecond))
	if got := frac.Text(); got != "14:30:05.250000000" {
		t.Errorf("Text() = %q, want 14:30:05.250000000", got)
	}
	if got := TimeOfDay(0).Text(); got != "00:00:00" {
		t.Errorf("midnight Text() = %q, want 00:00:00", got)
	}
}

func TestTemporalTextRoundTripsThroughItsParser(t *testing.T) {
	cases := []struct {
		v     Value
		parse func([]byte) (Value, error)
	}{
		{TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 0, time.FixedZone("AEST", 10*60*60))), ParseTimestamp},
		{TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 123456789, time.UTC)), ParseTimestamp},
		{DateOf(2026, time.August, 8), ParseDate},
		{TimeOfDay(int64(14*time.Hour + 30*time.Minute + 5*time.Second)), ParseTimeOfDay},
		{TimeOfDay(int64(23*time.Hour + 59*time.Minute + 59*time.Second)), ParseTimeOfDay},
	}
	for _, c := range cases {
		text := c.v.Text()
		back, err := c.parse([]byte(text))
		if err != nil {
			t.Errorf("re-parsing %q (%v): %v", text, c.v.Kind(), err)
			continue
		}
		if !back.EqualScalar(c.v) {
			t.Errorf("%v round trip: %q became %q", c.v.Kind(), text, back.Text())
		}
		if back.Kind() != c.v.Kind() {
			t.Errorf("%v round trip changed kind to %v", c.v.Kind(), back.Kind())
		}
	}
}

func TestTemporalParsersRejectTheWrongShape(t *testing.T) {
	if _, err := ParseTimestamp([]byte("2026-08-08")); err == nil {
		t.Error("a bare date is not an RFC 3339 timestamp")
	}
	if _, err := ParseDate([]byte("2026-08-08T00:00:00Z")); err == nil {
		t.Error("a timestamp is not a YYYY-MM-DD date")
	}
	if _, err := ParseTimeOfDay([]byte("25:00:00")); err == nil {
		t.Error("hour 25 is not a time of day")
	}
	if _, err := ParseTimeOfDay([]byte("14:30:05.25")); err != nil {
		t.Errorf("a fractional second is a valid time of day: %v", err)
	}
}

// TestDifferentTemporalKindsDoNotSecretlyCompare — a date and a timestamp are
// different types, and pretending otherwise would need a midnight nobody wrote.
func TestDifferentTemporalKindsDoNotSecretlyCompare(t *testing.T) {
	ts := TimestampAt(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
	d := DateOf(2026, time.August, 8)
	tod := TimeOfDay(0)
	for _, pair := range [][2]Value{{ts, d}, {d, tod}, {tod, ts}, {ts, Int(0)}, {d, Float(0)}} {
		if _, ok := Compare(pair[0], pair[1]); ok {
			t.Errorf("Compare(%v, %v) claimed an ordering", pair[0].Kind(), pair[1].Kind())
		}
		if pair[0].EqualScalar(pair[1]) {
			t.Errorf("EqualScalar(%v, %v) said equal", pair[0].Kind(), pair[1].Kind())
		}
	}
}

// TestCopyValueCarriesTheAuxByte guards the kinds against the one mistake that
// would be invisible: a deep copy that keeps num and drops aux turns 10.10
// into 1010 and +10:00 into UTC. Aggregate state depends on this.
func TestCopyValueCarriesTheAuxByte(t *testing.T) {
	src, dst := NewBatch(), NewBatch()
	bld := src.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(1010, 2)
	bld.KeyLiteral("at")
	bld.TimestampAt(time.Date(2026, 8, 8, 9, 30, 0, 0, time.FixedZone("AEST", 10*60*60)))
	bld.KeyLiteral("on")
	bld.Date(20673)
	bld.KeyLiteral("tod")
	bld.TimeOfDay(int64(90 * time.Second))
	bld.EndMap()
	rec := bld.Finish()

	names := []string{"amount", "at", "on", "tod"}
	wantKinds := make([]Kind, len(names))
	for i, name := range names {
		orig, _ := rec.Field(name)
		wantKinds[i] = orig.Kind()
	}

	copied := CopyValue(dst, rec)
	src.Reset() // the source arena is now recycled; the copy must stand alone

	for i, name := range names {
		got, ok := copied.Field(name)
		if !ok {
			t.Errorf("%s missing from the copy", name)
			continue
		}
		if got.Kind() != wantKinds[i] {
			t.Errorf("%s kind = %v, want %v", name, got.Kind(), wantKinds[i])
		}
	}
	amount, _ := copied.Field("amount")
	if got := amount.Text(); got != "10.10" {
		t.Errorf("copied decimal = %q, want 10.10 (aux byte lost?)", got)
	}
	at, _ := copied.Field("at")
	if got := at.ZoneOffset(); got != 10*time.Hour {
		t.Errorf("copied timestamp offset = %v, want 10h (aux byte lost?)", got)
	}
	if got := at.Text(); got != "2026-08-08T09:30:00+10:00" {
		t.Errorf("copied timestamp = %q", got)
	}
	on, _ := copied.Field("on")
	if got := on.DateDays(); got != 20673 {
		t.Errorf("copied date = %d days, want 20673", got)
	}
	tod, _ := copied.Field("tod")
	if got := tod.Text(); got != "00:01:30" {
		t.Errorf("copied time of day = %q, want 00:01:30", got)
	}
}

func TestKindNamesCoverTheNewKinds(t *testing.T) {
	want := map[Kind]string{
		KindDecimal: "decimal", KindTimestamp: "timestamp",
		KindDate: "date", KindTime: "time",
	}
	for k, s := range want {
		if got := k.String(); got != s {
			t.Errorf("Kind(%d).String() = %q, want %q", k, got, s)
		}
	}
	if got := Kind(200).String(); got != "invalid" {
		t.Errorf("unknown kind = %q, want invalid", got)
	}
}

func TestTemporalAccessorsReadZeroForTheWrongKind(t *testing.T) {
	v := Int(5)
	if v.UnixNano() != 0 || v.ZoneOffset() != 0 || v.DateDays() != 0 || v.DayNanos() != 0 {
		t.Error("temporal accessors must read zero for a non-temporal kind")
	}
	if !v.AsTime().IsZero() {
		t.Error("AsTime on a non-temporal kind must be the zero time")
	}
	// An int is in AppendText's set (one correct text form everywhere); a float
	// is not, because each format constrains floats differently.
	if got := v.Text(); got != "5" {
		t.Errorf("Text() on an int = %q, want 5", got)
	}
	if got := Float(1.5).Text(); got != "" {
		t.Errorf("Text() on a float = %q, want empty", got)
	}
}
