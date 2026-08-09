package record

// Differential/property tests for the temporal kinds (ADR-0051 §1/§2),
// closing register row TC-004 alongside decimal_diff_test.go.
//
// The reference here is the time package: it owns the calendar (leap years,
// DST rules, the proleptic Gregorian extension backwards), and none of that
// arithmetic is repeated in temporal.go — so "does the kind agree with time"
// is the whole claim. Instants, calendar days and clock times are generated
// rather than listed, including the ones that are only reachable by generation:
// pre-epoch instants, the ends of the representable range, and both halves of
// a DST fold.
//
// The tz database is embedded (time/tzdata) rather than read from the host:
// a DST test that silently skips because a container has no /usr/share/zoneinfo
// proves nothing, and this is the one property that needs real zone rules.

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
	_ "time/tzdata"
)

// realZoneOffsets are offsets that exist, all multiples of 15 minutes so a
// value renders and re-parses without the quantisation of §2 getting involved.
// The odd ones (Nepal, Chatham, Newfoundland) are here because a "whole hours"
// assumption is a real bug that only they catch.
var realZoneOffsets = []time.Duration{
	0,
	5*time.Hour + 30*time.Minute,  // India
	5*time.Hour + 45*time.Minute,  // Nepal
	-3*time.Hour - 30*time.Minute, // Newfoundland
	10 * time.Hour,                // Melbourne, standard
	11 * time.Hour,                // Melbourne, daylight
	-8 * time.Hour,                // US Pacific
	14 * time.Hour,                // Kiribati, the eastern extreme
	-12 * time.Hour,               // the western extreme
	12*time.Hour + 45*time.Minute, // Chatham Islands
}

// genNanos draws an instant. The int64 nanosecond range *is* the representable
// range of KindTimestamp, so both ends are drawn deliberately rather than left
// to a uniform generator that would reach them once in 2^63 tries.
func genNanos(r *rand.Rand) int64 {
	switch r.Intn(8) {
	case 0:
		return 0 // the epoch itself
	case 1:
		return math.MaxInt64 // 2262-04-11, the last instant the kind can hold
	case 2:
		return math.MinInt64 // 1677-09-21, the first
	case 3:
		return int64(r.Intn(2001) - 1000) // straddling the epoch by nanoseconds
	case 4:
		// A whole second before the epoch: the arm where truncation toward zero
		// and flooring disagree.
		return -int64(r.Intn(1<<31)) * int64(time.Second)
	default:
		return int64(r.Uint64()) //nolint:gosec // deliberate full-range draw, both signs
	}
}

func genZone(r *rand.Rand) time.Duration { return realZoneOffsets[r.Intn(len(realZoneOffsets))] }

func fixedAt(off time.Duration) *time.Location {
	if off == 0 {
		return time.UTC
	}
	return time.FixedZone("", int(off/time.Second))
}

// TestTimestampsRoundTripAgainstTheTimePackage: over generated instants in
// generated zones, the value must hold the same instant time.Time does, render
// the way time renders it, and come back from its own parser unchanged. The
// instant is checked separately from the offset because §2 says only one of
// them is exact.
func TestTimestampsRoundTripAgainstTheTimePackage(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed)) //nolint:gosec // deterministic test data, not security
	var preEpoch int
	for range 5000 {
		nanos, off := genNanos(r), genZone(r)
		src := time.Unix(0, nanos).In(fixedAt(off))
		v := TimestampAt(src)

		if got := v.UnixNano(); got != nanos {
			t.Fatalf("UnixNano() = %d, want %d (%s)", got, nanos, src.Format(time.RFC3339Nano))
		}
		if got := v.ZoneOffset(); got != off {
			t.Fatalf("ZoneOffset() = %v, want %v", got, off)
		}
		if got := v.AsTime(); !got.Equal(src) {
			t.Fatalf("AsTime() = %s, want %s", got.Format(time.RFC3339Nano), src.Format(time.RFC3339Nano))
		}
		if got, want := v.Text(), src.Format(time.RFC3339Nano); got != want {
			t.Fatalf("Text() = %q, time says %q", got, want)
		}

		back, err := ParseTimestamp([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if back.UnixNano() != nanos || back.ZoneOffset() != off {
			t.Fatalf("round trip of %q gave %d/%v, want %d/%v",
				v.Text(), back.UnixNano(), back.ZoneOffset(), nanos, off)
		}
		if nanos < 0 {
			preEpoch++
		}
	}
	if preEpoch == 0 {
		t.Fatal("no pre-epoch instants were generated — the negative arm went untested")
	}
}

// TestTimestampOrderingMatchesTheTimePackage is the property the stored offset
// depends on: a timestamp is an instant, so ordering must agree with
// time.Time.Compare regardless of the zone each side was written in. Pairs
// with the same instant and different offsets are constructed explicitly,
// since that is the pair a wall-clock comparison gets wrong.
func TestTimestampOrderingMatchesTheTimePackage(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 5)) //nolint:gosec // deterministic test data, not security

	type instant struct {
		v Value
		t time.Time
	}
	pool := make([]instant, 0, 160)
	for len(pool) < 160 {
		nanos := genNanos(r)
		src := time.Unix(0, nanos).In(fixedAt(genZone(r)))
		pool = append(pool, instant{TimestampAt(src), src})
		// The same instant, spelled in another zone.
		twin := src.In(fixedAt(genZone(r)))
		pool = append(pool, instant{TimestampAt(twin), twin})
	}

	var sameInstantDifferentZone, ordered int
	for i := range pool {
		for j := range pool {
			got, ok := Compare(pool[i].v, pool[j].v)
			if !ok {
				t.Fatalf("Compare(%s, %s) reported incomparable", pool[i].v.Text(), pool[j].v.Text())
			}
			want := pool[i].t.Compare(pool[j].t)
			if got != want {
				t.Fatalf("Compare(%s, %s) = %d, time says %d",
					pool[i].v.Text(), pool[j].v.Text(), got, want)
			}
			if eq := pool[i].v.EqualScalar(pool[j].v); eq != (want == 0) {
				t.Fatalf("EqualScalar(%s, %s) = %v, time says equal=%v",
					pool[i].v.Text(), pool[j].v.Text(), eq, want == 0)
			}
			switch {
			case want == 0 && pool[i].v.ZoneOffset() != pool[j].v.ZoneOffset():
				sameInstantDifferentZone++
			case want != 0:
				ordered++
			}
		}
	}
	if sameInstantDifferentZone == 0 {
		t.Fatal("no same-instant-different-zone pairs were generated — the case a wall-clock comparison gets wrong went untested")
	}
	if ordered == 0 {
		t.Fatal("nothing in the pool was strictly ordered")
	}
}

// TestAnOddZoneOffsetNeverMovesTheInstant pins the exact size of the one loss
// ADR-0051 §2 admits to. An offset that is not a multiple of 15 minutes is
// rounded *for display*, so the error must be bounded by half a unit and the
// instant must not move at all.
func TestAnOddZoneOffsetNeverMovesTheInstant(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 6)) //nolint:gosec // deterministic test data, not security
	var odd int
	for range 2000 {
		off := time.Duration(r.Intn(2*18*3600+1)-18*3600) * time.Second
		nanos := genNanos(r)
		v := TimestampAt(time.Unix(0, nanos).In(fixedAt(off)))

		if got := v.UnixNano(); got != nanos {
			t.Fatalf("an offset of %v moved the instant: %d, want %d", off, got, nanos)
		}
		if d := v.ZoneOffset() - off; d > zoneUnit/2 || d < -zoneUnit/2 {
			t.Fatalf("ZoneOffset() = %v for a source offset of %v: off by %v, more than half a unit",
				v.ZoneOffset(), off, d)
		}
		if off%zoneUnit != 0 {
			odd++
		}
	}
	if odd == 0 {
		t.Fatal("no offsets outside the 15-minute grid were generated")
	}
}

// TestDatesMatchTheCalendarTheTimePackageKeeps checks the date kind against
// the only calendar that matters: the one in the time package. The day count
// is recomputed from the year/month/day time itself reports, so a date is
// wrong here whenever it names a different day than time would — including
// across the epoch, where flooring and truncation diverge, and across zone
// offsets, where "the day the source saw" is the whole point.
func TestDatesMatchTheCalendarTheTimePackageKeeps(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 7)) //nolint:gosec // deterministic test data, not security
	// Year 2 to year 9998, staying clear of the ends so a zone offset cannot
	// push the value outside what the YYYY-MM-DD layout can render.
	const minDay, maxDay = -718797, 2932532
	var preEpoch, leapDays int
	for range 5000 {
		days := int64(minDay + r.Intn(maxDay-minDay))
		secs := days*secondsPerDay + int64(r.Intn(secondsPerDay))
		src := time.Unix(secs, 0).In(fixedAt(genZone(r)))

		v := DateAt(src)
		if got, want := v.Text(), src.Format(dateLayout); got != want {
			t.Fatalf("DateAt(%s).Text() = %q, time says %q", src.Format(time.RFC3339), got, want)
		}
		y, m, d := src.Date()
		if got, want := v.DateDays(), epochDays(t, y, m, d); got != want {
			t.Fatalf("DateAt(%s).DateDays() = %d, want %d", src.Format(time.RFC3339), got, want)
		}
		back, err := ParseDate([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if !back.EqualScalar(v) {
			t.Fatalf("round trip of %q gave %q", v.Text(), back.Text())
		}
		if v.DateDays() < 0 {
			preEpoch++
		}
		if m == time.February && d == 29 {
			leapDays++
		}
	}
	if preEpoch == 0 || leapDays == 0 {
		t.Fatalf("one case went unexercised: %d pre-epoch dates, %d leap days", preEpoch, leapDays)
	}
}

// epochDays is the reference day count, taken from the time package's own
// epoch arithmetic rather than from ours.
func epochDays(t *testing.T, y int, m time.Month, d int) int64 {
	t.Helper()
	secs := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix()
	if secs%secondsPerDay != 0 {
		t.Fatalf("midnight UTC on %04d-%02d-%02d is not a whole number of days from the epoch", y, m, d)
	}
	return secs / secondsPerDay
}

func TestDateOrderingMatchesTheCalendar(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 8)) //nolint:gosec // deterministic test data, not security
	const minDay, maxDay = -718797, 2932532
	days := make([]int64, 120)
	for i := range days {
		days[i] = int64(minDay + r.Intn(maxDay-minDay))
	}
	for _, a := range days {
		for _, b := range days {
			got, ok := Compare(Date(a), Date(b))
			if !ok {
				t.Fatalf("Compare(%d, %d days) reported incomparable", a, b)
			}
			want := time.Unix(a*secondsPerDay, 0).UTC().Compare(time.Unix(b*secondsPerDay, 0).UTC())
			if got != want {
				t.Fatalf("Compare(%s, %s) = %d, time says %d", Date(a).Text(), Date(b).Text(), got, want)
			}
		}
	}
}

// TestLeapDaysAreTheOnesTheCalendarHas: February 29 exists in a leap year and
// does not in a century that is not one, and the kind must agree with time on
// both — including on how time *normalises* a date that does not exist, since
// silently disagreeing there would put a record on the wrong day.
func TestLeapDaysAreTheOnesTheCalendarHas(t *testing.T) {
	cases := []struct {
		y    int
		d    int
		want string
	}{
		{2024, 29, "2024-02-29"}, // ordinary leap year
		{2000, 29, "2000-02-29"}, // divisible by 400: still a leap year
		{1900, 29, "1900-03-01"}, // divisible by 100 but not 400: normalised by time
		{2023, 29, "2023-03-01"}, // not a leap year at all
		{1972, 29, "1972-02-29"}, // pre-epoch leap year
		{1600, 29, "1600-02-29"}, // and one well before the Gregorian cutover
	}
	for _, c := range cases {
		src := time.Date(c.y, time.February, c.d, 12, 0, 0, 0, time.UTC)
		if got, want := DateAt(src).Text(), src.Format(dateLayout); got != want {
			t.Errorf("DateAt(%d-02-%d) = %q, time says %q", c.y, c.d, got, want)
		}
		if got := DateOf(c.y, time.February, c.d).Text(); got != c.want {
			t.Errorf("DateOf(%d, February, %d) = %q, want %q", c.y, c.d, got, c.want)
		}
	}
}

// TestTimesOfDayMatchTheClock checks the time-of-day kind against clock
// arithmetic computed independently of AsTime, so the rendering is not being
// compared with itself.
func TestTimesOfDayMatchTheClock(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 9)) //nolint:gosec // deterministic test data, not security
	const dayNanos = int64(secondsPerDay) * int64(time.Second)
	var fractional int
	for i := range 5000 {
		var nanos int64
		switch i {
		case 0:
			nanos = 0 // midnight
		case 1:
			nanos = dayNanos - 1 // the last nanosecond of the day
		default:
			nanos = r.Int63n(dayNanos)
			if r.Intn(3) == 0 {
				nanos -= nanos % int64(time.Second) // whole seconds, the common case
			}
		}
		v := TimeOfDay(nanos)
		if got, want := v.Text(), clockText(nanos); got != want {
			t.Fatalf("TimeOfDay(%d).Text() = %q, want %q", nanos, got, want)
		}
		back, err := ParseTimeOfDay([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if got := back.DayNanos(); got != nanos {
			t.Fatalf("round trip of %q gave %d ns, want %d", v.Text(), got, nanos)
		}
		if nanos%int64(time.Second) != 0 {
			fractional++
		}
	}
	if fractional == 0 {
		t.Fatal("no sub-second times were generated")
	}
}

// clockText is the reference rendering: plain arithmetic on the nanosecond
// count, sharing nothing with AppendText's route through time.Time.
func clockText(nanos int64) string {
	h := nanos / int64(time.Hour)
	m := nanos % int64(time.Hour) / int64(time.Minute)
	s := nanos % int64(time.Minute) / int64(time.Second)
	frac := nanos % int64(time.Second)
	if frac != 0 {
		return fmt.Sprintf("%02d:%02d:%02d.%09d", h, m, s, frac)
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// TestADSTFoldStaysTwoDistinctInstants is why §2 stores an offset instead of a
// zone name, and why comparison uses the instant. During a fall-back the same
// local clock reading happens twice, an hour apart; the two must remain
// distinct, ordered values that each render with the offset they had.
func TestADSTFoldStaysTwoDistinctInstants(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("loading a zone from the embedded tz database: %v", err)
	}
	// 2026-11-01 01:30 local occurs at 05:30 UTC (EDT, -04:00) and again at
	// 06:30 UTC (EST, -05:00).
	early := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).In(ny)
	late := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC).In(ny)
	if early.Format("15:04") != late.Format("15:04") {
		t.Fatalf("this test needs an ambiguous local time; got %s and %s",
			early.Format(time.RFC3339), late.Format(time.RFC3339))
	}

	a, b := TimestampAt(early), TimestampAt(late)
	if got, want := a.ZoneOffset(), -4*time.Hour; got != want {
		t.Errorf("first reading offset = %v, want %v", got, want)
	}
	if got, want := b.ZoneOffset(), -5*time.Hour; got != want {
		t.Errorf("second reading offset = %v, want %v", got, want)
	}
	// Identical wall clocks, an hour apart as instants: a comparison on the
	// rendered local time would call these equal.
	if c, ok := Compare(a, b); !ok || c != -1 {
		t.Errorf("Compare = %d,%v; the first reading is an hour earlier", c, ok)
	}
	if a.EqualScalar(b) {
		t.Error("the two halves of a DST fold are not the same instant")
	}
	for _, v := range []Value{a, b} {
		back, err := ParseTimestamp([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if back.UnixNano() != v.UnixNano() || back.ZoneOffset() != v.ZoneOffset() {
			t.Errorf("round trip of %q lost the fold", v.Text())
		}
	}
}

// TestASpringForwardGapKeepsTheInstantTimeChose is the other half of a DST
// transition: 02:30 local does not exist on the forward day, so time.Date
// normalises it. Whatever instant time settles on is the instant the value
// must carry — inventing a different one would move a record by an hour.
func TestASpringForwardGapKeepsTheInstantTimeChose(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("loading a zone from the embedded tz database: %v", err)
	}
	gap := time.Date(2026, 3, 8, 2, 30, 0, 0, ny) // a local time that does not occur
	v := TimestampAt(gap)
	if got := v.UnixNano(); got != gap.UnixNano() {
		t.Errorf("UnixNano() = %d, want %d", got, gap.UnixNano())
	}
	if got, want := v.Text(), gap.Format(time.RFC3339Nano); got != want {
		t.Errorf("Text() = %q, time says %q", got, want)
	}
	// A date taken either side of the transition is still the day the source
	// saw, whichever way the clock jumped.
	for _, at := range []time.Time{gap.Add(-2 * time.Hour), gap, gap.Add(22 * time.Hour)} {
		if got, want := DateAt(at).Text(), at.Format(dateLayout); got != want {
			t.Errorf("DateAt(%s) = %q, time says %q", at.Format(time.RFC3339), got, want)
		}
	}
}

// TestTheEndsOfTheRepresentableRangeStillRoundTrip: the extremes are reachable
// by generation but worth naming, because they are where a signed conversion or
// a formatting assumption breaks and where a "nobody has data from 1677"
// argument stops being a reason not to check.
func TestTheEndsOfTheRepresentableRangeStillRoundTrip(t *testing.T) {
	for _, nanos := range []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64} {
		src := time.Unix(0, nanos).UTC()
		v := TimestampAt(src)
		if got := v.UnixNano(); got != nanos {
			t.Errorf("UnixNano() = %d, want %d", got, nanos)
		}
		back, err := ParseTimestamp([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if got := back.UnixNano(); got != nanos {
			t.Errorf("round trip of %q gave %d, want %d", v.Text(), got, nanos)
		}
	}
	// The date kind's practical range is what the YYYY-MM-DD layout can render:
	// year 1 to year 9999.
	for _, c := range []struct{ y, m, d int }{{1, 1, 1}, {9999, 12, 31}} {
		src := time.Date(c.y, time.Month(c.m), c.d, 0, 0, 0, 0, time.UTC)
		v := DateAt(src)
		if got, want := v.Text(), src.Format(dateLayout); got != want {
			t.Errorf("DateAt(%s) = %q, time says %q", src.Format(time.RFC3339), got, want)
		}
		if got, want := v.DateDays(), epochDays(t, c.y, time.Month(c.m), c.d); got != want {
			t.Errorf("DateDays() = %d, want %d", got, want)
		}
	}
	for _, nanos := range []int64{0, int64(secondsPerDay)*int64(time.Second) - 1} {
		v := TimeOfDay(nanos)
		back, err := ParseTimeOfDay([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if got := back.DayNanos(); got != nanos {
			t.Errorf("round trip of %q gave %d, want %d", v.Text(), got, nanos)
		}
	}
}

// FuzzTimestampRoundTrip extends the round trip to instants and offsets nobody
// chose. The offset is quantised deliberately: an arbitrary offset is a §2
// display rounding, which the seeded test above covers, whereas this target is
// about the instant surviving text.
func FuzzTimestampRoundTrip(f *testing.F) {
	f.Add(int64(0), int8(0))
	f.Add(int64(math.MaxInt64), int8(56)) // +14:00
	f.Add(int64(math.MinInt64), int8(-48))
	f.Add(int64(-1), int8(23)) // +05:45
	f.Fuzz(func(t *testing.T, nanos int64, units int8) {
		if units > 56 || units < -48 {
			t.Skip() // beyond any offset that exists, and beyond RFC 3339
		}
		off := time.Duration(units) * zoneUnit
		src := time.Unix(0, nanos).In(fixedAt(off))
		v := TimestampAt(src)
		if got := v.UnixNano(); got != nanos {
			t.Fatalf("UnixNano() = %d, want %d", got, nanos)
		}
		if got, want := v.Text(), src.Format(time.RFC3339Nano); got != want {
			t.Fatalf("Text() = %q, time says %q", got, want)
		}
		back, err := ParseTimestamp([]byte(v.Text()))
		if err != nil {
			t.Fatalf("re-parsing %q: %v", v.Text(), err)
		}
		if back.UnixNano() != nanos || back.ZoneOffset() != off {
			t.Fatalf("round trip of %q gave %d/%v, want %d/%v",
				v.Text(), back.UnixNano(), back.ZoneOffset(), nanos, off)
		}
		if c, ok := Compare(v, back); !ok || c != 0 {
			t.Fatalf("a value and its round trip do not compare equal: %d,%v", c, ok)
		}
	})
}
