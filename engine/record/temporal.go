package record

import (
	"fmt"
	"strconv"
	"time"
)

// Native temporal values (ADR-0051 §1/§2). Three kinds, because a source
// distinguishes three things:
//
//   - KindTimestamp — an instant, held as unix nanoseconds, with the original
//     zone offset in aux purely so it renders back the way it arrived.
//   - KindDate      — a calendar day with no time, held as days since the
//     Unix epoch.
//   - KindTime      — a time of day with no date, held as nanoseconds since
//     midnight. A bare HHMMSS field in a fixed-width or EDI record is exactly
//     this; modelling it as a timestamp would invent a day the source never
//     stated.
//
// The instant is always exact. Only the offset is quantised (§2).

// secondsPerDay is the length of a calendar day as the Date kind counts them:
// 86400 exactly, with no leap seconds, matching time.Time's own epoch
// arithmetic.
const secondsPerDay = 24 * 60 * 60

// zoneUnit is the resolution of the stored zone offset. 15 minutes in an int8
// covers ±31.75 hours, which is every offset in use.
const zoneUnit = 15 * time.Minute

// Timestamp returns an instant with an explicit zone offset for rendering.
// The offset is rounded to the nearest 15 minutes (ADR-0051 §2) — a display
// concern only, since unixNano fixes the instant.
func Timestamp(unixNano int64, zoneOffset time.Duration) Value {
	return Value{kind: KindTimestamp, aux: offsetUnits(zoneOffset), num: uint64(unixNano)} //nolint:gosec // deliberate bit-store; UnixNano() reverses it
}

// TimestampAt returns an instant taken from t, preserving t's zone offset.
func TimestampAt(t time.Time) Value {
	_, offset := t.Zone()
	return Timestamp(t.UnixNano(), time.Duration(offset)*time.Second)
}

// Date returns a calendar date as whole days since 1970-01-01.
func Date(days int64) Value {
	return Value{kind: KindDate, num: uint64(days)} //nolint:gosec // deliberate bit-store; DateDays() reverses it
}

// DateAt returns the calendar date of t in t's own zone — the date a person
// reading the source would see, not the date it happens to be in UTC.
func DateAt(t time.Time) Value {
	_, offset := t.Zone()
	secs := t.Unix() + int64(offset)
	days := secs / secondsPerDay
	if secs < 0 && secs%secondsPerDay != 0 {
		days-- // floor, so dates before the epoch do not round toward it
	}
	return Date(days)
}

// DateOf returns a calendar date from its components.
func DateOf(year int, month time.Month, day int) Value {
	return DateAt(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

// TimeOfDay returns a time of day as nanoseconds since midnight.
func TimeOfDay(nanos int64) Value {
	return Value{kind: KindTime, num: uint64(nanos)} //nolint:gosec // deliberate bit-store; DayNanos() reverses it
}

// UnixNano returns the instant of a timestamp (0 unless KindTimestamp).
func (v Value) UnixNano() int64 {
	if v.kind != KindTimestamp {
		return 0
	}
	return int64(v.num) //nolint:gosec // reverses the bit-store in Timestamp()
}

// ZoneOffset returns the stored zone offset of a timestamp, quantised to 15
// minutes (0 unless KindTimestamp).
func (v Value) ZoneOffset() time.Duration {
	if v.kind != KindTimestamp {
		return 0
	}
	return time.Duration(v.aux) * zoneUnit
}

// DateDays returns days since the Unix epoch (0 unless KindDate).
func (v Value) DateDays() int64 {
	if v.kind != KindDate {
		return 0
	}
	return int64(v.num) //nolint:gosec // reverses the bit-store in Date()
}

// DayNanos returns nanoseconds since midnight (0 unless KindTime).
func (v Value) DayNanos() int64 {
	if v.kind != KindTime {
		return 0
	}
	return int64(v.num) //nolint:gosec // reverses the bit-store in TimeOfDay()
}

// AsTime converts a temporal value to a time.Time: a timestamp keeps its
// instant and stored offset, a date becomes midnight UTC on that day, and a
// time of day becomes that offset into the epoch day. The zero time is
// returned for every other kind.
//
// A date's midnight and a time-of-day's epoch day are placeholders the kinds
// themselves do not carry, so this is a convenience for interoperating with
// the standard library, not a statement about the value.
func (v Value) AsTime() time.Time {
	switch v.kind {
	case KindTimestamp:
		off := int(v.ZoneOffset() / time.Second)
		return time.Unix(0, v.UnixNano()).In(fixedZone(off))
	case KindDate:
		return time.Unix(v.DateDays()*secondsPerDay, 0).UTC()
	case KindTime:
		return time.Unix(0, v.DayNanos()).UTC()
	default:
		return time.Time{}
	}
}

// fixedZone avoids allocating a Location for the overwhelmingly common case.
func fixedZone(offsetSeconds int) *time.Location {
	if offsetSeconds == 0 {
		return time.UTC
	}
	return time.FixedZone("", offsetSeconds)
}

// offsetUnits quantises a zone offset to 15-minute units, rounding to nearest
// and clamping to the int8 range.
func offsetUnits(off time.Duration) int8 {
	units := (off + zoneUnit/2) / zoneUnit
	if off < 0 {
		units = (off - zoneUnit/2) / zoneUnit // round away from zero, symmetrically
	}
	switch {
	case units > 127:
		return 127
	case units < -128:
		return -128
	}
	return int8(units)
}

// Canonical text layouts. Dates and times are ISO 8601 with no zone, because
// neither kind carries one.
const (
	dateLayout      = "2006-01-02"
	timeLayout      = "15:04:05"
	timeNanosLayout = "15:04:05.000000000"
)

// AppendText appends the canonical text of an exact numeric or temporal value
// (int, decimal, timestamp, date, time) to dst and returns the extended slice.
// Other kinds append nothing.
//
// It exists so the format writers and the string coercion share one rendering
// rather than each deciding independently how a timestamp looks. Its scope is
// the kinds that have exactly one correct text form wherever they are written;
// KindFloat is deliberately excluded, because each format constrains floats
// differently (JSON cannot spell NaN, XML has no notation for infinity), so
// that rendering stays with the writer that owns the constraint.
func (v Value) AppendText(dst []byte) []byte {
	switch v.kind {
	case KindInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case KindDecimal:
		return v.AppendDecimal(dst)
	case KindTimestamp:
		return v.AsTime().AppendFormat(dst, time.RFC3339Nano)
	case KindDate:
		return v.AsTime().AppendFormat(dst, dateLayout)
	case KindTime:
		if v.DayNanos()%int64(time.Second) != 0 {
			return v.AsTime().AppendFormat(dst, timeNanosLayout)
		}
		return v.AsTime().AppendFormat(dst, timeLayout)
	default:
		return dst
	}
}

// Text returns the canonical text of an exact numeric or temporal value ("" for
// other kinds — see AppendText). Use AppendText on hot paths.
func (v Value) Text() string {
	var buf [40]byte
	return string(v.AppendText(buf[:0]))
}

// ParseTimestamp parses an RFC 3339 instant, keeping its written offset.
func ParseTimestamp(s []byte) (Value, error) {
	t, err := time.Parse(time.RFC3339Nano, string(s))
	if err != nil {
		return Value{}, fmt.Errorf("not an RFC 3339 timestamp: %q", s)
	}
	return TimestampAt(t), nil
}

// ParseDate parses a YYYY-MM-DD calendar date.
func ParseDate(s []byte) (Value, error) {
	t, err := time.Parse(dateLayout, string(s))
	if err != nil {
		return Value{}, fmt.Errorf("not a YYYY-MM-DD date: %q", s)
	}
	return DateAt(t), nil
}

// ParseTimeOfDay parses HH:MM:SS with an optional fractional second.
func ParseTimeOfDay(s []byte) (Value, error) {
	t, err := time.Parse(timeLayout, string(s))
	if err != nil {
		if t, err = time.Parse("15:04:05.999999999", string(s)); err != nil {
			return Value{}, fmt.Errorf("not an HH:MM:SS time: %q", s)
		}
	}
	h, m, sec := t.Clock()
	nanos := int64(h)*int64(time.Hour) + int64(m)*int64(time.Minute) +
		int64(sec)*int64(time.Second) + int64(t.Nanosecond())
	return TimeOfDay(nanos), nil
}
