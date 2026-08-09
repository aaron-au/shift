package record

import (
	"errors"
	"math"
	"testing"
	"unsafe"
)

// TestValueStaysEightyEightBytes guards the premise of ADR-0051 §1: the aux
// byte is free because it lands in the padding after kind. If a future field
// makes the struct grow, every value in every batch costs more memory and the
// zero-alloc steady state is quietly more expensive — so this is asserted
// rather than assumed.
func TestValueStaysEightyEightBytes(t *testing.T) {
	if got := unsafe.Sizeof(Value{}); got != valueSize {
		t.Fatalf("unsafe.Sizeof(Value{}) = %d, want %d (valueSize, used for memory accounting)", got, valueSize)
	}
}

// TestADecimalSurvivesWhatAFloatWouldRound is the reason the kind exists: a
// 2-decimal currency amount must come out the way it went in.
func TestADecimalSurvivesWhatAFloatWouldRound(t *testing.T) {
	v := Decimal(1010, 2)
	if got := v.Text(); got != "10.10" {
		t.Errorf("Text() = %q, want 10.10", got)
	}
	coef, scale := v.Decimal()
	if coef != 1010 || scale != 2 {
		t.Errorf("Decimal() = %d,%d, want 1010,2", coef, scale)
	}
}

func TestDecimalTextIsExactAtEveryScale(t *testing.T) {
	cases := []struct {
		coef  int64
		scale int8
		want  string
	}{
		{0, 0, "0"},
		{0, 2, "0.00"},
		{1010, 2, "10.10"},   // trailing zero kept: the scale is the source's claim
		{5, 3, "0.005"},      // fewer digits than the scale
		{-5, 3, "-0.005"},    // and negative
		{-1234, 2, "-12.34"}, //
		{101, -3, "101000"},  // negative scale multiplies
		{-101, -3, "-101000"},
		{math.MaxInt64, 0, "9223372036854775807"},
		{math.MinInt64, 0, "-9223372036854775808"},
		// MinInt64 has no positive counterpart, so formatting it via negation
		// would overflow; magnitude() is why this works.
		{math.MinInt64, 2, "-92233720368547758.08"},
	}
	for _, c := range cases {
		if got := string(AppendDecimal(nil, c.coef, c.scale)); got != c.want {
			t.Errorf("AppendDecimal(%d, %d) = %q, want %q", c.coef, c.scale, got, c.want)
		}
	}
}

func TestParseDecimalKeepsTheWrittenScale(t *testing.T) {
	cases := []struct {
		in    string
		coef  int64
		scale int8
	}{
		{"0", 0, 0},
		{"10.10", 1010, 2}, // not 101/1: trailing zeros are information
		{"10.100", 10100, 3},
		{"-12.34", -1234, 2},
		{"+12.34", 1234, 2},
		{".5", 5, 1},
		{"1e5", 1, -5},    // exponent adjusts the scale, not the digits
		{"1.5e3", 15, -2}, // 1500
		{"1.5e-3", 15, 4}, // 0.0015
		{"-9223372036854775808", math.MinInt64, 0}, // the exact int64 floor
	}
	for _, c := range cases {
		v, err := ParseDecimal([]byte(c.in))
		if err != nil {
			t.Errorf("ParseDecimal(%q): %v", c.in, err)
			continue
		}
		coef, scale := v.Decimal()
		if coef != c.coef || scale != c.scale {
			t.Errorf("ParseDecimal(%q) = %d,%d, want %d,%d", c.in, coef, scale, c.coef, c.scale)
		}
	}
}

func TestParseDecimalRejectsWhatItCannotRepresentExactly(t *testing.T) {
	// Rejected for range: representing these would need a wider coefficient or
	// scale, and a silently rounded answer is the defect this kind exists to
	// prevent.
	forRange := []string{
		"99999999999999999999999", // more digits than uint64 holds
		"9223372036854775808",     // one past MaxInt64
		"-9223372036854775809",    // one past MinInt64
		"1e-200",                  // scale past int8
		"1e200",                   // and past it the other way
	}
	for _, in := range forRange {
		if _, err := ParseDecimal([]byte(in)); !errors.Is(err, ErrDecimalRange) {
			t.Errorf("ParseDecimal(%q) error = %v, want ErrDecimalRange", in, err)
		}
	}
	// Rejected as malformed.
	malformed := []string{"", "-", "+", ".", "abc", "1.2.3", "1e", "1e+", "1 ", " 1", "1,000"}
	for _, in := range malformed {
		if _, err := ParseDecimal([]byte(in)); err == nil {
			t.Errorf("ParseDecimal(%q) accepted, want an error", in)
		} else if errors.Is(err, ErrDecimalRange) {
			t.Errorf("ParseDecimal(%q) reported a range error for malformed input: %v", in, err)
		}
	}
}

func TestDecimalsCompareExactlyAcrossScales(t *testing.T) {
	cases := []struct {
		a, b Value
		want int
	}{
		{Decimal(1, 1), Decimal(10, 2), 0}, // 0.1 == 0.10
		{Decimal(0, 0), Decimal(0, 9), 0},  // zero is zero at any scale
		{Decimal(0, 0), Decimal(0, -9), 0}, //
		{Decimal(1010, 2), Decimal(101, 1), 0},
		{Decimal(1011, 2), Decimal(101, 1), 1},    // 10.11 > 10.1
		{Decimal(-1011, 2), Decimal(-101, 1), -1}, // ordering inverts for negatives
		{Decimal(-1, 0), Decimal(1, 0), -1},
		{Decimal(-1, 0), Decimal(0, 5), -1},
		{Int(10), Decimal(1000, 2), 0}, // an int is a decimal with scale 0
		{Int(11), Decimal(1000, 2), 1},
		{Decimal(1000, 2), Int(11), -1},
		// A scale gap wide enough that rescaling into an int64 would overflow.
		// The comparison must still be decided, and decided correctly.
		{Decimal(1, 0), Decimal(math.MaxInt64, 30), 1},
		{Decimal(math.MaxInt64, 30), Decimal(1, 0), -1},
		{Decimal(1, -100), Decimal(1, 27), 1},
		{Decimal(-1, -100), Decimal(1, 27), -1},
		// Just inside the 64-bit multiply, where the shortcut does not apply.
		{Decimal(2, 0), Decimal(19, 1), 1}, // 2 > 1.9
		{Decimal(2, 0), Decimal(21, 1), -1},
	}
	for _, c := range cases {
		got, ok := Compare(c.a, c.b)
		if !ok {
			t.Errorf("Compare(%s, %s) reported incomparable", c.a.Text(), c.b.Text())
			continue
		}
		if got != c.want {
			t.Errorf("Compare(%s, %s) = %d, want %d", c.a.Text(), c.b.Text(), got, c.want)
		}
		if eq := c.a.EqualScalar(c.b); eq != (c.want == 0) {
			t.Errorf("EqualScalar(%s, %s) = %v, want %v", c.a.Text(), c.b.Text(), eq, c.want == 0)
		}
	}
}

// TestTheExactPathIsNotJustFloatComparisonInDisguise pins the specific
// difference: 0.1 + 0.2 != 0.3 in float64, and the decimal path must not
// inherit that.
func TestTheExactPathIsNotJustFloatComparisonInDisguise(t *testing.T) {
	a, b := Decimal(1, 1), Decimal(2, 1) // 0.1 and 0.2
	ac, _ := a.Decimal()
	bc, _ := b.Decimal()
	if c, ok := Compare(Decimal(ac+bc, 1), Decimal(3, 1)); !ok || c != 0 {
		t.Fatalf("0.1+0.2 == 0.3 exactly: Compare = %d,%v", c, ok)
	}
	// The float64 counterpart, through variables so the compiler cannot fold
	// it into exact constant arithmetic.
	af, bf, want := a.Float(), b.Float(), 0.3
	if af+bf == want {
		t.Error("float64 unexpectedly made 0.1+0.2 == 0.3; the premise of this test needs revisiting")
	}
}

func TestAFloatInTheComparisonMakesItAFloatComparison(t *testing.T) {
	// Documented as inexact (ADR-0051 §4): the point is that it still answers,
	// rather than failing a flow for a reason its author cannot act on.
	if c, ok := Compare(Decimal(1010, 2), Float(10.10)); !ok || c != 0 {
		t.Errorf("decimal vs float = %d,%v, want 0,true", c, ok)
	}
	if c, ok := Compare(Float(10.5), Decimal(1010, 2)); !ok || c != 1 {
		t.Errorf("float vs decimal = %d,%v, want 1,true", c, ok)
	}
	if c, ok := Compare(Int(3), Float(3.5)); !ok || c != -1 {
		t.Errorf("int vs float = %d,%v, want -1,true", c, ok)
	}
}

func TestNaNIsReportedAsUnorderedRatherThanGuessed(t *testing.T) {
	nan := Float(math.NaN())
	for _, other := range []Value{nan, Float(1), Int(1), Decimal(1, 0)} {
		if _, ok := Compare(nan, other); ok {
			t.Errorf("Compare(NaN, %v) claimed an ordering", other.Kind())
		}
		if _, ok := Compare(other, nan); ok {
			t.Errorf("Compare(%v, NaN) claimed an ordering", other.Kind())
		}
	}
	if nan.EqualScalar(nan) {
		t.Error("NaN must not equal itself")
	}
}

func TestDecimalWidensToFloatOnlyWhenAsked(t *testing.T) {
	if got := Decimal(1010, 2).Float(); got != 10.10 {
		t.Errorf("Float() = %v, want 10.10", got)
	}
	if !Decimal(1, 0).IsNumeric() || !Int(1).IsNumeric() || !Float(1).IsNumeric() {
		t.Error("int, float and decimal must all report IsNumeric")
	}
	if UnsafeString([]byte("1")).IsNumeric() {
		t.Error("a string is not numeric, whatever it contains")
	}
}

// TestAccessorsReadZeroForTheWrongKind keeps a skipped kind check from
// reinterpreting another kind's payload as a coefficient.
func TestAccessorsReadZeroForTheWrongKind(t *testing.T) {
	v := Float(1.5)
	if coef, scale := v.Decimal(); coef != 0 || scale != 0 {
		t.Errorf("Decimal() on a float = %d,%d, want 0,0", coef, scale)
	}
	if got := v.DecimalText(); got != "" {
		t.Errorf("DecimalText() on a float = %q, want empty", got)
	}
	if got := string(v.AppendDecimal([]byte("keep"))); got != "keep" {
		t.Errorf("AppendDecimal on a float appended %q", got)
	}
}
