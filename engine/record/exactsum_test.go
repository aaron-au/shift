package record

import (
	"errors"
	"math"
	"testing"
)

// TestSummingIntegersStaysExactPast2To53 is the regression test for issue #4:
// the aggregate accumulated in float64, where 2^53+1 does not exist, so a sum
// of large integers came back quietly wrong.
func TestSummingIntegersStaysExactPast2To53(t *testing.T) {
	const big = 1<<53 + 1 // the smallest integer float64 cannot represent

	var s ExactSum
	for range 2 {
		if err := s.Add(Int(big)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind() != KindInt {
		t.Errorf("a sum of ints is an int, got %v", got.Kind())
	}
	if got.Int() != 2*big {
		t.Errorf("sum = %d, want %d", got.Int(), int64(2*big))
	}
	// What the float64 accumulator produced instead, and why it was a defect:
	// the float sum, converted back, is not the true total. Compared against
	// the exact integer rather than against float64(2*big), which rounds to
	// the same wrong value and would hide the imprecision.
	f := float64(big) // through a variable: constant arithmetic is exact
	if int64(f+f) == int64(2*big) {
		t.Error("float64 unexpectedly summed these exactly; the premise of this test needs revisiting")
	}
}

func TestSummingDecimalsAlignsScalesWithoutLosingDigits(t *testing.T) {
	cases := []struct {
		name string
		add  []Value
		want string
	}{
		{"tenths", []Value{Decimal(1, 1), Decimal(2, 1)}, "0.3"},
		{"mixed scales keep the finer one", []Value{Decimal(10, 2), Decimal(2, 1)}, "0.30"},
		{"ints join decimals exactly", []Value{Int(1), Decimal(50, 2)}, "1.50"},
		{"currency column", []Value{Decimal(1010, 2), Decimal(9990, 2), Decimal(1, 2)}, "110.01"},
		{"negatives cancel to a positive zero", []Value{Decimal(5, 1), Decimal(-5, 1)}, "0.0"},
		{"crossing zero downward", []Value{Decimal(3, 0), Decimal(-5, 0)}, "-2"},
		{"crossing zero upward", []Value{Decimal(-3, 0), Decimal(5, 0)}, "2"},
		{"negative scales", []Value{Decimal(1, -3), Decimal(1, -3)}, "2000"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s ExactSum
			for _, v := range c.add {
				if err := s.Add(v); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.Value()
			if err != nil {
				t.Fatal(err)
			}
			if text := got.Text(); text != c.want {
				t.Errorf("sum = %q, want %q", text, c.want)
			}
		})
	}
}

// TestASumTooWideForItsScaleIsRetriedCoarser: ten values written to 18 decimal
// places total 10, which is representable even though 10×10^18 is not.
func TestASumTooWideForItsScaleIsRetriedCoarser(t *testing.T) {
	var s ExactSum
	for range 10 {
		if err := s.Add(Decimal(1_000_000_000_000_000_000, 18)); err != nil { // 1.000000000000000000
			t.Fatal(err)
		}
	}
	got, err := s.Value()
	if err != nil {
		t.Fatalf("a total of 10 must be representable: %v", err)
	}
	if c, ok := Compare(got, Int(10)); !ok || c != 0 {
		t.Errorf("sum = %s, want 10", got.Text())
	}

	// Twenty of them carries past 64 bits into the high word, and must still
	// come back as 20.
	var s2 ExactSum
	for range 20 {
		if err := s2.Add(Decimal(1_000_000_000_000_000_000, 18)); err != nil {
			t.Fatal(err)
		}
	}
	if s2.Hi == 0 {
		t.Fatal("this case is meant to exercise the high word; it no longer does")
	}
	got2, err := s2.Value()
	if err != nil {
		t.Fatalf("a total of 20 must be representable: %v", err)
	}
	if c, ok := Compare(got2, Int(20)); !ok || c != 0 {
		t.Errorf("sum = %s, want 20", got2.Text())
	}
}

// TestAnUnrepresentableTotalIsAnErrorNotAWrap is ADR-0051 §3: saturating or
// wrapping would be indistinguishable from a correct total downstream.
func TestAnUnrepresentableTotalIsAnErrorNotAWrap(t *testing.T) {
	var s ExactSum
	for range 2 {
		if err := s.Add(Int(math.MaxInt64)); err != nil {
			t.Fatal(err)
		}
	}
	// The accumulator holds the true total in 128 bits; it is only the int64
	// coefficient that cannot express it.
	if s.Lo != 2*uint64(math.MaxInt64) || s.Hi != 0 {
		t.Errorf("128-bit accumulation wrong: hi=%d lo=%d", s.Hi, s.Lo)
	}
	if _, err := s.Value(); !errors.Is(err, ErrSumOverflow) {
		t.Errorf("Value() error = %v, want ErrSumOverflow", err)
	}
}

func TestScalingPastOneHundredTwentyEightBitsIsAnError(t *testing.T) {
	var s ExactSum
	if err := s.Add(Decimal(1, -128)); err != nil { // 10^128
		t.Fatal(err)
	}
	// Aligning this with a value at scale 127 would need 10^255.
	if err := s.Add(Decimal(1, 127)); !errors.Is(err, ErrSumOverflow) {
		t.Errorf("Add error = %v, want ErrSumOverflow", err)
	}
}

func TestSummingRejectsWhatIsNotExact(t *testing.T) {
	var s ExactSum
	for _, v := range []Value{Float(1.5), UnsafeString([]byte("1")), Null(), Bool(true)} {
		if err := s.Add(v); err == nil {
			t.Errorf("Add(%v) accepted; only exact numbers belong in an exact sum", v.Kind())
		}
	}
}

func TestAnEmptySumIsZero(t *testing.T) {
	var s ExactSum
	if !s.IsZero() {
		t.Error("a fresh accumulator must be zero")
	}
	got, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind() != KindInt || got.Int() != 0 {
		t.Errorf("empty sum = %v %s, want int 0", got.Kind(), got.Text())
	}
}

func TestNegativeZeroIsNormalisedAway(t *testing.T) {
	var s ExactSum
	if err := s.Add(Int(-5)); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(Int(5)); err != nil {
		t.Fatal(err)
	}
	if !s.IsZero() || s.Neg {
		t.Errorf("expected a plain zero, got neg=%v hi=%d lo=%d", s.Neg, s.Hi, s.Lo)
	}
	got, _ := s.Value()
	if got.Int() != 0 {
		t.Errorf("sum = %d, want 0", got.Int())
	}
}

func TestSumWidensToFloatWhenExactnessIsAlreadyLost(t *testing.T) {
	var s ExactSum
	if err := s.Add(Decimal(1050, 2)); err != nil { // 10.50
		t.Fatal(err)
	}
	if got := s.Float(); math.Abs(got-10.5) > 1e-12 {
		t.Errorf("Float() = %v, want ~10.5", got)
	}
	// The high word must contribute, not be dropped.
	big := ExactSum{Hi: 1, Lo: 0}
	if got := big.Float(); math.Abs(got-math.Pow(2, 64)) > 1e6 {
		t.Errorf("Float() with a high word = %v, want ~2^64", got)
	}
	neg := ExactSum{Lo: 5, Neg: true}
	if got := neg.Float(); got != -5 {
		t.Errorf("Float() = %v, want -5", got)
	}
}

func TestExactDecimalViewRejectsFloats(t *testing.T) {
	if _, _, ok := Float(1.5).ExactDecimal(); ok {
		t.Error("a float has no exact decimal view")
	}
	coef, scale, ok := Int(7).ExactDecimal()
	if !ok || coef != 7 || scale != 0 {
		t.Errorf("Int(7).ExactDecimal() = %d,%d,%v", coef, scale, ok)
	}
	coef, scale, ok = Decimal(75, 1).ExactDecimal()
	if !ok || coef != 75 || scale != 1 {
		t.Errorf("Decimal(75,1).ExactDecimal() = %d,%d,%v", coef, scale, ok)
	}
}

// TestSumStateRoundTripsThroughItsFields covers the reason the fields are
// exported: aggregate state spills to scratch and comes back.
func TestSumStateRoundTripsThroughItsFields(t *testing.T) {
	var s ExactSum
	for _, v := range []Value{Decimal(1010, 2), Decimal(-5, 3), Int(7)} {
		if err := s.Add(v); err != nil {
			t.Fatal(err)
		}
	}
	restored := ExactSum{Hi: s.Hi, Lo: s.Lo, Neg: s.Neg, Scale: s.Scale}
	a, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	b, err := restored.Value()
	if err != nil {
		t.Fatal(err)
	}
	if a.Text() != b.Text() {
		t.Errorf("restored sum = %q, want %q", b.Text(), a.Text())
	}
	// Adding to the restored copy must continue from the same total.
	if err := restored.Add(Int(1)); err != nil {
		t.Fatal(err)
	}
	c, err := restored.Value()
	if err != nil {
		t.Fatal(err)
	}
	if cmp, ok := Compare(c, a); !ok || cmp != 1 {
		t.Errorf("continuing the restored sum did not increase it: %q vs %q", c.Text(), a.Text())
	}
}
