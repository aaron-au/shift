package record

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// ExactSum accumulates exact numeric values (KindInt, KindDecimal) in 128 bits
// (ADR-0051 §3).
//
// It exists because an int64 coefficient overflows around 9.2×10^18, which is
// ample for one currency amount and not remotely ample for a SUM over a large
// batch — the exact operation a finance-shaped flow performs. Accumulating in
// float64 instead, which is what the aggregate did before (issue #4), loses
// precision above 2^53 and does it silently.
//
// A sum that cannot be represented is reported as an error by Value. It is not
// saturated and not wrapped, because both of those are indistinguishable from
// a correct total once they reach the next system.
type ExactSum struct {
	// Hi and Lo are the running magnitude, most significant word first; Neg is
	// its sign and Scale its decimal scale. The fields are exported so that
	// accumulator state can be spilled to scratch and restored; prefer the
	// methods for everything else.
	Hi, Lo uint64
	Neg    bool
	Scale  int8
}

// ErrSumOverflow reports a sum too large to represent.
var ErrSumOverflow = errors.New("exact sum overflows")

// Add folds an exact numeric value (KindInt or KindDecimal) into the sum.
// Any other kind is an error: a float has no exact decimal, and mixing one in
// is a decision for the caller to make explicitly.
func (s *ExactSum) Add(v Value) error {
	coef, scale, ok := v.ExactDecimal()
	if !ok {
		return fmt.Errorf("exact sum: %v is not an exact number", v.Kind())
	}
	return s.AddDecimal(coef, scale)
}

// AddDecimal folds coef × 10^-scale into the sum.
func (s *ExactSum) AddDecimal(coef int64, scale int8) error {
	return s.AddSum(ExactSum{Lo: magnitude(coef), Neg: coef < 0, Scale: scale})
}

// AddSum folds another accumulator's total into this one, which is what merging
// spilled aggregate partitions needs: a partial total can already exceed an
// int64 coefficient even when the final answer will not.
func (s *ExactSum) AddSum(o ExactSum) error {
	hi, lo := o.Hi, o.Lo
	if s.IsZero() && o.Scale < s.Scale {
		// Nothing has been accumulated, so there are no digits to preserve.
		// Adopting the coarser scale avoids scaling the incoming value up into
		// an overflow it does not deserve — 10^128 cannot be held at scale 0,
		// but it is a perfectly ordinary value at scale -128.
		s.Scale = o.Scale
	}
	// Otherwise align to the finer of the two scales, so no digit written by
	// any input is dropped. The accumulator's scale then only ever rises.
	switch {
	case o.Scale > s.Scale:
		if err := s.rescale(o.Scale); err != nil {
			return err
		}
	case o.Scale < s.Scale:
		var err error
		if hi, lo, err = mul10(hi, lo, int(s.Scale)-int(o.Scale)); err != nil {
			return err
		}
	}
	return s.addMagnitude(hi, lo, o.Neg)
}

// AddFloat is deliberately absent: a float cannot join an exact sum without
// making it inexact, so the caller switches accumulators instead (see
// stream.accum).

// rescale raises the accumulated magnitude to a finer scale.
func (s *ExactSum) rescale(to int8) error {
	hi, lo, err := mul10(s.Hi, s.Lo, int(to)-int(s.Scale))
	if err != nil {
		return err
	}
	s.Hi, s.Lo, s.Scale = hi, lo, to
	return nil
}

// addMagnitude adds a signed 128-bit magnitude to the running total.
func (s *ExactSum) addMagnitude(hi, lo uint64, neg bool) error {
	if s.Neg == neg {
		nhi, nlo, carry := add128(s.Hi, s.Lo, hi, lo)
		if carry != 0 {
			return fmt.Errorf("%w: sum exceeds 128 bits", ErrSumOverflow)
		}
		s.Hi, s.Lo = nhi, nlo
		return nil
	}
	// Opposite signs: the larger magnitude keeps its sign.
	if cmp128(s.Hi, s.Lo, hi, lo) >= 0 {
		s.Hi, s.Lo = sub128(s.Hi, s.Lo, hi, lo)
		if s.Hi == 0 && s.Lo == 0 {
			s.Neg = false // no negative zero
		}
		return nil
	}
	s.Hi, s.Lo = sub128(hi, lo, s.Hi, s.Lo)
	s.Neg = neg
	return nil
}

// IsZero reports whether the accumulated total is exactly zero.
func (s ExactSum) IsZero() bool { return s.Hi == 0 && s.Lo == 0 }

// Value returns the sum as a Value: an Int when nothing finer than whole units
// was ever added, a Decimal otherwise.
//
// A magnitude too wide for an int64 coefficient is first retried at a coarser
// scale, because a total like 10 accumulated from values written to 18 decimal
// places is representable even though 10×10^18 is not. Only a total that
// survives that is reported as an overflow.
func (s ExactSum) Value() (Value, error) {
	hi, lo, scale := s.Hi, s.Lo, s.Scale
	for hi != 0 || !fitsInt64(lo, s.Neg) {
		if scale <= 0 {
			return Value{}, fmt.Errorf("%w: total does not fit an int64 coefficient", ErrSumOverflow)
		}
		qhi, qlo, rem := div10(hi, lo)
		if rem != 0 {
			return Value{}, fmt.Errorf("%w: total needs more than 19 significant digits", ErrSumOverflow)
		}
		hi, lo, scale = qhi, qlo, scale-1
	}
	coef, err := signedCoefficient(lo, s.Neg)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %w", ErrSumOverflow, err)
	}
	if scale == 0 {
		return Int(coef), nil
	}
	return Decimal(coef, scale), nil
}

// Float returns the sum widened to float64, for the case where a float has
// joined the column and exactness has already been surrendered.
func (s ExactSum) Float() float64 {
	f := float64(s.Hi)*math.Pow(2, 64) + float64(s.Lo)
	if s.Neg {
		f = -f
	}
	return f * math.Pow10(-int(s.Scale))
}

// fitsInt64 reports whether a magnitude fits an int64 of the given sign.
func fitsInt64(mag uint64, neg bool) bool {
	if neg {
		return mag <= uint64(math.MaxInt64)+1
	}
	return mag <= uint64(math.MaxInt64)
}

// --- 128-bit helpers -------------------------------------------------------

func add128(hi1, lo1, hi2, lo2 uint64) (hi, lo, carry uint64) {
	lo, c := bits.Add64(lo1, lo2, 0)
	hi, carry = bits.Add64(hi1, hi2, c)
	return hi, lo, carry
}

// sub128 subtracts the second magnitude from the first, which must be the
// larger or equal of the two.
func sub128(hi1, lo1, hi2, lo2 uint64) (hi, lo uint64) {
	lo, b := bits.Sub64(lo1, lo2, 0)
	hi, _ = bits.Sub64(hi1, hi2, b)
	return hi, lo
}

func cmp128(hi1, lo1, hi2, lo2 uint64) int {
	if hi1 != hi2 {
		return compareUint64(hi1, hi2)
	}
	return compareUint64(lo1, lo2)
}

// mul10 multiplies a 128-bit magnitude by 10^d, in chunks of the largest power
// of ten a uint64 holds.
func mul10(hi, lo uint64, d int) (uint64, uint64, error) {
	if d < 0 {
		return 0, 0, fmt.Errorf("record: mul10 with negative exponent %d", d)
	}
	if hi == 0 && lo == 0 {
		return 0, 0, nil // zero scales to zero at any exponent
	}
	for d > 0 {
		step := min(d, maxDecimalDigits)
		p := pow10[step]
		h1, l1 := bits.Mul64(lo, p)
		h2, l2 := bits.Mul64(hi, p)
		if h2 != 0 {
			return 0, 0, fmt.Errorf("%w: scaling by 10^%d exceeds 128 bits", ErrSumOverflow, d)
		}
		nhi, c := bits.Add64(h1, l2, 0)
		if c != 0 {
			return 0, 0, fmt.Errorf("%w: scaling by 10^%d exceeds 128 bits", ErrSumOverflow, d)
		}
		hi, lo = nhi, l1
		d -= step
	}
	return hi, lo, nil
}

// div10 divides a 128-bit magnitude by ten, returning the quotient and the
// remainder.
func div10(hi, lo uint64) (qhi, qlo, rem uint64) {
	qhi = hi / 10
	r := hi % 10
	qlo, rem = bits.Div64(r, lo, 10)
	return qhi, qlo, rem
}
