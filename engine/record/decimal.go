package record

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strconv"
)

// Exact decimal values (ADR-0051 §1): a Value of KindDecimal is exactly
// coefficient × 10^-scale, with the coefficient in num and the scale in the
// aux byte that alignment padding was already paying for.
//
// Scale is signed. A negative scale means multiples of a power of ten, which
// is how "amount in thousands" columns in fixed-width and EDI records declare
// themselves; the alternative would be storing a coefficient the source never
// wrote.

// maxDecimalDigits is the number of decimal digits an int64 coefficient can
// hold (9223372036854775807 has 19). A scale gap of this size or more decides
// a magnitude comparison outright: the smaller-scaled operand, having a
// non-zero coefficient, exceeds any int64 magnitude once multiplied out.
const maxDecimalDigits = 19

// pow10 holds 10^0..10^19; 10^19 is the largest power of ten that fits a
// uint64 (max ≈ 1.8×10^19).
var pow10 = [maxDecimalDigits + 1]uint64{
	1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
}

// Decimal returns an exact decimal value equal to coef × 10^-scale.
func Decimal(coef int64, scale int8) Value {
	return Value{kind: KindDecimal, aux: scale, num: uint64(coef)} //nolint:gosec // deliberate bit-store; Decimal() reverses it
}

// Decimal returns the coefficient and scale of a decimal value. Both are 0
// unless the value is KindDecimal, so a caller that skipped the kind check
// reads zero rather than a reinterpretation of another kind's payload.
func (v Value) Decimal() (coef int64, scale int8) {
	if v.kind != KindDecimal {
		return 0, 0
	}
	return int64(v.num), v.aux //nolint:gosec // reverses the bit-store in Decimal()
}

// IsNumeric reports whether the value carries a number: an exact one
// (KindInt, KindDecimal) or an inexact one (KindFloat).
func (v Value) IsNumeric() bool {
	switch v.kind {
	case KindInt, KindFloat, KindDecimal:
		return true
	default:
		return false
	}
}

// AppendDecimal appends the exact decimal text of v to dst and returns the
// extended slice. It appends nothing if v is not a decimal.
//
// Exact means every digit the scale claims is printed, trailing zeros
// included: a value read as "10.10" renders as "10.10", because the scale is
// the source's own statement about precision and dropping it loses
// information a downstream system may be relying on.
func (v Value) AppendDecimal(dst []byte) []byte {
	if v.kind != KindDecimal {
		return dst
	}
	coef, scale := v.Decimal()
	return AppendDecimal(dst, coef, scale)
}

// AppendDecimal appends the exact text of coef × 10^-scale to dst.
func AppendDecimal(dst []byte, coef int64, scale int8) []byte {
	if scale == 0 {
		return strconv.AppendInt(dst, coef, 10)
	}
	neg := coef < 0
	if neg {
		dst = append(dst, '-')
	}
	// digits holds the coefficient magnitude; magnitude() is used rather than
	// negation so math.MinInt64 formats correctly.
	var buf [20]byte
	digits := strconv.AppendUint(buf[:0], magnitude(coef), 10)

	if scale < 0 {
		// A negative scale multiplies: append the zeros it stands for.
		dst = append(dst, digits...)
		for range -int(scale) {
			dst = append(dst, '0')
		}
		return dst
	}
	if n := int(scale) - len(digits); n >= 0 {
		// Fewer digits than the scale: 5 at scale 3 is 0.005.
		dst = append(dst, '0', '.')
		for range n {
			dst = append(dst, '0')
		}
		return append(dst, digits...)
	}
	split := len(digits) - int(scale)
	dst = append(dst, digits[:split]...)
	dst = append(dst, '.')
	return append(dst, digits[split:]...)
}

// DecimalText returns the exact decimal text of v ("" if v is not a decimal).
// Use AppendDecimal on hot paths.
func (v Value) DecimalText() string {
	if v.kind != KindDecimal {
		return ""
	}
	var buf [40]byte
	return string(v.AppendDecimal(buf[:0]))
}

// ErrDecimalRange reports a decimal that cannot be represented exactly: a
// coefficient wider than int64 or a scale wider than int8.
var ErrDecimalRange = errors.New("decimal out of range")

// ParseDecimal parses exact decimal text into a Value, preserving the written
// scale (so "10.10" has scale 2, not 1). It accepts an optional sign, digits
// with an optional fractional part, and an optional decimal exponent, which
// adjusts the scale rather than the digits.
//
// It does not trim: a caller holding a padded fixed-width field trims first,
// so that whitespace is a decision made where the format is known.
func ParseDecimal(s []byte) (Value, error) {
	i, neg := 0, false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		neg = s[0] == '-'
		i++
	}
	var mag uint64
	digits, frac := 0, 0
	seenPoint := false
scan:
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			// Overflow check before the multiply, so a long input is rejected
			// rather than wrapping into a plausible wrong answer.
			if mag > (math.MaxUint64-9)/10 {
				return Value{}, fmt.Errorf("%w: %q has too many digits", ErrDecimalRange, s)
			}
			mag = mag*10 + uint64(c-'0')
			digits++
			if seenPoint {
				frac++
			}
		case c == '.':
			if seenPoint {
				return Value{}, fmt.Errorf("not a decimal: %q", s)
			}
			seenPoint = true
		case c == 'e' || c == 'E':
			exp, err := strconv.Atoi(string(s[i+1:]))
			if err != nil {
				return Value{}, fmt.Errorf("not a decimal: %q", s)
			}
			frac -= exp
			break scan
		default:
			return Value{}, fmt.Errorf("not a decimal: %q", s)
		}
	}
	if digits == 0 {
		return Value{}, fmt.Errorf("not a decimal: %q", s)
	}
	if frac < math.MinInt8 || frac > math.MaxInt8 {
		return Value{}, fmt.Errorf("%w: %q needs scale %d", ErrDecimalRange, s, frac)
	}
	coef, err := signedCoefficient(mag, neg)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %q", err, s)
	}
	return Decimal(coef, int8(frac)), nil //nolint:gosec // range-checked above
}

// signedCoefficient applies neg to a magnitude, rejecting magnitudes outside
// int64. math.MinInt64 is representable and is accepted.
func signedCoefficient(mag uint64, neg bool) (int64, error) {
	if neg {
		if mag > uint64(math.MaxInt64)+1 {
			return 0, ErrDecimalRange
		}
		return -int64(mag-1) - 1, nil //nolint:gosec // mag-1 <= MaxInt64, so this is MinInt64 at worst
	}
	if mag > uint64(math.MaxInt64) {
		return 0, ErrDecimalRange
	}
	return int64(mag), nil //nolint:gosec // range-checked immediately above
}

// magnitude returns |v| as a uint64, correctly for math.MinInt64 (whose
// negation does not fit int64).
func magnitude(v int64) uint64 {
	if v < 0 {
		return uint64(-(v + 1)) + 1 //nolint:gosec // the +1/-1 dance keeps MinInt64 in range
	}
	return uint64(v)
}

// decimalFloat widens a decimal to float64. Lossy by construction, which is
// why it is only reached through Float() and through comparisons that already
// involve a float (ADR-0051 §4).
func decimalFloat(coef int64, scale int8) float64 {
	return float64(coef) * math.Pow10(-int(scale))
}

// compareDecimals orders ac×10^-as against bc×10^-bs exactly, returning -1, 0
// or 1. No float64 is involved, so no result depends on binary rounding.
func compareDecimals(ac int64, as int8, bc int64, bs int8) int {
	sa, sb := sign(ac), sign(bc)
	if sa != sb {
		if sa < sb {
			return -1
		}
		return 1
	}
	if sa == 0 {
		return 0 // both zero, whatever their scales
	}
	c := compareMagnitudes(magnitude(ac), as, magnitude(bc), bs)
	if sa < 0 {
		return -c // ordering inverts for negatives
	}
	return c
}

// compareMagnitudes orders ma×10^-as against mb×10^-bs for non-zero
// magnitudes, by scaling the smaller-scaled operand up to the other's scale.
func compareMagnitudes(ma uint64, as int8, mb uint64, bs int8) int {
	if as == bs {
		return compareUint64(ma, mb)
	}
	if as > bs {
		return -compareMagnitudes(mb, bs, ma, as)
	}
	// as < bs, so ma must be multiplied by 10^d to reach scale bs.
	d := int(bs) - int(as)
	if d >= maxDecimalDigits {
		// ma is non-zero, so ma×10^19 exceeds any int64 magnitude: no
		// 128-bit arithmetic needed to know which side is larger.
		return 1
	}
	hi, lo := bits.Mul64(ma, pow10[d])
	if hi != 0 {
		return 1 // over 64 bits, so past every possible mb
	}
	return compareUint64(lo, mb)
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func sign(v int64) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	}
	return 0
}
