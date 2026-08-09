package record

// Differential/property tests for the exact decimal claim (ADR-0051 §3/§4),
// closing register row TC-004.
//
// The hand-picked cases in decimal_test.go and exactsum_test.go prove the
// examples. For an arithmetic claim that is not the same as proving the
// property: a comparison that ignored scale, or an accumulator that wrapped at
// 64 bits, would still pass a list of cases chosen while the code was being
// written. So everything here is *generated* and checked against math/big,
// which is the trusted reference — big.Rat for value and ordering, big.Int for
// the 128-bit accumulator. Neither reference shares a line of arithmetic with
// the code under test.
//
// Reproducibility: every generator is seeded with a constant, so a failure in
// CI reproduces verbatim on a developer's machine. The Fuzz targets extend the
// same properties to inputs nobody thought of; the seeded tests are what runs
// on every `go test`.

import (
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"testing"
)

// diffSeed fixes every generator in this file and in temporal_diff_test.go. A
// property test that cannot be re-run identically reports a failure nobody can
// reproduce, so the seed is a constant rather than the clock.
const diffSeed = 20260809

var bigTen = big.NewInt(10)

// maxRefExp bounds the precomputed powers of ten. Scales reach ±128 and the
// widest thing ever asked for is 10^128, so 300 is slack, not a limit.
const maxRefExp = 300

// pow10Rat is 10^e as an exact rational. It is precomputed because the
// reference side builds one for nearly every generated value, and big.Int.Exp
// is otherwise the slowest thing in the loop. Written once at init and never
// mutated, so the fuzz targets share it safely.
var pow10Rat = func() [maxRefExp + 1]*big.Rat {
	var t [maxRefExp + 1]*big.Rat
	for e := range t {
		t[e] = new(big.Rat).SetInt(new(big.Int).Exp(bigTen, big.NewInt(int64(e)), nil))
	}
	return t
}()

// ratOfDecimal is the trusted meaning of a decimal: coefficient × 10^-scale as
// an exact rational, so no reference value is itself rounded on the way in.
func ratOfDecimal(coef int64, scale int8) *big.Rat {
	return mulPow10(new(big.Rat).SetInt64(coef), -int(scale))
}

// mulPow10 multiplies r by 10^e, exactly, for either sign of e. The exponent
// is an int rather than an int8 because the scales in play reach ±128 and
// negating an int8 -128 does not change it.
func mulPow10(r *big.Rat, e int) *big.Rat {
	if e == 0 {
		return r
	}
	mag := e
	if mag < 0 {
		mag = -mag
	}
	p := pow10Rat[mag]
	if e > 0 {
		return r.Mul(r, p)
	}
	return r.Quo(r, p)
}

// ratOfSum reads an accumulator's own state as a rational: (Hi·2^64 + Lo),
// signed and scaled. It goes through the exported fields rather than through
// Value() so that the 128-bit state can be checked after every single Add —
// including the states that Value() would refuse to report.
func ratOfSum(s ExactSum) *big.Rat {
	mag := new(big.Int).SetUint64(s.Hi)
	mag.Lsh(mag, 64)
	mag.Add(mag, new(big.Int).SetUint64(s.Lo))
	if s.Neg {
		mag.Neg(mag)
	}
	return mulPow10(new(big.Rat).SetInt(mag), -int(s.Scale))
}

// genDec is one generated exact number, carried as the (coefficient, scale)
// pair it was generated from rather than as a Value, so that the reference
// rational is built from the inputs and never from the accessors under test.
type genDec struct {
	coef  int64
	scale int8
	asInt bool // built as KindInt — an int is a decimal at scale 0 (ADR-0051 §4)
}

func (g genDec) value() Value {
	if g.asInt {
		return Int(g.coef)
	}
	return Decimal(g.coef, g.scale)
}

func (g genDec) rat() *big.Rat { return ratOfDecimal(g.coef, g.scale) }

// genCoef draws a coefficient. The mix is deliberate: uniform int64 rarely
// lands near a power of ten, and it is the powers of ten — where rescaling
// either just fits or just does not — that decide which branch of the
// comparison and the accumulator runs.
func genCoef(r *rand.Rand) int64 {
	switch r.Intn(8) {
	case 0:
		return 0
	case 1:
		return math.MaxInt64 // the coefficient ceiling
	case 2:
		return math.MinInt64 // and the floor, which has no positive counterpart
	case 3, 4:
		// Straddling a power of ten: p-1, p, p+1.
		p := int64(pow10[r.Intn(19)]) //nolint:gosec // pow10[0..18] all fit int64
		v := p + int64(r.Intn(3)) - 1
		if r.Intn(2) == 0 {
			return -v
		}
		return v
	case 5:
		return int64(r.Intn(2001) - 1000) // currency-sized
	default:
		return int64(r.Uint64()) //nolint:gosec // deliberate full-range draw, both signs
	}
}

// genScale draws a scale. Most flows are currency-shaped (0..8); wide draws
// cover the whole int8 range, which is where scale alignment has to reach for
// 10^255 and report that it cannot.
func genScale(r *rand.Rand, wide bool) int8 {
	if wide && r.Intn(2) == 0 {
		return int8(r.Intn(256) - 128) //nolint:gosec // constructed inside the int8 range
	}
	return int8(r.Intn(9)) //nolint:gosec // 0..8
}

func genDecimal(r *rand.Rand, wide bool) genDec {
	if r.Intn(6) == 0 {
		return genDec{coef: genCoef(r), asInt: true}
	}
	return genDec{coef: genCoef(r), scale: genScale(r, wide)}
}

// generatedDecimals builds a comparison pool. Every entry that can be restated
// at a finer scale is followed by that restatement (1.5 → 1.50), because
// equal-value-different-scale is exactly the pair a comparison that forgot to
// align scales gets wrong, and a uniform generator produces it almost never.
func generatedDecimals(r *rand.Rand, n int) []genDec {
	pool := make([]genDec, 0, n)
	for len(pool) < n {
		g := genDecimal(r, true)
		pool = append(pool, g)
		if twin, ok := rescaled(g, 1+r.Intn(3)); ok && len(pool) < n {
			pool = append(pool, twin)
		}
	}
	return pool
}

// rescaled restates g at a scale d digits finer, when the wider coefficient
// still fits an int64 and the scale still fits an int8.
func rescaled(g genDec, d int) (genDec, bool) {
	scale := int(g.scale) + d
	if g.asInt || scale > math.MaxInt8 || d < 1 || d > 18 {
		return genDec{}, false
	}
	p := int64(pow10[d]) //nolint:gosec // d <= 18, so pow10[d] fits int64
	if g.coef != 0 && (g.coef > math.MaxInt64/p || g.coef < math.MinInt64/p) {
		return genDec{}, false
	}
	return genDec{coef: g.coef * p, scale: int8(scale)}, true //nolint:gosec // range-checked above
}

// TestDecimalComparisonAgreesWithBigRatOnEveryGeneratedPair is the ordering
// half of ADR-0051 §4. Agreement with big.Rat over every pair of a generated
// pool is stronger than a list of orderings: Rat.Cmp *is* a total order on the
// values, so agreeing with it everywhere makes Compare one too — reflexive,
// antisymmetric and transitive — with no case chosen by the author of the
// comparison.
func TestDecimalComparisonAgreesWithBigRatOnEveryGeneratedPair(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed)) //nolint:gosec // deterministic test data, not security
	pool := generatedDecimals(r, 160)

	values := make([]Value, len(pool))
	rats := make([]*big.Rat, len(pool))
	for i, g := range pool {
		values[i], rats[i] = g.value(), g.rat()
	}

	var equalDifferentScale, strictlyOrdered, crossKind int
	for i := range pool {
		for j := range pool {
			got, ok := Compare(values[i], values[j])
			if !ok {
				t.Fatalf("Compare(%s, %s) reported incomparable; every exact number orders against every other",
					values[i].Text(), values[j].Text())
			}
			want := rats[i].Cmp(rats[j])
			if got != want {
				t.Fatalf("Compare(%s, %s) = %d, big.Rat says %d (%s vs %s)",
					values[i].Text(), values[j].Text(), got, want,
					rats[i].RatString(), rats[j].RatString())
			}
			if back, _ := Compare(values[j], values[i]); back != -got {
				t.Fatalf("Compare is not antisymmetric for %s and %s: %d then %d",
					values[i].Text(), values[j].Text(), got, back)
			}
			if eq := values[i].EqualScalar(values[j]); eq != (want == 0) {
				t.Fatalf("EqualScalar(%s, %s) = %v, big.Rat says equal=%v",
					values[i].Text(), values[j].Text(), eq, want == 0)
			}
			switch {
			case want == 0 && pool[i].scale != pool[j].scale:
				equalDifferentScale++
			case want != 0:
				strictlyOrdered++
			}
			if pool[i].asInt != pool[j].asInt {
				crossKind++
			}
		}
	}

	// Non-vacuity: the generator must actually have produced the cases the
	// property is interesting for, or this test is a slow way of comparing a
	// number with itself.
	if equalDifferentScale == 0 {
		t.Fatal("no equal-value-different-scale pairs were generated — the case a naive comparison gets wrong went untested")
	}
	if crossKind == 0 {
		t.Fatal("no int-against-decimal pairs were generated — the scale-0 view went untested")
	}
	if strictlyOrdered == 0 {
		t.Fatal("nothing in the pool was strictly ordered")
	}
}

func FuzzDecimalCompareMatchesBigRat(f *testing.F) {
	f.Add(int64(1), int8(1), int64(10), int8(2))             // 0.1 vs 0.10
	f.Add(int64(0), int8(0), int64(0), int8(9))              // zero at two scales
	f.Add(int64(math.MaxInt64), int8(30), int64(1), int8(0)) // rescaling would overflow
	f.Add(int64(math.MinInt64), int8(-128), int64(math.MaxInt64), int8(127))
	f.Add(int64(-1), int8(-100), int64(1), int8(27))
	f.Fuzz(func(t *testing.T, ac int64, as int8, bc int64, bs int8) {
		a, b := Decimal(ac, as), Decimal(bc, bs)
		got, ok := Compare(a, b)
		if !ok {
			t.Fatalf("Compare(%s, %s) reported incomparable", a.Text(), b.Text())
		}
		want := ratOfDecimal(ac, as).Cmp(ratOfDecimal(bc, bs))
		if got != want {
			t.Fatalf("Compare(%dE-%d, %dE-%d) = %d, big.Rat says %d", ac, as, bc, bs, got, want)
		}
		if back, _ := Compare(b, a); back != -got {
			t.Fatalf("Compare is not antisymmetric: %d then %d", got, back)
		}
	})
}

// TestDecimalTextIsExactlyTheValueBigReads holds the rendering to the same
// standard as the arithmetic: the text a decimal writes must be the number it
// is, read back by an independent parser. It also pins that whatever
// AppendDecimal emits is at least well-formed — a rejection from our own
// parser is only acceptable as a range error, never as "not a decimal".
func TestDecimalTextIsExactlyTheValueBigReads(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 1)) //nolint:gosec // deterministic test data, not security
	var reparsed, outOfRange int
	for range 4000 {
		g := genDecimal(r, true)
		text := string(AppendDecimal(nil, g.coef, g.scale))

		ref, ok := new(big.Rat).SetString(text)
		if !ok {
			t.Fatalf("big.Rat cannot read %q, rendered from coef=%d scale=%d", text, g.coef, g.scale)
		}
		if ref.Cmp(g.rat()) != 0 {
			t.Fatalf("AppendDecimal(%d, %d) = %q, which big reads as %s, not %s",
				g.coef, g.scale, text, ref.RatString(), g.rat().RatString())
		}

		back, err := ParseDecimal([]byte(text))
		if err != nil {
			if !errors.Is(err, ErrDecimalRange) {
				t.Fatalf("ParseDecimal(%q) = %v; a rendering of our own must be well-formed", text, err)
			}
			outOfRange++
			continue
		}
		coef, scale := back.Decimal()
		if ratOfDecimal(coef, scale).Cmp(g.rat()) != 0 {
			t.Fatalf("round trip of coef=%d scale=%d through %q gave coef=%d scale=%d",
				g.coef, g.scale, text, coef, scale)
		}
		reparsed++
	}
	// Both arms must have run: all-rejected would prove nothing about parsing,
	// all-accepted would prove nothing about the range check.
	if reparsed == 0 || outOfRange == 0 {
		t.Fatalf("one arm went unexercised: %d re-parsed, %d out of range", reparsed, outOfRange)
	}
}

// TestExactSumMatchesBigOverGeneratedSequences is the accumulation half of the
// claim. The 128-bit state is checked against big.Rat after *every* Add rather
// than only at the end, so a wrap is caught at the value that caused it and
// cannot be cancelled out by a later one.
func TestExactSumMatchesBigOverGeneratedSequences(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 2)) //nolint:gosec // deterministic test data, not security
	var completed, addOverflow, reported int
	for iter := range 3000 {
		wide := iter%4 == 0 // most sequences are currency-shaped; some stress the scale extremes
		total := new(big.Rat)
		var s ExactSum
		aborted := false
		for range 1 + r.Intn(12) {
			g := genDecimal(r, wide)
			if err := s.Add(g.value()); err != nil {
				// Aligning scales can genuinely need more than 128 bits; that is
				// reported, and reporting it is all this arm claims.
				if !errors.Is(err, ErrSumOverflow) {
					t.Fatalf("Add(%s) error = %v, want ErrSumOverflow", g.value().Text(), err)
				}
				addOverflow++
				aborted = true
				break
			}
			total.Add(total, g.rat())
			if got := ratOfSum(s); got.Cmp(total) != 0 {
				t.Fatalf("accumulator holds %s, big says %s (after adding coef=%d scale=%d)",
					got.RatString(), total.RatString(), g.coef, g.scale)
			}
		}
		if aborted {
			continue
		}
		completed++

		v, err := s.Value()
		if err != nil {
			if !errors.Is(err, ErrSumOverflow) {
				t.Fatalf("Value() error = %v, want ErrSumOverflow", err)
			}
			// An overflow must be the truth, not a shortcut: big decides whether
			// the true total was reportable at all.
			if scale, ok := representableScale(total, s.Scale); ok {
				t.Fatalf("Value() reported overflow, but the true total %s is exact at scale %d",
					total.RatString(), scale)
			}
			reported++
			continue
		}
		coef, scale, ok := v.ExactDecimal()
		if !ok {
			t.Fatalf("a sum of exact numbers came back as %v", v.Kind())
		}
		if ratOfDecimal(coef, scale).Cmp(total) != 0 {
			t.Fatalf("sum = %s (coef=%d scale=%d), big says %s", v.Text(), coef, scale, total.RatString())
		}
	}
	if completed == 0 || reported == 0 || addOverflow == 0 {
		t.Fatalf("one arm went unexercised: %d completed, %d reported overflow at Value, %d at Add",
			completed, reported, addOverflow)
	}
}

// representableScale mirrors the search Value() performs, in big: divide the
// total by ten while that stays exact, and give up at scale 0. It returns the
// scale at which an int64 coefficient would have held the total, so an
// unjustified overflow report can be named rather than merely suspected.
//
// The available scales are [0, accScale] when the accumulator's scale is
// positive; when it is zero or negative there is nowhere coarser to go, so the
// only candidate is the accumulator's own scale.
func representableScale(total *big.Rat, accScale int8) (int, bool) {
	lo := 0
	if accScale < 0 {
		lo = int(accScale)
	}
	for s := int(accScale); s >= lo; s-- {
		scaled := mulPow10(new(big.Rat).Set(total), s)
		if !scaled.IsInt() {
			return 0, false // no coarser scale can be exact either
		}
		if scaled.Num().IsInt64() {
			return s, true
		}
	}
	return 0, false
}

// TestOneHundredTwentyEightBitOverflowIsReportedNotWrapped is ADR-0051 §3 as a
// property rather than an example: for magnitudes drawn right up against the
// 128-bit ceiling, the accumulator must report an error exactly when big says
// the true total needs 2^128 or more, and must hold the true total exactly
// whenever it does not. A silent wrap fails the second half; a saturating
// implementation fails it too.
func TestOneHundredTwentyEightBitOverflowIsReportedNotWrapped(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 3)) //nolint:gosec // deterministic test data, not security
	two128 := new(big.Int).Lsh(big.NewInt(1), 128)
	var overflowed, fitted int
	for range 4000 {
		aHi, aLo, aNeg := genMagnitude(r)
		bHi, bLo, bNeg := genMagnitude(r)
		s := ExactSum{Hi: aHi, Lo: aLo, Neg: aNeg}
		err := s.AddSum(ExactSum{Hi: bHi, Lo: bLo, Neg: bNeg})

		want := new(big.Int).Add(signed128(aHi, aLo, aNeg), signed128(bHi, bLo, bNeg))
		fits := new(big.Int).Abs(want).Cmp(two128) < 0

		switch {
		case err != nil:
			if !errors.Is(err, ErrSumOverflow) {
				t.Fatalf("AddSum error = %v, want ErrSumOverflow", err)
			}
			if fits {
				t.Fatalf("AddSum reported overflow for a total that fits 128 bits: %s", want.String())
			}
			overflowed++
		case !fits:
			t.Fatalf("AddSum accepted a total of %s, which needs more than 128 bits; it now holds %s",
				want.String(), ratOfSum(s).RatString())
		default:
			if got := ratOfSum(s); got.Cmp(new(big.Rat).SetInt(want)) != 0 {
				t.Fatalf("AddSum gave %s, big says %s — the true total was not preserved",
					got.RatString(), want.String())
			}
			fitted++
		}
	}
	if overflowed == 0 || fitted == 0 {
		t.Fatalf("one arm went unexercised: %d overflowed, %d fitted", overflowed, fitted)
	}
}

// genMagnitude draws a 128-bit magnitude biased toward the top of the range,
// where carries out of the high word actually happen. A uniform draw would
// overflow on essentially every pair and never exercise the other arm.
func genMagnitude(r *rand.Rand) (hi, lo uint64, neg bool) {
	neg = r.Intn(2) == 0
	switch r.Intn(4) {
	case 0:
		return 0, r.Uint64(), neg // 64-bit values, which never overflow together
	case 1:
		// Right at the 128-bit ceiling, where a carry out of the high word is
		// one added value away.
		return math.MaxUint64 - uint64(r.Intn(4)), r.Uint64(), neg //nolint:gosec // Intn(4) is 0..3
	case 2:
		return math.MaxUint64 / 2, r.Uint64(), neg // two of these carry, one does not
	default:
		return r.Uint64(), r.Uint64(), neg
	}
}

func signed128(hi, lo uint64, neg bool) *big.Int {
	v := new(big.Int).SetUint64(hi)
	v.Lsh(v, 64)
	v.Add(v, new(big.Int).SetUint64(lo))
	if neg {
		v.Neg(v)
	}
	return v
}

// TestATotalTooWideForAnInt64CoefficientKeepsTheTrueValue covers the seam
// between the two widths: the accumulator is 128 bits, a reported Value is an
// int64 coefficient, and a total that lives in between must produce an error
// *and* leave the exact total in the accumulator. That pairing is what makes
// "reported, never wrapped" checkable — big supplies the answer the caller did
// not get.
func TestATotalTooWideForAnInt64CoefficientKeepsTheTrueValue(t *testing.T) {
	r := rand.New(rand.NewSource(diffSeed + 4)) //nolint:gosec // deterministic test data, not security
	maxCoef := new(big.Int).SetInt64(math.MaxInt64)
	var checked int
	for range 500 {
		n := 2 + r.Intn(8)
		coef := math.MaxInt64 - int64(r.Intn(1000)) // large enough that n of them cannot fit
		total := new(big.Rat)
		var s ExactSum
		for range n {
			if err := s.Add(Int(coef)); err != nil {
				t.Fatalf("Add(%d) error = %v; %d of these fit 128 bits easily", coef, err, n)
			}
			total.Add(total, new(big.Rat).SetInt64(coef))
		}
		// The 128-bit state is the proof that nothing wrapped: it still equals
		// the true total even though no int64 coefficient can express it.
		if got := ratOfSum(s); got.Cmp(total) != 0 {
			t.Fatalf("accumulator holds %s, big says %s", got.RatString(), total.RatString())
		}
		if total.Num().Cmp(maxCoef) <= 0 {
			t.Fatalf("this case must exceed an int64 coefficient; %s does not", total.RatString())
		}
		if _, err := s.Value(); !errors.Is(err, ErrSumOverflow) {
			t.Fatalf("Value() error = %v, want ErrSumOverflow (true total %s)", err, total.RatString())
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no overflowing totals were generated")
	}
}

// FuzzExactSumMatchesBig reads the input as a sequence of 9-byte
// (coefficient, scale) pairs, so the fuzzer explores orderings of mixed scales
// — which is where alignment either preserves every digit or quietly drops
// one.
func FuzzExactSumMatchesBig(f *testing.F) {
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 3, 0xF2, 2, // 1010 at scale 2 — 10.10
		0, 0, 0, 0, 0, 0, 0, 1, 1, // 0.1
		0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, // MaxInt64
		0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, // and again: overflow
	})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0x80}) // scale -128
	f.Fuzz(func(t *testing.T, data []byte) {
		total := new(big.Rat)
		var s ExactSum
		for i := 0; i+9 <= len(data) && i < 64*9; i += 9 {
			coef := int64(binary.BigEndian.Uint64(data[i : i+8])) //nolint:gosec // deliberate full-range draw
			scale := int8(data[i+8])                              //nolint:gosec // deliberate full-range draw
			if err := s.Add(Decimal(coef, scale)); err != nil {
				if !errors.Is(err, ErrSumOverflow) {
					t.Fatalf("Add error = %v, want ErrSumOverflow", err)
				}
				return
			}
			total.Add(total, ratOfDecimal(coef, scale))
			if got := ratOfSum(s); got.Cmp(total) != 0 {
				t.Fatalf("accumulator holds %s, big says %s", got.RatString(), total.RatString())
			}
		}
		v, err := s.Value()
		if err != nil {
			if !errors.Is(err, ErrSumOverflow) {
				t.Fatalf("Value() error = %v, want ErrSumOverflow", err)
			}
			if scale, ok := representableScale(total, s.Scale); ok {
				t.Fatalf("Value() reported overflow, but %s is exact at scale %d", total.RatString(), scale)
			}
			return
		}
		coef, scale, ok := v.ExactDecimal()
		if !ok {
			t.Fatalf("a sum of exact numbers came back as %v", v.Kind())
		}
		if ratOfDecimal(coef, scale).Cmp(total) != 0 {
			t.Fatalf("sum = %s, big says %s", v.Text(), total.RatString())
		}
	})
}
