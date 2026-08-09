package schema

import (
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

func compileOrFail(t *testing.T, src string) *Schema {
	t.Helper()
	s, err := Compile([]byte(src))
	if err != nil {
		t.Fatalf("compile %s: %v", src, err)
	}
	return s
}

// oneField builds {"v": value} for validation.
func oneField(v record.Value) record.Value {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("v")
	bld.Value(v)
	bld.EndMap()
	return bld.Finish()
}

// TestADecimalIsANumberToTheValidator: a declared decimal field must satisfy
// the same assertions the equivalent JSON number would, or declaring the type
// would turn a passing payload into a failing one.
func TestADecimalIsANumberToTheValidator(t *testing.T) {
	s := compileOrFail(t, `{"type":"object","properties":{"v":{"type":"number","minimum":10,"maximum":20}}}`)

	if !s.Valid(oneField(record.Decimal(1010, 2))) { // 10.10
		t.Error("10.10 should satisfy number with 10 <= v <= 20")
	}
	if s.Valid(oneField(record.Decimal(999, 2))) { // 9.99
		t.Error("9.99 should fail minimum 10")
	}
	if s.Valid(oneField(record.Decimal(2001, 2))) { // 20.01
		t.Error("20.01 should fail maximum 20")
	}
}

// TestAWholeDecimalIsAnInteger mirrors the existing float rule (1.0 satisfies
// "integer"), decided exactly rather than through float64.
func TestAWholeDecimalIsAnInteger(t *testing.T) {
	s := compileOrFail(t, `{"type":"object","properties":{"v":{"type":"integer"}}}`)

	integral := []record.Value{
		record.Decimal(1000, 2), // 10.00
		record.Decimal(7, 0),    // 7
		record.Decimal(101, -3), // 101000
		record.Decimal(0, 5),    // 0.00000
	}
	for _, v := range integral {
		if !s.Valid(oneField(v)) {
			t.Errorf("%s should satisfy integer", v.Text())
		}
	}
	fractional := []record.Value{
		record.Decimal(1010, 2), // 10.10
		record.Decimal(5, 3),    // 0.005
	}
	for _, v := range fractional {
		if s.Valid(oneField(v)) {
			t.Errorf("%s should not satisfy integer", v.Text())
		}
	}
}

// TestAVeryPreciseDecimalIsJudgedWithoutFloat64: the integral test walks the
// coefficient rather than asking float64, which cannot represent this value.
func TestAVeryPreciseDecimalIsJudgedWithoutFloat64(t *testing.T) {
	s := compileOrFail(t, `{"type":"object","properties":{"v":{"type":"integer"}}}`)
	// 9007199254740993.00 — 2^53+1 with a scale. Integral, and float64 cannot
	// hold it.
	if !s.Valid(oneField(record.Decimal(900719925474099300, 2))) {
		t.Error("2^53+1 written to 2 decimal places is still an integer")
	}
	if s.Valid(oneField(record.Decimal(900719925474099301, 2))) {
		t.Error("2^53+1.01 is not an integer")
	}
}

func TestADecimalReportsItsExactValueInErrors(t *testing.T) {
	s := compileOrFail(t, `{"type":"object","properties":{"v":{"type":"string"}}}`)
	vs := s.Validate(oneField(record.Decimal(1010, 2)), nil)
	if len(vs) == 0 {
		t.Fatal("expected a violation")
	}
	// The message quotes the value; it must be the exact decimal, not a float
	// rendering that drops the trailing zero.
	if got := vs[0].String(); !strings.Contains(got, "10.10") {
		t.Errorf("violation %q should quote the exact value 10.10", got)
	}
}

// TestTemporalKindsCarryNoStringAssertions: a document this package validates
// arrives as JSON or YAML and parses to strings, so a temporal value cannot
// occur in one. If it somehow does, asserting string keywords against it would
// invent a lexical form the schema never described — so it simply fails the
// type check rather than being silently measured.
func TestTemporalKindsCarryNoStringAssertions(t *testing.T) {
	s := compileOrFail(t, `{"type":"object","properties":{"v":{"type":"string","minLength":5}}}`)
	ts := record.TimestampAt(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
	vs := s.Validate(oneField(ts), nil)
	if len(vs) == 0 {
		t.Fatal("a timestamp is not a JSON string; expected a type violation")
	}
	// And the error names the value usefully rather than showing empty text.
	if got := vs[0].String(); !strings.Contains(got, "2026-08-08") {
		t.Errorf("violation %q should describe the value", got)
	}
}
