package starlarkop

import (
	"errors"
	"fmt"

	"go.starlark.net/starlark"

	"github.com/aaron-au/shift/engine/record"
)

// Decimal is an exact decimal inside a script (ADR-0052 §3).
//
// It exists because the obvious alternative is worse in a way nobody would
// notice: exposing a decimal as a Starlark float means `qty * price` on money
// silently becomes binary floating point, at the exact moment the flow author
// is doing the arithmetic they care about. All of ADR-0051 would be undone by
// one multiplication.
//
// So +, - and * are exact, computed through record.ExactSum and 128-bit
// scaling, and comparison is exact via record.Compare. Division is refused —
// see div below.
type Decimal struct {
	v record.Value
}

var (
	_ starlark.Value      = Decimal{}
	_ starlark.Comparable = Decimal{}
	_ starlark.HasBinary  = Decimal{}
	_ starlark.HasAttrs   = Decimal{}
)

func (d Decimal) String() string       { return d.v.Text() }
func (d Decimal) Type() string         { return "decimal" }
func (d Decimal) Freeze()              {}
func (d Decimal) Truth() starlark.Bool { coef, _ := d.v.Decimal(); return starlark.Bool(coef != 0) }

func (d Decimal) Hash() (uint32, error) {
	coef, scale := d.v.Decimal()
	return uint32(coef) ^ uint32(scale), nil //nolint:gosec // a hash, not a value
}

// Attr exposes the parts a script may legitimately want.
func (d Decimal) Attr(name string) (starlark.Value, error) {
	coef, scale := d.v.Decimal()
	switch name {
	case "text":
		return starlark.String(d.v.Text()), nil
	case "coefficient":
		return starlark.MakeInt64(coef), nil
	case "scale":
		return starlark.MakeInt(int(scale)), nil
	case "float":
		// Named so the loss is visible at the call site rather than implied.
		return starlark.Float(d.v.Float()), nil
	case "rescale":
		return starlark.NewBuiltin("rescale", d.rescale), nil
	default:
		return nil, nil
	}
}

func (d Decimal) AttrNames() []string {
	return []string{"coefficient", "float", "rescale", "scale", "text"}
}

// rescale moves a decimal to a different number of places. Reducing precision
// is a rounding decision, so it must be asked for explicitly — this is the
// call division points at.
func (d Decimal) rescale(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kw []starlark.Tuple) (starlark.Value, error) {
	var want int
	if err := starlark.UnpackPositionalArgs(b.Name(), args, kw, 1, &want); err != nil {
		return nil, err
	}
	if want < -128 || want > 127 {
		return nil, fmt.Errorf("rescale: scale %d is out of range", want)
	}
	coef, scale := d.v.Decimal()
	target := int8(want) //nolint:gosec // range-checked immediately above
	for scale < target {
		if coef > (1<<62)/10 || coef < -(1<<62)/10 {
			return nil, fmt.Errorf("rescale: %s does not fit scale %d", d.v.Text(), want)
		}
		coef *= 10
		scale++
	}
	for scale > target {
		// Round half away from zero: the rule invoices use, and stated rather
		// than inherited from whatever the language does.
		rem := coef % 10
		coef /= 10
		if rem >= 5 {
			coef++
		} else if rem <= -5 {
			coef--
		}
		scale--
	}
	return Decimal{v: record.Decimal(coef, target)}, nil
}

func (d Decimal) CompareSameType(op syntaxToken, other starlark.Value, _ int) (bool, error) {
	o, ok := other.(Decimal)
	if !ok {
		return false, fmt.Errorf("cannot compare decimal with %s", other.Type())
	}
	c, ok := record.Compare(d.v, o.v)
	if !ok {
		return false, fmt.Errorf("cannot order %s against %s", d.v.Text(), o.v.Text())
	}
	return compareResult(op, c)
}

// Binary implements the exact arithmetic. An int operand is exact too (a
// decimal at scale 0), so `amount * 3` works; a float operand is REFUSED
// rather than silently making the result inexact — the whole point of the type.
func (d Decimal) Binary(op syntaxToken, y starlark.Value, side starlark.Side) (starlark.Value, error) {
	other, err := exactOperand(y)
	if err != nil {
		return nil, err
	}
	switch op {
	case tokenPlus:
		return d.sum(other, false)
	case tokenMinus:
		if side == starlark.Right {
			// y - d, so negate this side.
			return Decimal{v: other}.sum(d.v, true)
		}
		return d.sum(other, true)
	case tokenStar:
		return d.mul(other)
	case tokenSlash, tokenSlashSlash, tokenPercent:
		return nil, errors.New("decimal division is not available: there is no exact quotient in " +
			"general, so divide explicitly — e.g. (a.coefficient / b.coefficient) — or call " +
			".rescale(n) to state the rounding you want")
	default:
		return nil, nil // let Starlark report the unsupported operation
	}
}

// sum adds (or subtracts) exactly, through the 128-bit accumulator.
func (d Decimal) sum(other record.Value, negate bool) (starlark.Value, error) {
	var acc record.ExactSum
	if err := acc.Add(d.v); err != nil {
		return nil, err
	}
	add := other
	if negate {
		coef, scale, ok := other.ExactDecimal()
		if !ok {
			return nil, fmt.Errorf("cannot subtract %v", other.Kind())
		}
		if coef == minInt64 {
			return nil, fmt.Errorf("cannot negate %s", other.Text())
		}
		add = record.Decimal(-coef, scale)
	}
	if err := acc.Add(add); err != nil {
		return nil, err
	}
	v, err := acc.Value()
	if err != nil {
		return nil, err
	}
	return asDecimal(v), nil
}

// mul multiplies exactly: coefficients multiply, scales add. An overflow is an
// error rather than a wrap, for the same reason as everywhere else in ADR-0051
// — a wrapped total is indistinguishable from a correct one downstream.
func (d Decimal) mul(other record.Value) (starlark.Value, error) {
	ac, as := d.v.Decimal()
	bc, bs, ok := other.ExactDecimal()
	if !ok {
		return nil, fmt.Errorf("cannot multiply by %v", other.Kind())
	}
	prod := ac * bc
	if ac != 0 && (prod/ac != bc || (ac == -1 && bc == minInt64) || (bc == -1 && ac == minInt64)) {
		return nil, fmt.Errorf("decimal overflow multiplying %s by %s", d.v.Text(), other.Text())
	}
	scale := int(as) + int(bs)
	if scale < -128 || scale > 127 {
		return nil, fmt.Errorf("decimal overflow: %s × %s needs scale %d", d.v.Text(), other.Text(), scale)
	}
	return asDecimal(record.Decimal(prod, int8(scale))), nil //nolint:gosec // range-checked above
}

const minInt64 = -1 << 63

// asDecimal keeps the script-visible type stable: an exact result stays a
// decimal even when its scale reduced to zero, so `a * b` never changes type
// under the author on particular data.
func asDecimal(v record.Value) starlark.Value {
	if v.Kind() == record.KindInt {
		return Decimal{v: record.Decimal(v.Int(), 0)}
	}
	return Decimal{v: v}
}

// exactOperand accepts the operands that keep arithmetic exact, and refuses the
// one that would not.
func exactOperand(y starlark.Value) (record.Value, error) {
	switch t := y.(type) {
	case Decimal:
		return t.v, nil
	case starlark.Int:
		n, ok := t.Int64()
		if !ok {
			return record.Value{}, fmt.Errorf("integer %s is too large for exact arithmetic", t.String())
		}
		return record.Int(n), nil
	case starlark.Float:
		return record.Value{}, errors.New("cannot combine a decimal with a float: that would make the " +
			"result inexact, which is what the decimal type exists to prevent — convert deliberately " +
			"with .float() if you mean it")
	default:
		return record.Value{}, fmt.Errorf("cannot combine a decimal with %s", y.Type())
	}
}

// Temporal is an instant, date or time-of-day inside a script: comparable and
// printable, with no arithmetic yet (date maths needs a stated calendar
// convention, which is its own decision).
type Temporal struct {
	v record.Value
}

var (
	_ starlark.Value      = Temporal{}
	_ starlark.Comparable = Temporal{}
	_ starlark.HasAttrs   = Temporal{}
)

func (t Temporal) String() string       { return t.v.Text() }
func (t Temporal) Type() string         { return t.v.Kind().String() }
func (t Temporal) Freeze()              {}
func (t Temporal) Truth() starlark.Bool { return starlark.True }
func (t Temporal) Hash() (uint32, error) {
	return uint32(t.v.UnixNano()) ^ uint32(t.v.DateDays()) ^ uint32(t.v.DayNanos()), nil //nolint:gosec // a hash
}

func (t Temporal) Attr(name string) (starlark.Value, error) {
	switch name {
	case "text":
		return starlark.String(t.v.Text()), nil
	case "unix_nanos":
		return starlark.MakeInt64(t.v.UnixNano()), nil
	case "days":
		return starlark.MakeInt64(t.v.DateDays()), nil
	case "nanos_of_day":
		return starlark.MakeInt64(t.v.DayNanos()), nil
	default:
		return nil, nil
	}
}

func (t Temporal) AttrNames() []string {
	return []string{"days", "nanos_of_day", "text", "unix_nanos"}
}

func (t Temporal) CompareSameType(op syntaxToken, other starlark.Value, _ int) (bool, error) {
	o, ok := other.(Temporal)
	if !ok {
		return false, fmt.Errorf("cannot compare %s with %s", t.Type(), other.Type())
	}
	c, ok := record.Compare(t.v, o.v)
	if !ok {
		return false, fmt.Errorf("cannot compare %s with %s", t.Type(), o.Type())
	}
	return compareResult(op, c)
}
