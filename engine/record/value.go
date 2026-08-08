// Package record implements SHIFT's hierarchical, typed record model
// (ADR-0004): values arranged in batches whose memory comes from
// chunk-allocated arenas, so a batch is recycled wholesale instead of
// allocating per record. No map[string]interface{} anywhere.
//
// Lifetime contract: a Value is only valid while the Batch it was built in
// is live. Sources reuse batches (see stream.Source), so anything that
// retains data across batches must deep-copy it into its own Batch.
package record

import (
	"bytes"
	"math"
)

// Kind identifies the type of a Value.
type Kind uint8

// Value kinds. The zero Kind is Null. New kinds are appended, never
// renumbered — the spill codec's tags are a separate numbering precisely so
// that this set can grow (ADR-0051 §6).
const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindBytes
	KindList
	KindMap
	// KindDecimal is an exact decimal: coefficient × 10^-scale (ADR-0051).
	KindDecimal
	// KindTimestamp is an instant in time, with a zone offset for rendering.
	KindTimestamp
	// KindDate is a calendar day with no time of day.
	KindDate
	// KindTime is a time of day with no date.
	KindTime
)

func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindBool:
		return "bool"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindString:
		return "string"
	case KindBytes:
		return "bytes"
	case KindList:
		return "list"
	case KindMap:
		return "map"
	case KindDecimal:
		return "decimal"
	case KindTimestamp:
		return "timestamp"
	case KindDate:
		return "date"
	case KindTime:
		return "time"
	default:
		return "invalid"
	}
}

// Value is one node of a record tree. The zero Value is null.
//
// Scalars are stored inline (num holds bool/int/float bits; str views
// string/bytes data in the batch arena). Containers reference contiguous
// child slices in the batch's slab allocators: lists use kids; maps use
// kids for field values and keys for the parallel field names, preserving
// field order.
//
// aux is a second byte of kind-specific payload that costs nothing: kind is
// followed by alignment padding before num, so the struct is the same size
// with it as without (asserted by TestValueStaysEightyEightBytes). It carries
// a decimal's scale and a timestamp's zone offset, which is what lets those
// kinds be exact without an allocation (ADR-0051 §1).
type Value struct {
	kind Kind
	aux  int8
	num  uint64
	str  []byte
	kids []Value
	keys [][]byte
}

// Null returns the null value.
func Null() Value { return Value{} }

// Bool returns a boolean value.
func Bool(b bool) Value {
	var n uint64
	if b {
		n = 1
	}
	return Value{kind: KindBool, num: n}
}

// Int returns an integer value.
func Int(i int64) Value {
	return Value{kind: KindInt, num: uint64(i)} //nolint:gosec // deliberate bit-store; Int() reverses it
}

// Float returns a floating-point value.
func Float(f float64) Value { return Value{kind: KindFloat, num: math.Float64bits(f)} }

// UnsafeString wraps b as a string value WITHOUT copying it into an arena.
// The caller must guarantee b outlives every use of the value; prefer
// Builder.String for data flowing through pipelines.
func UnsafeString(b []byte) Value { return Value{kind: KindString, str: b} }

// Kind reports the value's type.
func (v Value) Kind() Kind { return v.kind }

// IsNull reports whether the value is null.
func (v Value) IsNull() bool { return v.kind == KindNull }

// Bool returns the boolean payload (false unless KindBool).
func (v Value) Bool() bool { return v.kind == KindBool && v.num != 0 }

// Int returns the integer payload (0 unless KindInt).
func (v Value) Int() int64 {
	if v.kind != KindInt {
		return 0
	}
	return int64(v.num) //nolint:gosec // reverses the bit-store in Int()
}

// Float returns the float payload. KindInt and KindDecimal are widened for
// convenience, which is lossy for both: prefer Decimal and exact comparison
// when the value's precision is the point (ADR-0051 §4).
func (v Value) Float() float64 {
	switch v.kind {
	case KindFloat:
		return math.Float64frombits(v.num)
	case KindInt:
		return float64(int64(v.num)) //nolint:gosec // reverses the bit-store in Int()
	case KindDecimal:
		return decimalFloat(int64(v.num), v.aux) //nolint:gosec // reverses the bit-store in Decimal()
	default:
		return 0
	}
}

// Bytes returns the raw string/bytes payload as an arena view. Callers must
// not modify or retain it beyond the batch lifetime.
func (v Value) Bytes() []byte {
	if v.kind != KindString && v.kind != KindBytes {
		return nil
	}
	return v.str
}

// String returns the string payload, copying out of the arena. Use Bytes on
// hot paths.
func (v Value) String() string { return string(v.Bytes()) }

// Len returns the number of children (list elements or map fields).
func (v Value) Len() int { return len(v.kids) }

// Index returns the i-th list element or map field value.
func (v Value) Index(i int) Value { return v.kids[i] }

// KeyAt returns the i-th map field name as an arena view.
func (v Value) KeyAt(i int) []byte { return v.keys[i] }

// Field returns the value of the named map field. Lookup is a linear scan:
// records are typically narrow, and field slices stay cache-resident.
func (v Value) Field(name string) (Value, bool) {
	if v.kind != KindMap {
		return Value{}, false
	}
	for i, k := range v.keys {
		if string(k) == name { // alloc-free comparison
			return v.kids[i], true
		}
	}
	return Value{}, false
}

// SetIndex replaces the i-th child (list element or map field value) in
// place. The child slab is shared, so every Value header referencing this
// container observes the change. nv must belong to the same batch (or be a
// scalar).
func (v Value) SetIndex(i int, nv Value) {
	if v.kind != KindList && v.kind != KindMap {
		panic("record: SetIndex on non-container")
	}
	v.kids[i] = nv
}

// EqualScalar reports whether two scalar values are equal. Containers
// compare as unequal (use application-level comparison for those). The
// numeric kinds cross-compare numerically, and Int/Decimal do so exactly.
func (v Value) EqualScalar(o Value) bool {
	if v.kind == KindNull || o.kind == KindNull {
		return v.kind == KindNull && o.kind == KindNull
	}
	c, ok := Compare(v, o)
	return ok && c == 0
}

// Compare orders two scalar values, returning -1, 0 or 1. ok is false when the
// kinds are not comparable — two different temporal kinds, a container, a
// null, or a number against a string — so a caller can report the mismatch
// rather than acting on a fabricated ordering.
//
// Exactness follows ADR-0051 §4: Int and Decimal compare without float64 in
// the path, in either order and at any scales. A comparison involving
// KindFloat goes through float64 and is therefore as exact as the float is.
func Compare(a, b Value) (int, bool) {
	switch a.kind {
	case KindBool:
		if b.kind != KindBool {
			return 0, false
		}
		return compareUint64(a.num, b.num), true // false < true
	case KindInt, KindDecimal, KindFloat:
		return compareNumeric(a, b)
	case KindString, KindBytes:
		if b.kind != KindString && b.kind != KindBytes {
			return 0, false
		}
		return bytes.Compare(a.str, b.str), true
	case KindTimestamp:
		if b.kind != KindTimestamp {
			return 0, false
		}
		// Instants, not wall clocks: the stored offset is presentation only,
		// so two timestamps from different zones still order correctly.
		return compareInt64(a.UnixNano(), b.UnixNano()), true
	case KindDate:
		if b.kind != KindDate {
			return 0, false
		}
		return compareInt64(a.DateDays(), b.DateDays()), true
	case KindTime:
		if b.kind != KindTime {
			return 0, false
		}
		return compareInt64(a.DayNanos(), b.DayNanos()), true
	default:
		return 0, false
	}
}

// compareNumeric orders two numbers, staying exact unless a float is involved.
func compareNumeric(a, b Value) (int, bool) {
	if !b.IsNumeric() {
		return 0, false
	}
	if a.kind == KindFloat || b.kind == KindFloat {
		af, bf := a.Float(), b.Float()
		if math.IsNaN(af) || math.IsNaN(bf) {
			// NaN is ordered against nothing, itself included. Reporting it as
			// unordered lets the caller say so; inventing an ordering would
			// make a filter silently keep or drop the record.
			return 0, false
		}
		return compareFloat64(af, bf), true
	}
	// Both exact. An int is a decimal with scale 0.
	ac, as := a.exactDecimal()
	bc, bs := b.exactDecimal()
	return compareDecimals(ac, as, bc, bs), true
}

// ScalarBits is the inline payload of a scalar Value — kind, aux and num, with
// none of the slice headers — in 16 bytes rather than 88.
//
// It exists for state that must be held per group or per row rather than per
// record: the aggregate keeps a running MIN/MAX for every group, and paying 88
// bytes each to carry three used fields moved the aggregate's peak RSS by
// nearly a factor of two. Anything holding one value per unit of a large
// cardinality should hold these instead.
type ScalarBits struct {
	Num  uint64
	Kind Kind
	Aux  int8
}

// ScalarBitsOf extracts the inline payload of v. ok is false for the kinds
// whose payload is *not* inline — containers, strings and bytes all point into
// a batch's allocators, so they cannot be held this way and must be copied with
// CopyValue instead.
func ScalarBitsOf(v Value) (ScalarBits, bool) {
	switch v.kind {
	case KindNull, KindBool, KindInt, KindFloat,
		KindDecimal, KindTimestamp, KindDate, KindTime:
		return ScalarBits{Num: v.num, Kind: v.kind, Aux: v.aux}, true
	default:
		return ScalarBits{}, false
	}
}

// Value rebuilds the scalar. The result is independent of any batch.
func (s ScalarBits) Value() Value {
	return Value{kind: s.Kind, aux: s.Aux, num: s.Num}
}

// ExactDecimal views an exact numeric value as a coefficient and scale: a
// decimal as itself, an int as a decimal at scale 0. ok is false for every
// other kind, KindFloat included — a float has no exact decimal, and treating
// one as though it did is how precision goes missing quietly.
func (v Value) ExactDecimal() (coef int64, scale int8, ok bool) {
	switch v.kind {
	case KindDecimal:
		c, s := v.Decimal()
		return c, s, true
	case KindInt:
		return v.Int(), 0, true
	default:
		return 0, 0, false
	}
}

// exactDecimal is ExactDecimal for callers that have already checked the kind.
func (v Value) exactDecimal() (coef int64, scale int8) {
	c, s, _ := v.ExactDecimal()
	return c, s
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// compareFloat64 orders two non-NaN floats.
func compareFloat64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
