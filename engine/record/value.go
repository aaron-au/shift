// Package record implements SHIFT's hierarchical, typed record model
// (ADR-0004): values arranged in batches whose memory comes from
// chunk-allocated arenas, so a batch is recycled wholesale instead of
// allocating per record. No map[string]interface{} anywhere.
//
// Lifetime contract: a Value is only valid while the Batch it was built in
// is live. Sources reuse batches (see stream.Source), so anything that
// retains data across batches must deep-copy it into its own Batch.
package record

import "math"

// Kind identifies the type of a Value.
type Kind uint8

// Value kinds. The zero Kind is Null.
const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindBytes
	KindList
	KindMap
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
type Value struct {
	kind Kind
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

// Float returns the float payload. KindInt is widened for convenience.
func (v Value) Float() float64 {
	switch v.kind {
	case KindFloat:
		return math.Float64frombits(v.num)
	case KindInt:
		return float64(int64(v.num)) //nolint:gosec // reverses the bit-store in Int()
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
// compare as unequal (use application-level comparison for those). Int and
// Float cross-compare numerically.
func (v Value) EqualScalar(o Value) bool {
	switch v.kind {
	case KindNull:
		return o.kind == KindNull
	case KindBool:
		return o.kind == KindBool && v.num == o.num
	case KindInt:
		switch o.kind {
		case KindInt:
			return v.num == o.num
		case KindFloat:
			return float64(int64(v.num)) == o.Float() //nolint:gosec // reverses the bit-store in Int()
		}
		return false
	case KindFloat:
		switch o.kind {
		case KindFloat:
			return v.Float() == o.Float()
		case KindInt:
			return v.Float() == float64(int64(o.num)) //nolint:gosec // reverses the bit-store in Int()
		}
		return false
	case KindString, KindBytes:
		return (o.kind == KindString || o.kind == KindBytes) && string(v.str) == string(o.str)
	default:
		return false
	}
}
