package record

import "fmt"

// Builder constructs values inside a Batch. It is stack-based: scalars and
// containers are emitted depth-first; when a container closes, its children
// are copied from scratch space into an exact-size slab slice, so finished
// values are contiguous and the scratch is reused.
//
// Typical use:
//
//	bld := batch.Builder()
//	bld.BeginMap()
//	bld.Key([]byte("id"))
//	bld.Int(42)
//	bld.EndMap()
//	batch.Append(bld.Finish())
type Builder struct {
	batch *Batch
	// scratch stacks; frames mark container starts.
	vals   []Value
	keys   [][]byte
	frames []frame
}

type frame struct {
	isMap      bool
	valStart   int
	keyStart   int
	pendingKey bool
}

func (b *Builder) reset() {
	b.vals = b.vals[:0]
	b.keys = b.keys[:0]
	b.frames = b.frames[:0]
}

func (b *Builder) push(v Value) {
	b.vals = append(b.vals, v)
	if n := len(b.frames); n > 0 && b.frames[n-1].isMap {
		fr := &b.frames[n-1]
		if !fr.pendingKey {
			panic("record: map value emitted without a preceding Key")
		}
		fr.pendingKey = false
	}
}

// Null emits a null value.
func (b *Builder) Null() { b.push(Null()) }

// Bool emits a boolean value.
func (b *Builder) Bool(v bool) { b.push(Bool(v)) }

// Int emits an integer value.
func (b *Builder) Int(v int64) { b.push(Int(v)) }

// Float emits a float value.
func (b *Builder) Float(v float64) { b.push(Float(v)) }

// String emits a string value, copying s into the batch arena.
func (b *Builder) String(s []byte) {
	b.push(Value{kind: KindString, str: b.batch.arena.copyIn(s)})
}

// StringLiteral emits a string value from a Go string (copied into the
// arena).
func (b *Builder) StringLiteral(s string) {
	b.push(Value{kind: KindString, str: b.batch.arena.copyInString(s)})
}

// Bytes emits a bytes value, copying data into the batch arena.
func (b *Builder) Bytes(data []byte) {
	b.push(Value{kind: KindBytes, str: b.batch.arena.copyIn(data)})
}

// Value emits an already-built value. The value must either be scalar or
// belong to the same batch (its children/backing bytes are referenced, not
// copied) — passing values from another batch violates the lifetime
// contract; use CopyValue for that.
func (b *Builder) Value(v Value) { b.push(v) }

// Key sets the field name for the next value in the open map. k is copied
// into the arena.
func (b *Builder) Key(k []byte) {
	n := len(b.frames)
	if n == 0 || !b.frames[n-1].isMap {
		panic("record: Key outside a map")
	}
	fr := &b.frames[n-1]
	if fr.pendingKey {
		panic("record: two Keys without a value between them")
	}
	fr.pendingKey = true
	b.keys = append(b.keys, b.batch.arena.copyIn(k))
}

// KeyLiteral is Key for a Go string.
func (b *Builder) KeyLiteral(k string) {
	n := len(b.frames)
	if n == 0 || !b.frames[n-1].isMap {
		panic("record: Key outside a map")
	}
	fr := &b.frames[n-1]
	if fr.pendingKey {
		panic("record: two Keys without a value between them")
	}
	fr.pendingKey = true
	b.keys = append(b.keys, b.batch.arena.copyInString(k))
}

// KeyNoCopy sets the field name for the next value WITHOUT copying it into
// the arena. The caller must guarantee k is stable for the batch lifetime
// (e.g. a compile-time field name).
func (b *Builder) KeyNoCopy(k []byte) {
	n := len(b.frames)
	if n == 0 || !b.frames[n-1].isMap {
		panic("record: Key outside a map")
	}
	fr := &b.frames[n-1]
	if fr.pendingKey {
		panic("record: two Keys without a value between them")
	}
	fr.pendingKey = true
	b.keys = append(b.keys, k)
}

// BeginList opens a list container.
func (b *Builder) BeginList() {
	b.frames = append(b.frames, frame{valStart: len(b.vals), keyStart: len(b.keys)})
}

// EndList closes the innermost list.
func (b *Builder) EndList() {
	n := len(b.frames)
	if n == 0 || b.frames[n-1].isMap {
		panic("record: EndList without matching BeginList")
	}
	fr := b.frames[n-1]
	b.frames = b.frames[:n-1]
	kids := b.batch.vals.alloc(len(b.vals) - fr.valStart)
	copy(kids, b.vals[fr.valStart:])
	b.vals = b.vals[:fr.valStart]
	b.push(Value{kind: KindList, kids: kids})
}

// BeginMap opens a map container.
func (b *Builder) BeginMap() {
	b.frames = append(b.frames, frame{isMap: true, valStart: len(b.vals), keyStart: len(b.keys)})
}

// EndMap closes the innermost map.
func (b *Builder) EndMap() {
	n := len(b.frames)
	if n == 0 || !b.frames[n-1].isMap {
		panic("record: EndMap without matching BeginMap")
	}
	fr := b.frames[n-1]
	if fr.pendingKey {
		panic("record: map closed with a Key but no value")
	}
	b.frames = b.frames[:n-1]
	nkids := len(b.vals) - fr.valStart
	kids := b.batch.vals.alloc(nkids)
	copy(kids, b.vals[fr.valStart:])
	keys := b.batch.keys.alloc(nkids)
	copy(keys, b.keys[fr.keyStart:])
	b.vals = b.vals[:fr.valStart]
	b.keys = b.keys[:fr.keyStart]
	b.push(Value{kind: KindMap, kids: kids, keys: keys})
}

// Finish returns the single completed top-level value and resets the
// builder for the next construction.
func (b *Builder) Finish() Value {
	if len(b.frames) != 0 {
		panic(fmt.Sprintf("record: Finish with %d unclosed containers", len(b.frames)))
	}
	if len(b.vals) != 1 {
		panic(fmt.Sprintf("record: Finish expects exactly 1 value, have %d", len(b.vals)))
	}
	v := b.vals[0]
	b.reset()
	return v
}

// CopyValue deep-copies v (which may belong to another batch) into batch
// dst and returns the copy. This is the only sanctioned way to retain data
// beyond its source batch's lifetime (e.g. aggregate state).
func CopyValue(dst *Batch, v Value) Value {
	switch v.kind {
	case KindNull, KindBool, KindInt, KindFloat:
		return v
	case KindString, KindBytes:
		return Value{kind: v.kind, str: dst.arena.copyIn(v.str)}
	case KindList:
		kids := dst.vals.alloc(len(v.kids))
		for i, k := range v.kids {
			kids[i] = CopyValue(dst, k)
		}
		return Value{kind: KindList, kids: kids}
	case KindMap:
		kids := dst.vals.alloc(len(v.kids))
		keys := dst.keys.alloc(len(v.keys))
		for i := range v.kids {
			kids[i] = CopyValue(dst, v.kids[i])
			keys[i] = dst.arena.copyIn(v.keys[i])
		}
		return Value{kind: KindMap, kids: kids, keys: keys}
	default:
		return Null()
	}
}
