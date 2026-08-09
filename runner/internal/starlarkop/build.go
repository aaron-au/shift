package starlarkop

import (
	"fmt"
	"math"

	"go.starlark.net/starlark"

	"github.com/aaron-au/shift/engine/record"
)

// build converts a script's return value into a record.Value in dst's arena.
//
// Bounded on both field count and depth: a script is arbitrary code, and an
// accidental (or deliberate) unbounded structure would otherwise turn one
// record into as much memory as the process has. Fuel bounds the WORK a script
// does; these bound what it can HAND BACK, which is a different budget.
func (p *Program) build(dst *record.Batch, v starlark.Value, depth int) (record.Value, error) {
	if depth > p.depth {
		return record.Value{}, fmt.Errorf("returned value nests deeper than %d levels", p.depth)
	}
	bld := dst.Builder()

	switch t := v.(type) {
	case starlark.NoneType:
		return record.Null(), nil
	case starlark.Bool:
		return record.Bool(bool(t)), nil
	case starlark.Int:
		n, ok := t.Int64()
		if !ok {
			return record.Value{}, fmt.Errorf("integer %s does not fit an int64", t.String())
		}
		return record.Int(n), nil
	case starlark.Float:
		f := float64(t)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return record.Value{}, fmt.Errorf("%v has no representation in a record", f)
		}
		return record.Float(f), nil
	case starlark.String:
		return p.scalarInto(bld, func() { bld.StringLiteral(string(t)) })
	case starlark.Bytes:
		return p.scalarInto(bld, func() { bld.Bytes([]byte(t)) })

	// The wrappers come back unchanged: a field a script read and returned
	// untouched keeps its exact kind, scale and zone rather than being
	// round-tripped through a Starlark scalar.
	case Decimal:
		return t.v, nil
	case Temporal:
		return t.v, nil
	case *Map:
		return t.v, nil
	case *List:
		return t.v, nil

	case *starlark.Dict:
		return p.buildDict(dst, t, depth)
	case *starlark.List:
		return p.buildList(dst, t, depth)
	case starlark.Tuple:
		return p.buildSeq(dst, t.Len(), func(i int) starlark.Value { return t.Index(i) }, depth)
	default:
		return record.Value{}, fmt.Errorf("cannot return a %s from a flow script", v.Type())
	}
}

// scalarInto emits one scalar through a scratch container, so the builder owns
// the arena copy (the same trick the coerce operator uses).
func (p *Program) scalarInto(bld *record.Builder, emit func()) (record.Value, error) {
	bld.BeginList()
	emit()
	bld.EndList()
	lst := bld.Finish()
	return lst.Index(0), nil
}

func (p *Program) buildDict(dst *record.Batch, d *starlark.Dict, depth int) (record.Value, error) {
	if d.Len() > p.fields {
		return record.Value{}, fmt.Errorf("returned record has %d fields, limit is %d", d.Len(), p.fields)
	}
	// Children are built before the map is opened: the builder is a stack
	// machine, and a nested build would otherwise interleave with this frame.
	items := d.Items()
	keys := make([]string, len(items))
	vals := make([]record.Value, len(items))
	for i, kv := range items {
		name, ok := starlark.AsString(kv[0])
		if !ok {
			return record.Value{}, fmt.Errorf("record keys must be strings, got %s", kv[0].Type())
		}
		child, err := p.build(dst, kv[1], depth+1)
		if err != nil {
			return record.Value{}, fmt.Errorf("field %q: %w", name, err)
		}
		keys[i], vals[i] = name, child
	}
	bld := dst.Builder()
	bld.BeginMap()
	for i := range keys {
		bld.KeyLiteral(keys[i])
		bld.Value(vals[i])
	}
	bld.EndMap()
	return bld.Finish(), nil
}

func (p *Program) buildList(dst *record.Batch, l *starlark.List, depth int) (record.Value, error) {
	return p.buildSeq(dst, l.Len(), l.Index, depth)
}

func (p *Program) buildSeq(dst *record.Batch, n int, at func(int) starlark.Value, depth int) (record.Value, error) {
	if n > p.fields {
		return record.Value{}, fmt.Errorf("returned list has %d elements, limit is %d", n, p.fields)
	}
	vals := make([]record.Value, n)
	for i := range n {
		child, err := p.build(dst, at(i), depth+1)
		if err != nil {
			return record.Value{}, fmt.Errorf("element %d: %w", i, err)
		}
		vals[i] = child
	}
	bld := dst.Builder()
	bld.BeginList()
	for _, v := range vals {
		bld.Value(v)
	}
	bld.EndList()
	return bld.Finish(), nil
}
