package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// Map (the declarative mapper, ADR-0027) rebuilds each record into a new,
// possibly nested, target shape from a list of field assignments. Each field
// writes one value at a dotted output path (so "customer.name" nests under a
// "customer" map); the value comes from a source path, a constant, or a concat
// expression, with an optional default (when the source is missing/null) and an
// optional inline type coercion.
//
// The target structure is compiled once into a tree; per record only the leaf
// values are recomputed and emitted, so there is no per-record map allocation
// on the hot path. Like Project, records are rebuilt in place into the flowing
// batch's arena (append-only until Reset), referencing source values without
// copying them.

// MapPart is one piece of a concat expression: a source path or a literal.
type MapPart struct {
	Path   record.Path
	IsPath bool
	Lit    string
}

// MapField is one output assignment. Exactly one source (From / Const / Concat)
// applies; Default and To are optional modifiers.
type MapField struct {
	Out []string // target path segments (≥1); nested maps for >1

	From    record.Path
	FromSet bool

	Const    record.Value
	ConstSet bool

	Concat []MapPart // non-empty ⇒ a concat expression (always yields a string)

	Default    record.Value
	DefaultSet bool

	To    record.Kind
	ToSet bool
}

// Map appends the declarative mapper operator.
func (p *Pipeline) Map(fields []MapField) *Pipeline {
	root, err := buildMapTree(fields)
	if err != nil {
		return p.fail(err)
	}
	var scratch []byte
	return p.Apply("map", func(_ context.Context, b *record.Batch) (*record.Batch, error) {
		recs := b.Records()
		bld := b.Builder()
		for i := range recs {
			rec := recs[i]
			if err := emitMapNode(bld, root, fields, rec, &scratch); err != nil {
				return nil, err
			}
			recs[i] = bld.Finish()
		}
		return b, nil
	})
}

// mapNode is a compiled target-shape node: a leaf (field index) or a branch
// (ordered child keys → subnodes).
type mapNode struct {
	field int // ≥0 ⇒ leaf; -1 ⇒ branch
	keys  [][]byte
	kids  []*mapNode
}

// buildMapTree compiles the field list into the target shape, rejecting an
// output path that collides with another (one a prefix of the other, or a
// duplicate).
func buildMapTree(fields []MapField) (*mapNode, error) {
	root := &mapNode{field: -1}
	for fi := range fields {
		segs := fields[fi].Out
		if len(segs) == 0 {
			return nil, fmt.Errorf("map field %d: empty output path", fi)
		}
		cur := root
		for si, seg := range segs {
			last := si == len(segs)-1
			// find existing child
			idx := -1
			for k, kb := range cur.keys {
				if string(kb) == seg {
					idx = k
					break
				}
			}
			if idx < 0 {
				n := &mapNode{field: -1}
				if last {
					n.field = fi
				}
				cur.keys = append(cur.keys, []byte(seg))
				cur.kids = append(cur.kids, n)
				cur = n
				continue
			}
			child := cur.kids[idx]
			if last || child.field >= 0 {
				return nil, fmt.Errorf("map: output path %q collides with another field", strings.Join(segs, "."))
			}
			cur = child
		}
	}
	return root, nil
}

func emitMapNode(bld *record.Builder, n *mapNode, fields []MapField, rec record.Value, scratch *[]byte) error {
	if n.field >= 0 {
		return emitField(bld, &fields[n.field], rec, scratch)
	}
	bld.BeginMap()
	for i := range n.keys {
		bld.KeyNoCopy(n.keys[i]) // keys stable for the pipeline lifetime
		if err := emitMapNode(bld, n.kids[i], fields, rec, scratch); err != nil {
			return err
		}
	}
	bld.EndMap()
	return nil
}

func emitField(bld *record.Builder, f *MapField, rec record.Value, scratch *[]byte) error {
	switch {
	case len(f.Concat) > 0:
		*scratch = (*scratch)[:0]
		for _, part := range f.Concat {
			if part.IsPath {
				if v, ok := part.Path.Get(rec); ok {
					var err error
					if *scratch, err = appendStringified(*scratch, v); err != nil {
						return err
					}
				}
			} else {
				*scratch = append(*scratch, part.Lit...)
			}
		}
		// A concat yields a string; coerce it if a non-string target is asked.
		if f.ToSet && f.To != record.KindString {
			nv, err := coerceValue(bld, record.UnsafeString(*scratch), f.To)
			if err != nil {
				return err
			}
			bld.Value(nv)
			return nil
		}
		bld.String(*scratch) // copies into the arena
		return nil
	case f.FromSet:
		v, ok := f.From.Get(rec)
		if !ok || v.IsNull() {
			return emitFallback(bld, f, scratch)
		}
		return emitCoerced(bld, v, f, scratch)
	case f.ConstSet:
		return emitCoerced(bld, f.Const, f, scratch)
	default:
		bld.Null()
		return nil
	}
}

func emitFallback(bld *record.Builder, f *MapField, scratch *[]byte) error {
	if f.DefaultSet {
		return emitCoerced(bld, f.Default, f, scratch)
	}
	bld.Null()
	return nil
}

// emitCoerced writes v, applying the inline coercion if set. String targets are
// formatted inline (coerceValue's string path uses the builder and would
// corrupt the in-progress map); non-string targets use coerceValue, which does
// not touch the builder.
func emitCoerced(bld *record.Builder, v record.Value, f *MapField, scratch *[]byte) error {
	if !f.ToSet {
		bld.Value(v)
		return nil
	}
	if f.To == record.KindString {
		*scratch = (*scratch)[:0]
		var err error
		if *scratch, err = appendStringified(*scratch, v); err != nil {
			return err
		}
		bld.String(*scratch)
		return nil
	}
	nv, err := coerceValue(bld, v, f.To)
	if err != nil {
		return err
	}
	bld.Value(nv)
	return nil
}

// appendStringified renders a scalar value into dst (for concat and string
// coercion). A container is an error; null renders as empty.
func appendStringified(dst []byte, v record.Value) ([]byte, error) {
	switch v.Kind() {
	case record.KindString, record.KindBytes:
		return append(dst, v.Bytes()...), nil
	case record.KindInt:
		return strconv.AppendInt(dst, v.Int(), 10), nil
	case record.KindFloat:
		return strconv.AppendFloat(dst, v.Float(), 'g', -1, 64), nil
	case record.KindBool:
		return strconv.AppendBool(dst, v.Bool()), nil
	case record.KindNull:
		return dst, nil
	default:
		return dst, fmt.Errorf("map: cannot render %v as string", v.Kind())
	}
}
