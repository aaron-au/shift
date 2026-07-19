package stream

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aaron-au/shift/engine/record"
)

// ProjectField maps a source path to an output field name.
type ProjectField struct {
	// Out is the output field name; empty uses the path's leaf name.
	Out string
	// From locates the source value; missing paths project null.
	From record.Path
}

// Project rebuilds each record as a flat map of the given fields, in order.
// Values are referenced, not copied — they stay backed by the same batch.
func (p *Pipeline) Project(fields ...ProjectField) *Pipeline {
	keys := make([][]byte, len(fields))
	for i, f := range fields {
		name := f.Out
		if name == "" {
			name = f.From.LeafName()
		}
		if name == "" {
			return p.fail(fmt.Errorf("stream: project field %d (%s) needs an output name", i, f.From))
		}
		keys[i] = []byte(name)
	}
	return p.Apply("project", func(_ context.Context, b *record.Batch) (*record.Batch, error) {
		recs := b.Records()
		bld := b.Builder()
		for i, rec := range recs {
			bld.BeginMap()
			for j, f := range fields {
				bld.KeyNoCopy(keys[j]) // stable for the pipeline lifetime
				if v, ok := f.From.Get(rec); ok {
					bld.Value(v)
				} else {
					bld.Null()
				}
			}
			bld.EndMap()
			recs[i] = bld.Finish()
		}
		return b, nil
	})
}

// Filter keeps records where pred returns true, compacting in place.
func (p *Pipeline) Filter(name string, pred func(record.Value) bool) *Pipeline {
	return p.Apply(name, func(_ context.Context, b *record.Batch) (*record.Batch, error) {
		recs := b.Records()
		kept := recs[:0]
		for _, rec := range recs {
			if pred(rec) {
				kept = append(kept, rec)
			}
		}
		b.SetRecords(kept)
		return b, nil
	})
}

// CoerceRule converts a top-level field to a target kind.
type CoerceRule struct {
	Field string
	To    record.Kind
}

// Coerce converts top-level fields to target kinds in place. Null values
// pass through untouched; a value that cannot convert is an error tagged
// with the operator (OpError) so the runner can route it to the step's
// onFailure handler (ADR-0013).
func (p *Pipeline) Coerce(rules ...CoerceRule) *Pipeline {
	return p.Apply("coerce", func(_ context.Context, b *record.Batch) (*record.Batch, error) {
		bld := b.Builder()
		for _, rec := range b.Records() {
			if rec.Kind() != record.KindMap {
				return nil, fmt.Errorf("coerce: record is %v, want map", rec.Kind())
			}
			for i := range rec.Len() {
				key := rec.KeyAt(i)
				for _, r := range rules {
					if string(key) != r.Field {
						continue
					}
					nv, err := coerceValue(bld, rec.Index(i), r.To)
					if err != nil {
						return nil, fmt.Errorf("coerce %s: %w", r.Field, err)
					}
					rec.SetIndex(i, nv)
				}
			}
		}
		return b, nil
	})
}

func coerceValue(bld *record.Builder, v record.Value, to record.Kind) (record.Value, error) {
	if v.IsNull() || v.Kind() == to {
		return v, nil
	}
	switch to {
	case record.KindInt:
		switch v.Kind() {
		case record.KindFloat:
			return record.Int(int64(v.Float())), nil
		case record.KindString:
			n, err := strconv.ParseInt(v.String(), 10, 64)
			if err != nil {
				return record.Value{}, fmt.Errorf("cannot parse %q as int", v.String())
			}
			return record.Int(n), nil
		case record.KindBool:
			if v.Bool() {
				return record.Int(1), nil
			}
			return record.Int(0), nil
		}
	case record.KindFloat:
		switch v.Kind() {
		case record.KindInt:
			return record.Float(float64(v.Int())), nil
		case record.KindString:
			f, err := strconv.ParseFloat(v.String(), 64)
			if err != nil {
				return record.Value{}, fmt.Errorf("cannot parse %q as float", v.String())
			}
			return record.Float(f), nil
		}
	case record.KindBool:
		if v.Kind() == record.KindString {
			switch v.String() {
			case "true", "1":
				return record.Bool(true), nil
			case "false", "0":
				return record.Bool(false), nil
			}
			return record.Value{}, fmt.Errorf("cannot parse %q as bool", v.String())
		}
	case record.KindString:
		bld.BeginList() // scratch container so the builder owns the arena copy
		switch v.Kind() {
		case record.KindInt:
			bld.StringLiteral(strconv.FormatInt(v.Int(), 10))
		case record.KindFloat:
			bld.StringLiteral(strconv.FormatFloat(v.Float(), 'g', -1, 64))
		case record.KindBool:
			bld.StringLiteral(strconv.FormatBool(v.Bool()))
		case record.KindBytes:
			bld.Value(v) // representation change only
			bld.EndList()
			lst := bld.Finish()
			return lst.Index(0), nil
		default:
			bld.EndList()
			bld.Finish()
			return record.Value{}, fmt.Errorf("cannot render %v as string", v.Kind())
		}
		bld.EndList()
		lst := bld.Finish()
		return lst.Index(0), nil
	}
	return record.Value{}, fmt.Errorf("unsupported coercion %v -> %v", v.Kind(), to)
}

// Flatten replaces nested maps with dotted top-level keys (lists are left
// intact). E.g. {"a":{"b":1}} with sep "." becomes {"a.b":1}.
func (p *Pipeline) Flatten(sep string) *Pipeline {
	// One shared key buffer; nesting levels truncate back to their prefix
	// length, so flattening allocates nothing on the steady state.
	var keyScratch []byte
	var flatten func(bld *record.Builder, prefixLen int, v record.Value)
	flatten = func(bld *record.Builder, prefixLen int, v record.Value) {
		for i := range v.Len() {
			keyScratch = keyScratch[:prefixLen]
			if prefixLen > 0 {
				keyScratch = append(keyScratch, sep...)
			}
			keyScratch = append(keyScratch, v.KeyAt(i)...)
			child := v.Index(i)
			if child.Kind() == record.KindMap {
				flatten(bld, len(keyScratch), child)
				continue
			}
			bld.Key(keyScratch) // copied into the batch arena
			bld.Value(child)
		}
	}
	return p.Apply("flatten", func(_ context.Context, b *record.Batch) (*record.Batch, error) {
		recs := b.Records()
		bld := b.Builder()
		for i, rec := range recs {
			if rec.Kind() != record.KindMap {
				return nil, fmt.Errorf("flatten: record is %v, want map", rec.Kind())
			}
			bld.BeginMap()
			flatten(bld, 0, rec)
			bld.EndMap()
			recs[i] = bld.Finish()
		}
		return b, nil
	})
}
