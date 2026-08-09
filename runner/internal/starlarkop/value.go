// Package starlarkop is the `starlark` transform step (ADR-0052, implementing
// ADR-0017 tier 1): small user transforms, run in-process, fuel-metered, with
// no I/O and no ambient clock or randomness.
//
// It lives in the runner rather than in `engine` for a reason worth keeping:
// the engine is stdlib-only and a leaf, and it stays that way. The operator
// attaches through the public `stream.Pipeline.Apply`, so the engine never
// learns that Starlark exists.
//
// Values are ADAPTED, not converted. A record is exposed to a script through
// wrappers that read the underlying `record.Value` in place, so a script
// touching two fields of a fifty-field record pays for two fields, and nothing
// is ever materialised into map[string]interface{} — the pattern ADR-0004
// exists to prevent, and the reason JSONata was rejected as a runtime
// (ADR-0052).
package starlarkop

import (
	"errors"
	"fmt"

	"go.starlark.net/starlark"

	"github.com/aaron-au/shift/engine/record"
)

// wrap exposes a record.Value to Starlark without copying it.
//
// The returned value is only valid while the underlying batch is: scripts run
// inside the operator callback and nothing they return retains a wrapper, so
// the batch-lifetime contract holds. A script that stores a wrapper in a
// module-level variable would violate it, which is why the thread is discarded
// after every record and globals are frozen.
func wrap(v record.Value) starlark.Value {
	switch v.Kind() {
	case record.KindNull:
		return starlark.None
	case record.KindBool:
		return starlark.Bool(v.Bool())
	case record.KindInt:
		return starlark.MakeInt64(v.Int())
	case record.KindFloat:
		return starlark.Float(v.Float())
	case record.KindString:
		return starlark.String(v.Bytes())
	case record.KindBytes:
		return starlark.Bytes(v.Bytes())
	case record.KindDecimal:
		return Decimal{v: v}
	case record.KindTimestamp, record.KindDate, record.KindTime:
		return Temporal{v: v}
	case record.KindMap:
		return &Map{v: v}
	case record.KindList:
		return &List{v: v}
	default:
		return starlark.None
	}
}

// --- map ------------------------------------------------------------------

// Map is a read-only view of a record map. Writes go to a script's own dict,
// never back into the flowing batch: an operator that mutated the batch under
// a script would make the batch-lifetime contract a script author's problem.
type Map struct {
	v record.Value
}

var (
	_ starlark.Value           = (*Map)(nil)
	_ starlark.Mapping         = (*Map)(nil)
	_ starlark.IterableMapping = (*Map)(nil)
	_ starlark.Sequence        = (*Map)(nil)
	_ starlark.HasAttrs        = (*Map)(nil)
)

func (m *Map) String() string        { return fmt.Sprintf("<record with %d fields>", m.v.Len()) }
func (m *Map) Type() string          { return "record" }
func (m *Map) Freeze()               {} // already immutable
func (m *Map) Truth() starlark.Bool  { return starlark.Bool(m.v.Len() > 0) }
func (m *Map) Len() int              { return m.v.Len() }
func (m *Map) Hash() (uint32, error) { return 0, errors.New("record is not hashable") }

// Get implements rec["field"]. A missing field is absent rather than an error,
// matching how the rest of the engine treats a path miss.
func (m *Map) Get(k starlark.Value) (starlark.Value, bool, error) {
	name, ok := starlark.AsString(k)
	if !ok {
		return nil, false, fmt.Errorf("record keys are strings, got %s", k.Type())
	}
	fv, found := m.v.Field(name)
	if !found {
		return nil, false, nil
	}
	return wrap(fv), true, nil
}

// Attr implements rec.field, which reads better than rec["field"] for the
// ordinary case.
func (m *Map) Attr(name string) (starlark.Value, error) {
	switch name {
	case "keys":
		return starlark.NewBuiltin("keys", m.keysBuiltin), nil
	case "get":
		return starlark.NewBuiltin("get", m.getBuiltin), nil
	}
	fv, found := m.v.Field(name)
	if !found {
		return nil, nil // absent attribute, not an error: Starlark reports it
	}
	return wrap(fv), nil
}

func (m *Map) AttrNames() []string {
	out := make([]string, 0, m.v.Len()+2)
	for i := range m.v.Len() {
		out = append(out, string(m.v.KeyAt(i)))
	}
	return append(out, "get", "keys")
}

func (m *Map) keysBuiltin(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kw []starlark.Tuple) (starlark.Value, error) {
	if err := starlark.UnpackArgs(b.Name(), args, kw); err != nil {
		return nil, err
	}
	elems := make([]starlark.Value, m.v.Len())
	for i := range m.v.Len() {
		elems[i] = starlark.String(m.v.KeyAt(i))
	}
	return starlark.NewList(elems), nil
}

func (m *Map) getBuiltin(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kw []starlark.Tuple) (starlark.Value, error) {
	var key starlark.Value
	var def starlark.Value = starlark.None
	if err := starlark.UnpackPositionalArgs(b.Name(), args, kw, 1, &key, &def); err != nil {
		return nil, err
	}
	got, found, err := m.Get(key)
	if err != nil || !found {
		return def, err
	}
	return got, nil
}

// Items and Iterate let a script walk fields in the record's own order, which
// the record model preserves.
func (m *Map) Items() []starlark.Tuple {
	out := make([]starlark.Tuple, m.v.Len())
	for i := range m.v.Len() {
		out[i] = starlark.Tuple{starlark.String(m.v.KeyAt(i)), wrap(m.v.Index(i))}
	}
	return out
}

func (m *Map) Iterate() starlark.Iterator { return &keyIter{m: m} }

type keyIter struct {
	m *Map
	i int
}

func (it *keyIter) Next(p *starlark.Value) bool {
	if it.i >= it.m.v.Len() {
		return false
	}
	*p = starlark.String(it.m.v.KeyAt(it.i))
	it.i++
	return true
}

func (it *keyIter) Done() {}

// --- list -----------------------------------------------------------------

// List is a read-only view of a record list.
type List struct {
	v record.Value
}

var (
	_ starlark.Value     = (*List)(nil)
	_ starlark.Indexable = (*List)(nil)
	_ starlark.Sequence  = (*List)(nil)
)

func (l *List) String() string        { return fmt.Sprintf("<list of %d>", l.v.Len()) }
func (l *List) Type() string          { return "list" }
func (l *List) Freeze()               {}
func (l *List) Truth() starlark.Bool  { return starlark.Bool(l.v.Len() > 0) }
func (l *List) Hash() (uint32, error) { return 0, errors.New("list is not hashable") }
func (l *List) Len() int              { return l.v.Len() }
func (l *List) Index(i int) starlark.Value {
	return wrap(l.v.Index(i))
}
func (l *List) Iterate() starlark.Iterator { return &elemIter{l: l} }

type elemIter struct {
	l *List
	i int
}

func (it *elemIter) Next(p *starlark.Value) bool {
	if it.i >= it.l.v.Len() {
		return false
	}
	*p = it.l.Index(it.i)
	it.i++
	return true
}

func (it *elemIter) Done() {}
