package record

import (
	"fmt"
	"testing"
)

// buildOrder builds {"id":7,"name":"ada","tags":["x","y"],"addr":{"city":"melb","po":false},"score":1.5}
func buildOrder(b *Batch) Value {
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(7)
	bld.KeyLiteral("name")
	bld.StringLiteral("ada")
	bld.KeyLiteral("tags")
	bld.BeginList()
	bld.StringLiteral("x")
	bld.StringLiteral("y")
	bld.EndList()
	bld.KeyLiteral("addr")
	bld.BeginMap()
	bld.KeyLiteral("city")
	bld.StringLiteral("melb")
	bld.KeyLiteral("po")
	bld.Bool(false)
	bld.EndMap()
	bld.KeyLiteral("score")
	bld.Float(1.5)
	bld.EndMap()
	return bld.Finish()
}

func TestBuilderAndAccess(t *testing.T) {
	b := NewBatch()
	v := buildOrder(b)
	b.Append(v)

	if v.Kind() != KindMap || v.Len() != 5 {
		t.Fatalf("root kind=%v len=%d, want map/5", v.Kind(), v.Len())
	}
	id, ok := v.Field("id")
	if !ok || id.Int() != 7 {
		t.Errorf("id = %v %v, want 7", id, ok)
	}
	name, _ := v.Field("name")
	if name.String() != "ada" {
		t.Errorf("name = %q, want ada", name.String())
	}
	tags, _ := v.Field("tags")
	if tags.Kind() != KindList || tags.Len() != 2 || tags.Index(1).String() != "y" {
		t.Errorf("tags wrong: %v", tags)
	}
	addr, _ := v.Field("addr")
	city, _ := addr.Field("city")
	if city.String() != "melb" {
		t.Errorf("city = %q", city.String())
	}
	if po, _ := addr.Field("po"); po.Bool() {
		t.Error("po should be false")
	}
	score, _ := v.Field("score")
	if score.Float() != 1.5 {
		t.Errorf("score = %v", score.Float())
	}
	if _, ok := v.Field("missing"); ok {
		t.Error("missing field found")
	}
	// field order preserved
	if string(v.KeyAt(0)) != "id" || string(v.KeyAt(4)) != "score" {
		t.Error("field order not preserved")
	}
}

func TestBatchResetReuses(t *testing.T) {
	b := NewBatch()
	for range 3 {
		for range 100 {
			b.Append(buildOrder(b))
		}
		if b.Len() != 100 {
			t.Fatalf("len = %d", b.Len())
		}
		b.Reset()
		if b.Len() != 0 {
			t.Fatal("reset did not clear records")
		}
	}
	// A warmed batch should not allocate on refill.
	b.Reset()
	allocs := testing.AllocsPerRun(10, func() {
		b.Reset()
		for range 100 {
			b.Append(buildOrder(b))
		}
	})
	// Allow a small epsilon for map growth internals; steady state should be ~0.
	if allocs > 5 {
		t.Errorf("warmed batch allocates %.1f allocs/run, want ~0", allocs)
	}
}

func TestArenaViewsStableAcrossGrowth(t *testing.T) {
	b := NewBatch()
	bld := b.Builder()
	// Force many chunk growths and verify early views still read correctly.
	var first Value
	for i := range 10000 {
		bld.BeginMap()
		bld.KeyLiteral("k")
		bld.StringLiteral(fmt.Sprintf("val-%d-%s", i, string(make([]byte, 100))))
		bld.EndMap()
		v := bld.Finish()
		if i == 0 {
			first = v
		}
		b.Append(v)
	}
	f, _ := first.Field("k")
	if got := f.String()[:6]; got != "val-0-" {
		t.Errorf("early arena view corrupted: %q", got)
	}
}

func TestCopyValueAcrossBatches(t *testing.T) {
	src := NewBatch()
	v := buildOrder(src)
	dst := NewBatch()
	cp := CopyValue(dst, v)
	src.Reset() // src memory recycled; copy must survive
	// overwrite src arena to catch aliasing
	for range 50 {
		dst2 := buildOrder(src)
		_ = dst2
	}
	name, _ := cp.Field("name")
	if name.String() != "ada" {
		t.Errorf("copied value corrupted: %q", name.String())
	}
	addr, _ := cp.Field("addr")
	city, _ := addr.Field("city")
	if city.String() != "melb" {
		t.Errorf("nested copy corrupted: %q", city.String())
	}
}

func TestEqualScalar(t *testing.T) {
	cases := []struct {
		a, b Value
		want bool
	}{
		{Int(3), Int(3), true},
		{Int(3), Float(3.0), true},
		{Float(2.5), Int(2), false},
		{Bool(true), Bool(true), true},
		{Null(), Null(), true},
		{Int(1), Null(), false},
		{UnsafeString([]byte("a")), UnsafeString([]byte("a")), true},
		{UnsafeString([]byte("a")), UnsafeString([]byte("b")), false},
	}
	for i, c := range cases {
		if got := c.a.EqualScalar(c.b); got != c.want {
			t.Errorf("case %d: EqualScalar = %v, want %v", i, got, c.want)
		}
	}
}

func TestBuilderPanics(t *testing.T) {
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected panic", name)
			}
		}()
		fn()
	}
	expectPanic("value without key", func() {
		b := NewBatch().Builder()
		b.BeginMap()
		b.Int(1)
	})
	expectPanic("double key", func() {
		b := NewBatch().Builder()
		b.BeginMap()
		b.KeyLiteral("a")
		b.KeyLiteral("b")
	})
	expectPanic("mismatched end", func() {
		b := NewBatch().Builder()
		b.BeginList()
		b.EndMap()
	})
	expectPanic("finish with open container", func() {
		b := NewBatch().Builder()
		b.BeginList()
		b.Finish()
	})
}

func TestPath(t *testing.T) {
	b := NewBatch()
	v := buildOrder(b)

	get := func(expr string) (Value, bool) {
		t.Helper()
		p, err := ParsePath(expr)
		if err != nil {
			t.Fatalf("parse %q: %v", expr, err)
		}
		return p.Get(v)
	}
	if got, ok := get("$.id"); !ok || got.Int() != 7 {
		t.Error("$.id failed")
	}
	if got, ok := get("$.addr.city"); !ok || got.String() != "melb" {
		t.Error("$.addr.city failed")
	}
	if got, ok := get("$.tags[1]"); !ok || got.String() != "y" {
		t.Error("$.tags[1] failed")
	}
	if _, ok := get("$.tags[9]"); ok {
		t.Error("$.tags[9] should miss")
	}
	if _, ok := get("$.nope.deep"); ok {
		t.Error("$.nope.deep should miss")
	}
	if got, ok := get("$"); !ok || got.Kind() != KindMap {
		t.Error("$ (root) failed")
	}

	for _, bad := range []string{"", "id", "$.", "$[x]", "$[", "$.a[-1]"} {
		if _, err := ParsePath(bad); err == nil {
			t.Errorf("ParsePath(%q) should fail", bad)
		}
	}
	if MustParsePath("$.addr.city").LeafName() != "city" {
		t.Error("LeafName failed")
	}
}

func BenchmarkBuildRecord(b *testing.B) {
	batch := NewBatch()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%1000 == 0 {
			batch.Reset()
		}
		batch.Append(buildOrder(batch))
	}
}

func BenchmarkPathGet(b *testing.B) {
	batch := NewBatch()
	v := buildOrder(batch)
	p := MustParsePath("$.addr.city")
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := p.Get(v); !ok {
			b.Fatal("miss")
		}
	}
}
