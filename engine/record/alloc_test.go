package record

// Allocation budgets for the record model (TC-006).
//
// TestBatchResetReuses already asserts that a warmed batch refills a flat
// record for ~0 allocations. This file extends that to the shapes the engine
// actually carries: nested maps, lists, the ADR-0051 exact kinds, and
// CopyValue — the one sanctioned way to retain data past a batch's lifetime,
// and therefore the allocation path every blocking operator (aggregate, join)
// and every checkpoint runs per retained record.
//
// Every budget here is 0, asserted exactly. That is not aspirational: the whole
// point of the arena/slab design is that a warmed batch hands out memory it
// already owns, so the first non-zero figure is a regression, not a tolerance
// to widen. Figures were identical with and without -race.

import (
	"testing"
	"time"
)

const (
	// allocWarmupRounds is how many full refills happen before measurement, so
	// the chunk doubling in arena.go has converged and what is measured is the
	// steady state rather than growth.
	allocWarmupRounds = 100
	// allocBatchRecords per refill: large enough that one allocation per record
	// reads as 128, not as noise near a budget of 0.
	allocBatchRecords = 128
)

// buildNestedOrder builds the shape the engine is designed for — a map holding
// scalars, a nested map, a nested list, and a list OF maps — which between them
// exercise every allocator: the byte arena (keys and strings), the value slab
// (children) and the key slab (field names).
func buildNestedOrder(b *Batch, i int) Value {
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(int64(i))
	bld.KeyLiteral("sku")
	bld.StringLiteral("AB-1234-XYZ")
	bld.KeyLiteral("addr")
	bld.BeginMap()
	bld.KeyLiteral("city")
	bld.StringLiteral("Melbourne")
	bld.KeyLiteral("geo")
	bld.BeginList()
	bld.Float(-37.8136)
	bld.Float(144.9631)
	bld.EndList()
	bld.EndMap()
	bld.KeyLiteral("lines")
	bld.BeginList()
	for j := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("sku")
		bld.String([]byte("L-0001"))
		bld.KeyLiteral("qty")
		bld.Int(int64(j))
		bld.EndMap()
	}
	bld.EndList()
	bld.KeyLiteral("note")
	bld.Bytes([]byte("raw-bytes-payload"))
	bld.EndMap()
	return bld.Finish()
}

// buildExactKinds builds a record of the ADR-0051 kinds. Their whole design is
// that scale and zone offset ride in the alignment padding after Value.kind, so
// exactness costs no allocation; a regression to a boxed representation would
// show up here and nowhere else.
func buildExactKinds(b *Batch, i int) Value {
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(int64(i)*10111, 4)
	bld.KeyLiteral("ts")
	bld.Timestamp(int64(i)*1e9, 10*time.Hour)
	bld.KeyLiteral("ts_at")
	bld.TimestampAt(time.Unix(int64(i), 0).UTC())
	bld.KeyLiteral("day")
	bld.Date(int64(i))
	bld.KeyLiteral("clock")
	bld.TimeOfDay(int64(i) * 1e6)
	bld.EndMap()
	return bld.Finish()
}

// assertRefillIsFree drives fill over a warmed batch and fails on any
// allocation. fill must Reset-and-refill: measuring a batch that only ever
// grows would measure arena growth instead of the steady state.
func assertRefillIsFree(t *testing.T, what string, fill func(*Batch)) {
	t.Helper()
	b := NewBatch()
	for range allocWarmupRounds {
		b.Reset()
		fill(b)
	}
	n := testing.AllocsPerRun(allocWarmupRounds, func() {
		b.Reset()
		fill(b)
	})
	if n != 0 {
		t.Errorf("%s allocates %.1f times per batch of %d records (%.3f per record), want 0",
			what, n, allocBatchRecords, n/allocBatchRecords)
	}
}

func TestBuildingNestedRecordsIntoAWarmedBatchIsFree(t *testing.T) {
	assertRefillIsFree(t, "nested map/list build", func(b *Batch) {
		for i := range allocBatchRecords {
			b.Append(buildNestedOrder(b, i))
		}
	})
}

func TestBuildingTheExactKindsIsFree(t *testing.T) {
	assertRefillIsFree(t, "exact-kind build", func(b *Batch) {
		for i := range allocBatchRecords {
			b.Append(buildExactKinds(b, i))
		}
	})
}

// CopyValue is the retention path: aggregate group keys, join build sides,
// pipe hand-offs. It runs once per retained record, so an allocation here is
// paid at record rate by every blocking operator in the engine.
func TestCopyValueIntoAWarmedBatchIsFree(t *testing.T) {
	src := NewBatch()
	for i := range allocBatchRecords {
		src.Append(buildNestedOrder(src, i))
	}
	assertRefillIsFree(t, "CopyValue of a nested record", func(b *Batch) {
		for i := range allocBatchRecords {
			b.Append(CopyValue(b, src.Record(i)))
		}
	})

	exact := NewBatch()
	for i := range allocBatchRecords {
		exact.Append(buildExactKinds(exact, i))
	}
	// The exact kinds are inline scalars, so CopyValue returns them by value
	// and only the enclosing map costs anything — still nothing, once warmed.
	assertRefillIsFree(t, "CopyValue of exact kinds", func(b *Batch) {
		for i := range allocBatchRecords {
			b.Append(CopyValue(b, exact.Record(i)))
		}
	})
}

// Reading is on the hot path too: every operator resolves compiled paths and
// named fields per record. Field lookup compares key bytes against a string
// without converting either (value.go), and Path.Get walks the same views, so
// both must be free — a regression to a []byte(name) conversion in a lookup
// would cost one allocation per field per record.
func TestReadingRecordsIsFree(t *testing.T) {
	b := NewBatch()
	rec := buildNestedOrder(b, 1)
	city := MustParsePath("$.addr.city")
	lat := MustParsePath("$.addr.geo[0]")
	qty := MustParsePath("$.lines[2].qty")

	if n := testing.AllocsPerRun(allocWarmupRounds, func() {
		if _, ok := city.Get(rec); !ok {
			t.Fatal("path $.addr.city missed")
		}
		if _, ok := lat.Get(rec); !ok {
			t.Fatal("path $.addr.geo[0] missed")
		}
		if _, ok := qty.Get(rec); !ok {
			t.Fatal("path $.lines[2].qty missed")
		}
		if _, ok := rec.Field("sku"); !ok {
			t.Fatal("field sku missed")
		}
		if _, ok := rec.Field("nope"); ok {
			t.Fatal("field nope found")
		}
	}); n != 0 {
		t.Errorf("reading a record allocates %.1f times per pass, want 0", n)
	}
}
