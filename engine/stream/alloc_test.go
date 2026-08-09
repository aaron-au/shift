package stream

// Allocation budgets for the non-blocking operators (TC-006).
//
// "0-alloc steady state on the hot path" is the claim ADR-0003/ADR-0004 and the
// README make, and it is the whole performance thesis. Until this file the only
// evidence was benchmark OUTPUT, and benchmark output gates nothing: the CI
// benchstat job is continue-on-error, so a change adding one allocation per
// record could not fail a build. These are assertions, so it now can.
//
// How to read a budget here:
//
//   - 0 is asserted EXACTLY. Every operator in this package rebuilds records
//     into the flowing batch's own arena, so zero is not aspirational — it is
//     the observed figure, and anything above it is a regression.
//   - A non-zero budget is asserted as an UPPER BOUND with the observed figure
//     stated in the comment above it, and a reason. A bound that nobody can
//     explain is a number somebody raises the first time it fails.
//
// Every measurement drives a WARMED pipeline: allocWarmup batches are pulled
// before AllocsPerRun starts, so the arena/slab chunk doubling in record/arena.go
// has converged and what is left is the steady state. testing.AllocsPerRun
// discards one further round of its own.
//
// The figures below were identical across repeated runs both with and without
// -race (the race detector does not perturb them), so these are exact gates
// rather than tolerance bands.

import (
	"context"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// allocWarmup is how many batches flow through a pipeline before measurement.
// Chunk capacity doubles per growth from 64 KiB, so a handful of rounds is
// already enough; 200 is deliberately far past the point of convergence, since
// an under-warmed measurement would report growth as a leak.
const allocWarmup = 200

// allocRecords per batch. Large enough that a single per-record allocation is
// unmissable against a budget of 0 — a regression shows up as 128, not as noise.
const allocRecords = 128

// allocSource refills one reused batch on every Next, which is what a format
// reader does: Reset, then rebuild. Refilling (rather than handing back the
// same untouched batch) is required, not incidental — operators rebuild records
// into the batch arena, which is append-only until Reset, so a source that
// never Reset would grow without bound and the measurement would be of arena
// growth rather than of the operator.
type allocSource struct {
	b *record.Batch
	n int
}

func newAllocSource() *allocSource {
	return &allocSource{b: record.NewBatch(), n: allocRecords}
}

func (s *allocSource) Next(_ context.Context) (*record.Batch, error) {
	s.b.Reset()
	bld := s.b.Builder()
	for i := range s.n {
		allocRecord(bld, i)
		s.b.Append(bld.Finish())
	}
	return s.b, nil
}

func (s *allocSource) Close() error { return nil }

// allocRecord builds one representative record: scalars, an exact decimal, text
// that the coercions parse, a nested map and a list. Field ids start at 1000 so
// the integer→text renderings below are all 4 digits — strconv returns constant
// strings for 0..99 without allocating, and a fixture drifting across that
// boundary would silently change a budget.
func allocRecord(bld *record.Builder, i int) {
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(int64(1000 + i))
	bld.KeyLiteral("sku")
	bld.StringLiteral("AB-1234")
	bld.KeyLiteral("qty")
	bld.Int(int64(i%7) + 1)
	bld.KeyLiteral("amount")
	bld.Decimal(int64(1000+i)*101, 2)
	bld.KeyLiteral("amount_text")
	bld.StringLiteral("1234.56")
	bld.KeyLiteral("when")
	bld.StringLiteral("2026-08-09T01:02:03Z")
	bld.KeyLiteral("flag")
	bld.StringLiteral("true")
	bld.KeyLiteral("addr")
	bld.BeginMap()
	bld.KeyLiteral("city")
	bld.StringLiteral("Melbourne")
	bld.KeyLiteral("zip")
	bld.StringLiteral("3000")
	bld.EndMap()
	bld.KeyLiteral("tags")
	bld.BeginList()
	bld.StringLiteral("retail")
	bld.StringLiteral("au")
	bld.EndList()
	bld.EndMap()
}

// allocsPerBatch returns the steady-state allocations for one pull through the
// built pipeline — the source, the pipeline plumbing (measuredSource/opSource),
// and every operator. It is the whole per-batch cost of a stage, which is what
// the doctrine's claim is about.
func allocsPerBatch(t *testing.T, build func(*Pipeline) *Pipeline) float64 {
	t.Helper()
	p := New(newAllocSource(), "read")
	if build != nil {
		p = build(p)
	}
	src, err := p.AsSource()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx := context.Background()
	for range allocWarmup {
		if _, err := src.Next(ctx); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}
	return testing.AllocsPerRun(allocWarmup, func() {
		if _, err := src.Next(ctx); err != nil {
			t.Fatalf("measure: %v", err)
		}
	})
}

// assertNoAllocs fails when a stage allocates at all. The message reports the
// per-record figure too, because that is the number that decides whether a
// regression is a one-off (a rebuilt lookup table) or the expensive kind (one
// allocation per record, which is what sinks the streaming claim).
func assertNoAllocs(t *testing.T, stage string, build func(*Pipeline) *Pipeline) {
	t.Helper()
	if n := allocsPerBatch(t, build); n != 0 {
		t.Errorf("%s allocates %.1f times per batch of %d records (%.3f per record), want 0",
			stage, n, allocRecords, n/allocRecords)
	}
}

// The pipeline itself — a source and the per-stage instrumentation wrapped
// around it — must cost nothing. Measured separately so that a failure in an
// operator test below is attributable to the operator and not to the plumbing
// every one of them shares.
func TestThePipelinePlumbingDoesNotAllocate(t *testing.T) {
	assertNoAllocs(t, "source alone", nil)
	assertNoAllocs(t, "tap", func(p *Pipeline) *Pipeline { return p.Tap() })
}

func TestProjectDoesNotAllocate(t *testing.T) {
	assertNoAllocs(t, "project", func(p *Pipeline) *Pipeline {
		return p.Project(
			ProjectField{From: record.MustParsePath("$.id")},
			ProjectField{Out: "city", From: record.MustParsePath("$.addr.city")},
			ProjectField{From: record.MustParsePath("$.amount")},
			ProjectField{Out: "tag", From: record.MustParsePath("$.tags[0]")},
			// A missing path projects null, and must not allocate to say so.
			ProjectField{Out: "missing", From: record.MustParsePath("$.nope")},
		)
	})
}

func TestFilterDoesNotAllocate(t *testing.T) {
	// Filter compacts the record slice in place; keeping roughly half is the
	// case that would expose a copy into a fresh slice.
	assertNoAllocs(t, "filter", func(p *Pipeline) *Pipeline {
		return p.Filter("odd-qty", func(v record.Value) bool {
			q, _ := v.Field("qty")
			return q.Int()%2 == 1
		})
	})
}

func TestCoerceDoesNotAllocate(t *testing.T) {
	// Every conversion whose target is not text: the numeric parses, the exact
	// decimal parse, and all three temporal parses (ADR-0051). Text targets are
	// the exception and are measured separately below.
	assertNoAllocs(t, "coerce", func(p *Pipeline) *Pipeline {
		return p.Coerce(
			CoerceRule{Field: "zip", To: record.KindInt},
			CoerceRule{Field: "amount_text", To: record.KindDecimal},
			CoerceRule{Field: "when", To: record.KindTimestamp},
			CoerceRule{Field: "flag", To: record.KindBool},
			CoerceRule{Field: "qty", To: record.KindFloat},
		)
	})
	assertNoAllocs(t, "coerce date/time", func(p *Pipeline) *Pipeline {
		return p.
			Coerce(CoerceRule{Field: "when", To: record.KindTimestamp}).
			Coerce(CoerceRule{Field: "when", To: record.KindDate})
	})
	// Rendering an exact kind as text goes through Value.AppendText into a
	// stack buffer, so it is free. The int rendering next to it is not — see
	// TestCoercingAnIntToTextAllocatesPerRecord.
	assertNoAllocs(t, "coerce decimal to string", func(p *Pipeline) *Pipeline {
		return p.Coerce(CoerceRule{Field: "amount", To: record.KindString})
	})
}

// Was a defect, now the assertion that keeps it fixed (TC-006).
//
// coerceValue's KindString arm rendered an int with strconv.FormatInt, which
// returns a freshly allocated Go string for anything outside its 0..99 constant
// table — one heap allocation per record, on an operator that runs per record,
// in a project whose whole differentiation is per-record cost. The arm
// immediately below it already rendered decimals and temporals into a stack
// buffer for free, and stream/map.go's appendStringified already did the
// integer case correctly. It was an inconsistency inside one switch, not a cost
// the operation required; ops.go now uses strconv.AppendInt into a stack buffer
// and this measures 0.
func TestCoerceToTextDoesNotAllocate(t *testing.T) {
	// The id field is four digits by construction: strconv returns a constant
	// string for 0..99 and the runtime returns a static string for any
	// one-byte conversion, so a narrower fixture would measure nothing and the
	// test would pass without exercising the allocating path at all.
	assertNoAllocs(t, "coerce int→text", func(p *Pipeline) *Pipeline {
		return p.Coerce(CoerceRule{Field: "id", To: record.KindString})
	})
}

func TestFlattenDoesNotAllocate(t *testing.T) {
	// Flatten builds dotted keys in a shared scratch buffer that is truncated
	// back to each prefix, so the key building must not allocate either.
	assertNoAllocs(t, "flatten", func(p *Pipeline) *Pipeline { return p.Flatten(".") })
}

func TestMapDoesNotAllocate(t *testing.T) {
	// The declarative mapper (ADR-0027) compiles the target shape once; per
	// record only the leaves are re-emitted. Nested output, a concat, a
	// constant, a default for a missing source and an inline coercion are all
	// exercised — the concat and the string coercion share the scratch buffer
	// whose reuse is the thing being asserted.
	assertNoAllocs(t, "map", func(p *Pipeline) *Pipeline {
		return p.Map([]MapField{
			{Out: []string{"id"}, From: record.MustParsePath("$.id"), FromSet: true},
			{Out: []string{"cust", "city"}, From: record.MustParsePath("$.addr.city"), FromSet: true},
			{Out: []string{"cust", "zip"}, From: record.MustParsePath("$.addr.zip"), FromSet: true, To: record.KindInt, ToSet: true},
			{Out: []string{"label"}, Concat: []MapPart{
				{IsPath: true, Path: record.MustParsePath("$.sku")},
				{Lit: "/"},
				{IsPath: true, Path: record.MustParsePath("$.qty")},
			}},
			{Out: []string{"amount"}, From: record.MustParsePath("$.amount"), FromSet: true},
			{Out: []string{"amount_text"}, From: record.MustParsePath("$.amount"), FromSet: true, To: record.KindString, ToSet: true},
			{Out: []string{"region"}, From: record.MustParsePath("$.nope"), FromSet: true,
				Default: record.Int(0), DefaultSet: true},
			{Out: []string{"grade"}, Const: record.Int(3), ConstSet: true},
		})
	})
}

// A realistic multi-stage flow, because per-stage zeros do not by themselves
// prove the composition is zero: each operator hands the same batch on, and an
// operator that quietly needed a fresh batch would only show up here.
func TestAChainedPipelineDoesNotAllocate(t *testing.T) {
	assertNoAllocs(t, "coerce→flatten→filter→project", func(p *Pipeline) *Pipeline {
		return p.
			Coerce(CoerceRule{Field: "amount_text", To: record.KindDecimal}).
			Flatten(".").
			Filter("odd-qty", func(v record.Value) bool {
				q, _ := v.Field("qty")
				return q.Int()%2 == 1
			}).
			Project(
				ProjectField{From: record.MustParsePath("$.id")},
				ProjectField{Out: "city", From: record.MustParsePath("$.addr.city")},
				ProjectField{From: record.MustParsePath("$.amount")},
			)
	})
}
