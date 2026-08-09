package ndjson

// Allocation budgets for the NDJSON read and write paths (TC-006).
//
// This is the format on the hot path of nearly every flow, and the reason the
// parser is hand-rolled rather than encoding/json: values are built straight
// into the batch arena, so a steady-state batch costs nothing. Until this file
// that was only ever demonstrated by benchmark output, which gates nothing.
//
// Budgets: 0 exactly, for the line reader and for the writer. The one non-zero
// figure is JSONReader, which frames elements through encoding/json — pinned
// below with its cause and an upper bound rather than left unmeasured.
//
// Each measurement runs against a WARMED reader/writer: allocWarmupBatches
// batches are consumed before AllocsPerRun begins, so arena chunk growth and
// the bufio buffers have converged. Figures were identical with and without
// -race.

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

const (
	// allocBatchRecords per batch — small enough that the fixture stays cheap,
	// large enough that one allocation per record reads as 64 against 0.
	allocBatchRecords = 64
	// allocWarmupBatches consumed before measuring.
	allocWarmupBatches = 50
	// allocMeasureRuns passed to AllocsPerRun. The fixtures below hold far more
	// batches than warmup+runs, so no measured call ever hits end-of-input —
	// a short fixture would measure the EOF path instead of the parse.
	allocMeasureRuns = 100
	// allocFixtureBatches worth of input generated per fixture.
	allocFixtureBatches = 400
)

// allocLines generates allocFixtureBatches worth of NDJSON.
func allocLines(line func(*strings.Builder, int)) string {
	var sb strings.Builder
	for i := range allocBatchRecords * allocFixtureBatches {
		line(&sb, i)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// assertReadIsFree fails if pulling a steady-state batch out of the line reader
// allocates at all.
func assertReadIsFree(t *testing.T, shape, in string) {
	t.Helper()
	r := NewReader(strings.NewReader(in), ReaderOptions{BatchRecords: allocBatchRecords})
	ctx := context.Background()
	for range allocWarmupBatches {
		if _, err := r.Next(ctx); err != nil {
			t.Fatalf("%s: warmup: %v", shape, err)
		}
	}
	n := testing.AllocsPerRun(allocMeasureRuns, func() {
		b, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("%s: %v", shape, err)
		}
		if b.Len() != allocBatchRecords {
			t.Fatalf("%s: short batch (%d records): the fixture ran out and the "+
				"measurement is of end-of-input, not of parsing", shape, b.Len())
		}
	})
	if n != 0 {
		t.Errorf("reading %s allocates %.1f times per batch of %d records (%.3f per record), want 0",
			shape, n, allocBatchRecords, n/allocBatchRecords)
	}
}

func TestReadingNDJSONDoesNotAllocate(t *testing.T) {
	// Flat scalars: the common shape, and the one the parser's fast paths are
	// written for.
	assertReadIsFree(t, "flat scalars", allocLines(func(sb *strings.Builder, i int) {
		sb.WriteString(`{"id":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"sku":"AB-1234","qty":`)
		sb.WriteString(strconv.Itoa(i%7 + 1))
		sb.WriteString(`,"ok":true,"note":null}`)
	}))

	// Nested maps and lists exercise the value and key slabs, not just the
	// byte arena.
	assertReadIsFree(t, "nested maps and lists", allocLines(func(sb *strings.Builder, i int) {
		sb.WriteString(`{"id":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"addr":{"city":"Melbourne","geo":[-37.8136,144.9631]},"tags":["retail","au"]}`)
	}))

	// Escapes take the parser's slow path, which decodes into a reused scratch
	// buffer. If that buffer were ever reallocated per string this is where it
	// would show.
	assertReadIsFree(t, "escaped strings", allocLines(func(sb *strings.Builder, i int) {
		sb.WriteString(`{"id":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"s":"a\tbéc\"d\\e","t":"😀 emoji"}`)
	}))

	// Floats go through strconv.ParseFloat, which the package doc once called
	// the one remaining allocation. It is not one: the conversion of the token
	// does not escape, so the whole parse is free. Asserted so it stays that
	// way.
	assertReadIsFree(t, "floats", allocLines(func(sb *strings.Builder, i int) {
		sb.WriteString(`{"id":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"f":1.5,"g":`)
		sb.WriteString(strconv.FormatFloat(float64(i)+0.03125, 'f', -1, 64))
		sb.WriteString(`,"e":1.7e-9}`)
	}))
}

// Was 2 allocations per element, now the assertion that keeps it at 0 (TC-006).
//
// JSONReader frames elements with encoding/json rather than by line, so it can
// read a pretty-printed array. `var raw json.RawMessage` was declared INSIDE
// the per-record loop (jsonreader.go), so the buffer was reallocated for every
// element instead of reusing its capacity, and taking &raw for Decode boxed it.
// This is the reader the http connector uses for JSON-array APIs, so it is a
// hot path on the most common REST shape there is. raw is now a JSONReader
// field; the parse itself, shared with the line reader, was always free.
func TestReadingAJSONArrayDoesNotAllocate(t *testing.T) {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := range allocBatchRecords * allocFixtureBatches {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"sku":"AB-1234","addr":{"city":"Melbourne"}}`)
	}
	sb.WriteByte(']')

	r := NewJSONReader(strings.NewReader(sb.String()), ReaderOptions{BatchRecords: allocBatchRecords})
	ctx := context.Background()
	for range allocWarmupBatches {
		if _, err := r.Next(ctx); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}
	n := testing.AllocsPerRun(allocMeasureRuns, func() {
		if _, err := r.Next(ctx); err != nil {
			t.Fatal(err)
		}
	})
	if n != 0 {
		t.Errorf("JSONReader allocates %.1f per batch of %d elements (%.3f per element), want 0 — "+
			"steady-state decoding must reuse its buffer, not reallocate per element",
			n, allocBatchRecords, n/allocBatchRecords)
	}
}

// assertWriteIsFree fails if encoding a steady-state batch allocates. The
// destination is io.Discard behind the writer's own bufio buffer, so nothing
// downstream of the encoder can contribute.
func assertWriteIsFree(t *testing.T, shape string, fill func(*record.Batch)) {
	t.Helper()
	b := record.NewBatch()
	fill(b)
	w := NewWriter(io.Discard)
	ctx := context.Background()
	for range allocWarmupBatches {
		if err := w.Write(ctx, b); err != nil {
			t.Fatalf("%s: warmup: %v", shape, err)
		}
	}
	n := testing.AllocsPerRun(allocMeasureRuns, func() {
		if err := w.Write(ctx, b); err != nil {
			t.Fatalf("%s: %v", shape, err)
		}
	})
	if n != 0 {
		t.Errorf("writing %s allocates %.1f times per batch of %d records (%.3f per record), want 0",
			shape, n, b.Len(), n/float64(b.Len()))
	}
}

func TestWritingNDJSONDoesNotAllocate(t *testing.T) {
	assertWriteIsFree(t, "mixed scalars, nested maps and lists", func(b *record.Batch) {
		bld := b.Builder()
		for i := range allocBatchRecords {
			bld.BeginMap()
			bld.KeyLiteral("id")
			bld.Int(int64(1000 + i))
			bld.KeyLiteral("sku")
			bld.StringLiteral("AB-1234")
			bld.KeyLiteral("f")
			bld.Float(float64(i) + 0.03125)
			bld.KeyLiteral("ok")
			bld.Bool(true)
			bld.KeyLiteral("note")
			bld.Null()
			bld.KeyLiteral("addr")
			bld.BeginMap()
			bld.KeyLiteral("city")
			bld.StringLiteral("Melbourne")
			bld.EndMap()
			bld.KeyLiteral("tags")
			bld.BeginList()
			bld.StringLiteral("retail")
			bld.StringLiteral("au")
			bld.EndList()
			bld.EndMap()
			b.Append(bld.Finish())
		}
	})

	// The escape path writes through the same buffered writer in slices; a
	// regression to building the escaped string first would show here.
	assertWriteIsFree(t, "strings needing escapes", func(b *record.Batch) {
		bld := b.Builder()
		for range allocBatchRecords {
			bld.BeginMap()
			bld.KeyLiteral("s")
			bld.StringLiteral("a\tb\"c\\d\ne — ünïcode ✓")
			bld.KeyLiteral("ctl")
			bld.StringLiteral("bell:\x07")
			bld.EndMap()
			b.Append(bld.Finish())
		}
	})

	// The exact kinds (ADR-0051) render through Value.AppendText into the
	// writer's reused scratch — the property that lets a decimal keep every
	// digit its scale claims without paying a string per field.
	assertWriteIsFree(t, "decimals and temporals", func(b *record.Batch) {
		bld := b.Builder()
		for i := range allocBatchRecords {
			bld.BeginMap()
			bld.KeyLiteral("amount")
			bld.Decimal(int64(i)*10111, 4)
			bld.KeyLiteral("ts")
			bld.Timestamp(int64(i)*1e9, 0)
			bld.KeyLiteral("day")
			bld.Date(int64(i))
			bld.KeyLiteral("clock")
			bld.TimeOfDay(int64(i) * 1e6)
			bld.EndMap()
			b.Append(bld.Finish())
		}
	})
}
