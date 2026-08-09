package csvf

// Allocation budgets for the CSV read and write paths (TC-006).
//
// CSV is the one hot-path format in the engine that is NOT allocation-free, and
// this file is where that is stated in numbers instead of left to a benchmark
// nobody gates on. The cause is a deliberate design choice recorded in the
// package doc: csvf delegates quoting to encoding/csv, whose API is
// string-shaped in both directions — Reader.Read hands back []string and
// Writer.Write takes []string. Strings are immutable, so every cell that is not
// already a string in the batch has to become one.
//
// Budgets are therefore UPPER BOUNDS, each with the observed figure and its
// cause in the comment above it, so that:
//
//   - a regression that adds a per-record allocation fails the gate; and
//   - reducing one (by hand-rolling a tokenizer/encoder the way ndjson does,
//     or by removing the avoidable ones named below) does not.
//
// Two of the figures are OURS and avoidable, not encoding/csv's — see
// TestReadingTypedExactColumnsCostsAnExtraAllocation. Figures were identical
// with and without -race.

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

const (
	allocBatchRecords   = 64
	allocWarmupBatches  = 50
	allocMeasureRuns    = 100
	allocFixtureBatches = 400
)

// allocCSV generates a fixture far longer than warmup+runs consume, so no
// measured call reaches end-of-input.
func allocCSV() string {
	var sb strings.Builder
	sb.WriteString("id,sku,qty,amount,when,ok\n")
	for i := range allocBatchRecords * allocFixtureBatches {
		sb.WriteString(strconv.Itoa(1000 + i))
		sb.WriteString(",AB-1234,")
		sb.WriteString(strconv.Itoa(i%7 + 1))
		sb.WriteString(",1234.56,2026-08-09T01:02:03Z,true\n")
	}
	return sb.String()
}

// allocsPerReadBatch returns steady-state allocations for one Next.
func allocsPerReadBatch(t *testing.T, in string, opts ReaderOptions) float64 {
	t.Helper()
	opts.BatchRecords = allocBatchRecords
	r := NewReader(strings.NewReader(in), opts)
	ctx := context.Background()
	for range allocWarmupBatches {
		if _, err := r.Next(ctx); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}
	return testing.AllocsPerRun(allocMeasureRuns, func() {
		b, err := r.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b.Len() != allocBatchRecords {
			t.Fatalf("short batch (%d records): the fixture ran out and the measurement "+
				"is of end-of-input, not of parsing", b.Len())
		}
	})
}

// The floor for any CSV read, and it is not zero.
//
// encoding/csv in ReuseRecord mode reuses the []string header slice but still
// materialises ONE string per row holding that row's cells end to end, which
// the returned []string then sub-slices. So the budget is 1 allocation per row
// however the columns are typed — csvf's own work (copying cells into the batch
// arena, parsing ints, floats and bools) adds nothing on top.
//
// Observed: exactly 1 per record (64 per 64-record batch) for untyped, int,
// float, bool and quoted columns alike.
func TestReadingCSVCostsOneAllocationPerRow(t *testing.T) {
	in := allocCSV()
	cases := []struct {
		name string
		opts ReaderOptions
	}{
		{"untyped (all strings)", ReaderOptions{}},
		{"int, float and bool columns", ReaderOptions{Types: map[string]ColumnType{
			"id": TypeInt, "qty": TypeInt, "amount": TypeFloat, "ok": TypeBool,
		}}},
	}
	for _, c := range cases {
		n := allocsPerReadBatch(t, in, c.opts)
		t.Logf("%-28s %.1f allocs per batch of %d rows (%.3f per row)",
			c.name, n, allocBatchRecords, n/allocBatchRecords)
		if want := float64(allocBatchRecords); n > want {
			t.Errorf("reading %s allocates %.1f per batch, above the known budget of %.0f "+
				"(1 per row, from encoding/csv) — csvf has grown a per-row allocation of its own",
				c.name, n, want)
		}
	}
}

// Quoted and escaped cells take encoding/csv's slow path, which appends into a
// reused buffer; the per-row budget is unchanged. Measured separately because a
// regression there would be invisible in the fixture above, whose cells are all
// bare.
func TestReadingQuotedCSVCostsNoMoreThanBareCSV(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,sku,qty,amount,when,ok\n")
	for i := range allocBatchRecords * allocFixtureBatches {
		sb.WriteString(strconv.Itoa(1000 + i))
		sb.WriteString(`,"a,b ""quoted"" c",1,1234.56,x,true` + "\n")
	}
	n := allocsPerReadBatch(t, sb.String(), ReaderOptions{})
	if want := float64(allocBatchRecords); n > want {
		t.Errorf("reading quoted CSV allocates %.1f per batch, above the %.0f (1 per row) "+
			"that bare CSV costs", n, want)
	}
}

// Was 2 allocations per row, now pinned at the 1-per-row floor (TC-006).
//
// A column declared decimal, timestamp, date or time is parsed by parseTyped,
// which did []byte(strings.TrimSpace(cell)) to hand record.Parse* a byte slice
// — a second heap allocation per row, on top of encoding/csv's row string.
// parseTyped now trims into a scratch buffer owned by the Reader, so a typed
// column costs exactly what an untyped one does.
//
// This mattered more than its size suggests: TypeDecimal is the declared opt-in
// from ADR-0051 §5 for money columns, so the exact-decimal path — the one a
// customer turns on precisely because they care about it — was the one paying.
func TestReadingTypedExactColumnsCostsNoMoreThanUntyped(t *testing.T) {
	in := allocCSV()
	for _, c := range []struct {
		name string
		typ  ColumnType
		col  string
	}{
		{"decimal", TypeDecimal, "amount"},
		{"timestamp", TypeTimestamp, "when"},
	} {
		n := allocsPerReadBatch(t, in, ReaderOptions{Types: map[string]ColumnType{c.col: c.typ}})
		t.Logf("%-12s column: %.1f allocs per batch of %d rows (%.3f per row)",
			c.name, n, allocBatchRecords, n/allocBatchRecords)
		if want := float64(allocBatchRecords); n > want {
			t.Errorf("a %s column costs %.1f allocs per batch, above the %.0f (1 per row) "+
				"that an untyped column costs — the typed path has grown an allocation again",
				c.name, n, want)
		}
	}
}

// The write path, measured for the same reason as the read path: csvf is the
// one hot-path format that is not allocation-free in either direction, and the
// only way that stays honest is to pin the number.
//
// Three per record is encoding/csv.Writer's own shape — it takes []string, so
// every rendered non-bool cell becomes a Go string before it reaches the
// buffer. Bools are free because strconv returns the constant "true"/"false".
// This is the cost of not re-implementing CSV quoting; the assertion exists so
// a FOURTH allocation cannot appear unnoticed.
func TestWritingCSVCostsOneAllocationPerRenderedCell(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	for i := range allocBatchRecords {
		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(int64(1000 + i))
		bld.KeyLiteral("sku")
		bld.StringLiteral("AB-1234")
		bld.KeyLiteral("amount")
		bld.Decimal(123456, 2)
		bld.KeyLiteral("ok")
		bld.Bool(i%2 == 0)
		bld.EndMap()
		b.Append(bld.Finish())
	}

	w := NewWriter(io.Discard, WriterOptions{})
	ctx := context.Background()
	for range allocWarmupBatches { // let the writer's buffers converge
		if err := w.Write(ctx, b); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}
	n := testing.AllocsPerRun(allocMeasureRuns, func() {
		if err := w.Write(ctx, b); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("CSV write: %.1f allocs per batch of %d records (%.3f per record)",
		n, allocBatchRecords, n/allocBatchRecords)
	if want := float64(allocBatchRecords * 3); n > want {
		t.Errorf("writing CSV allocates %.1f per batch, above the known budget of %.0f "+
			"(3 per record, one per rendered non-bool cell) — a NEW per-record allocation has appeared",
			n, want)
	}
}
