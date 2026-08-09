package batchtest

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// A harness that cannot be shown to catch the bug it exists for is decoration.
// These tests pin both directions: a retainer must be caught, and correct code
// must not be disturbed.

// fakeSource reuses ONE batch across every Next — the normal, contract-abiding
// behaviour of every reader in engine/format.
type fakeSource struct {
	batch  *record.Batch
	rows   [][]string
	next   int
	closed bool
}

func newFakeSource(rows ...[]string) *fakeSource {
	return &fakeSource{batch: record.NewBatch(), rows: rows}
}

func (f *fakeSource) Next(context.Context) (*record.Batch, error) {
	if f.next >= len(f.rows) {
		return nil, io.EOF
	}
	f.batch.Reset()
	b := f.batch.Builder()
	for _, s := range f.rows[f.next] {
		b.BeginMap()
		b.KeyLiteral("name")
		b.StringLiteral(s)
		b.EndMap()
		f.batch.Append(b.Finish())
	}
	f.next++
	return f.batch, nil
}

func (f *fakeSource) Close() error { f.closed = true; return nil }

// nameIs reports whether v still reads as the intact record it was built as.
// Poisoning destroys a record in either of two ways — the arena bytes behind
// the value, or the key slab holding the field name — so a vanished field is
// as much proof of destruction as a corrupted value, and treating it as a test
// error would make the harness look broken on the case it handled best.
func nameIs(v record.Value, want string) bool {
	got, ok := v.Field("name")
	return ok && bytes.Equal(got.Bytes(), []byte(want))
}

// The bug the harness exists for: keeping a Value past its batch. Without
// poisoning the retained record still reads "a1", because Go keeps the arena
// alive — the assertion passes and the bug ships.
func TestARetainedRecordIsDestroyedWhenItsBatchRetires(t *testing.T) {
	src := Poisoning(newFakeSource([]string{"a1"}, []string{"b1"}))
	ctx := context.Background()

	first, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("first next: %v", err)
	}
	retained := first.Record(0) // ILLEGAL: no CopyValue

	if !nameIs(retained, "a1") {
		t.Fatal("before retirement the value must be intact")
	}

	if _, err := src.Next(ctx); err != nil { // retires the first batch
		t.Fatalf("second next: %v", err)
	}

	if nameIs(retained, "a1") {
		t.Error("a record retained across Next still reads its old value — the harness " +
			"did not poison, so a real retention bug would pass unnoticed")
	}
}

// The other half: poisoning must not punish code that plays by the rules.
func TestACopiedRecordSurvivesRetirement(t *testing.T) {
	src := Poisoning(newFakeSource([]string{"a1"}, []string{"b1"}))
	ctx := context.Background()

	first, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("first next: %v", err)
	}
	mine := record.NewBatch()
	copied := record.CopyValue(mine, first.Record(0)) // LEGAL

	if _, err := src.Next(ctx); err != nil {
		t.Fatalf("second next: %v", err)
	}
	if !nameIs(copied, "a1") {
		t.Error("CopyValue must survive poisoning — a harness that fails correct code " +
			"is worse than no harness")
	}
}

// Scalars are copied by value into the Value struct, so a retained scalar is
// legitimately safe. Poisoning must not pretend otherwise, or every aggregate
// holding a running total would look like a bug.
func TestARetainedScalarIsNotDisturbed(t *testing.T) {
	b := record.NewBatch()
	bl := b.Builder()
	bl.BeginMap()
	bl.KeyLiteral("n")
	bl.Int(42)
	bl.EndMap()
	b.Append(bl.Finish())

	v, ok := b.Record(0).Field("n")
	if !ok {
		t.Fatal("no n field")
	}
	b.Poison()
	if v.Int() != 42 {
		t.Errorf("retained scalar = %d, want 42: scalars carry no pointer into a batch "+
			"and must not be flagged", v.Int())
	}
}

// Close is the other end of a batch's life. Without poisoning there, the last
// batch of every stream is the one a consumer could retain for free — and
// end-of-stream is exactly where retention bugs concentrate.
func TestTheFinalBatchIsRetiredOnClose(t *testing.T) {
	src := Poisoning(newFakeSource([]string{"only"}))
	ctx := context.Background()

	b, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	retained := b.Record(0)

	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if nameIs(retained, "only") {
		t.Error("the final batch survived Close intact — a consumer retaining it would never be caught")
	}
}

// Retired is what a caller asserts on to know the harness had a chance to
// bite. If it miscounted, every test using it would be trusting a number that
// means nothing.
func TestRetiredCountsEveryBatchIncludingTheLast(t *testing.T) {
	src := Poisoning(newFakeSource([]string{"a"}, []string{"b"}, []string{"c"}))
	ctx := context.Background()
	for {
		if _, err := src.Next(ctx); err != nil {
			break
		}
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if src.Retired() != 3 {
		t.Errorf("Retired() = %d, want 3 (two superseded plus the final one at Close)", src.Retired())
	}
}

func TestCopyAllReturnsRecordsThatOutliveTheSource(t *testing.T) {
	src := Poisoning(newFakeSource([]string{"a1", "a2"}, []string{"b1"}))
	out, err := CopyAll(context.Background(), src)
	if err != nil {
		t.Fatalf("copy all: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	want := []string{"a1", "a2", "b1"}
	if out.Len() != len(want) {
		t.Fatalf("copied %d records, want %d", out.Len(), len(want))
	}
	for i, w := range want {
		if !nameIs(out.Record(i), w) {
			t.Errorf("record %d did not survive as %q", i, w)
		}
	}
}

// A source with a resume cursor must keep it through the harness, or a test
// would "pass" having quietly lost the behaviour it was checking (ADR-0037).
type resumableSource struct{ *fakeSource }

func (r resumableSource) Checkpoint() []byte { return []byte("cursor-7") }

func TestCheckpointPassesThroughTheHarness(t *testing.T) {
	src := Poisoning(resumableSource{newFakeSource([]string{"a"})})
	if got := src.Checkpoint(); !bytes.Equal(got, []byte("cursor-7")) {
		t.Errorf("Checkpoint() = %q, want the wrapped source's cursor", got)
	}
	plain := Poisoning(newFakeSource([]string{"a"}))
	if got := plain.Checkpoint(); got != nil {
		t.Errorf("Checkpoint() = %q on a non-resumable source, want nil", got)
	}
}
