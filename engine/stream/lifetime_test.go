package stream

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
)

// TC-009 (docs/assurance/test-conformance.md). Operators are handed a batch
// that belongs to their upstream and are allowed to mutate it in place — but
// not to keep any part of it past the next pull. Blocking operators are where
// that gets hard: an aggregate holds a group KEY for the whole input, a join
// holds every build-side row, and both are handed those values inside batches
// that are recycled underneath them.
//
// The rule was previously enforced by review. Poisoning the input at exactly
// the instant each batch dies means a retained value reads as a marker, so the
// operator's own output changes — and these tests compare against the same
// pipeline over unpoisoned input, so any dependence shows up as a diff.

// lifetimeInput builds n records across k groups, wide enough that a small
// batch size guarantees many retirements mid-stream.
func lifetimeInput(n, k int) string {
	var sb strings.Builder
	for i := range n {
		g := i % k
		fmt.Fprintf(&sb, `{"group":"g%02d","amount":%d,"name":"row-%d","nested":{"deep":"d%d"},"tags":["a","b"]}`+"\n",
			g, g*100+i, i, i)
	}
	return sb.String()
}

func lifetimeReader(in string) func() batchtest.Source {
	return func() batchtest.Source {
		return ndjson.NewReader(strings.NewReader(in), ndjson.ReaderOptions{BatchRecords: 4})
	}
}

// asSource builds the pipeline and adapts it, failing the test rather than
// returning an error the caller would have to thread through build().
func asSource(tb testing.TB, p *Pipeline) Source {
	tb.Helper()
	src, err := p.AsSource()
	if err != nil {
		tb.Fatalf("as source: %v", err)
	}
	return src
}

func TestNonBlockingOperatorsDoNotRetainARetiredBatch(t *testing.T) {
	in := lifetimeInput(60, 5)
	batchtest.AssertPoisonSafeChain(t, lifetimeReader(in), func(s batchtest.Source) batchtest.Source {
		return asSource(t, New(s.(Source), "read").
			Filter("keep", func(v record.Value) bool {
				a, ok := v.Field("amount")
				return !ok || a.Int()%7 != 0
			}).
			Project(
				ProjectField{From: record.MustParsePath("$.group"), Out: "group"},
				ProjectField{From: record.MustParsePath("$.name"), Out: "name"},
				ProjectField{From: record.MustParsePath("$.nested.deep"), Out: "deep"},
			).
			Flatten("_"))
	})
}

// The aggregate holds a group key for the lifetime of the whole input — the
// longest-lived reference any operator takes into a batch it does not own.
func TestAggregateDoesNotRetainGroupKeysFromARetiredBatch(t *testing.T) {
	in := lifetimeInput(60, 5)
	// Unordered: groups come out in hash order, which is unspecified and varies
	// run to run. The values are what this test is about.
	batchtest.AssertPoisonSafeChainUnordered(t, lifetimeReader(in), func(s batchtest.Source) batchtest.Source {
		return asSource(t, New(s.(Source), "read").Aggregate(AggregateSpec{
			Key: record.MustParsePath("$.group"),
			Gov: mem.New(1 << 20),
			Aggs: []Agg{
				{Op: AggCount, Out: "n"},
				{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
				{Op: AggMin, From: record.MustParsePath("$.amount"), Out: "lo"},
				{Op: AggMax, From: record.MustParsePath("$.amount"), Out: "hi"},
			},
		}))
	})
}

// Spilling is the harder half: state leaves memory, comes back off disk, and
// merges. A key that was a slice into a recycled batch when it was written out
// would spill garbage — which is indistinguishable from correct until the
// timing changes.
func TestSpillingAggregateDoesNotRetainGroupKeysFromARetiredBatch(t *testing.T) {
	in := lifetimeInput(400, 40)
	dir := t.TempDir()
	batchtest.AssertPoisonSafeChainUnordered(t, lifetimeReader(in), func(s batchtest.Source) batchtest.Source {
		return asSource(t, New(s.(Source), "read").Aggregate(AggregateSpec{
			Key:      record.MustParsePath("$.group"),
			Gov:      mem.New(4 << 10), // small enough to force spilling
			SpillDir: dir,
			Aggs: []Agg{
				{Op: AggCount, Out: "n"},
				{Op: AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
			},
		}))
	})
}

// Concat reads its inputs in turn; the first input's batches are long retired
// by the time the last is being read.
func TestConcatDoesNotRetainARetiredBatch(t *testing.T) {
	in := lifetimeInput(40, 4)
	other := lifetimeInput(40, 3)
	batchtest.AssertPoisonSafeChain(t, lifetimeReader(in), func(s batchtest.Source) batchtest.Source {
		return Concat(s.(Source), ndjson.NewReader(strings.NewReader(other), ndjson.ReaderOptions{BatchRecords: 4}))
	})
}

// Non-vacuity. Every test above passes, which is only meaningful if the
// harness would have failed had an operator misbehaved. This builds an
// operator that breaks the rule on purpose — it keeps a Value from the first
// batch and stamps it onto later ones — and proves the poisoning reaches
// through a real pipeline (measured source, transforms and all) to corrupt it.
func TestTheHarnessCatchesAnOperatorThatRetainsAcrossBatches(t *testing.T) {
	in := lifetimeInput(40, 4)

	// stash holds a value from whichever batch it first saw. Copying it with
	// record.CopyValue is what the contract requires; keeping the reference,
	// as here, is the bug.
	run := func(src Source) string {
		var stash record.Value
		var have bool
		p := New(src, "read").Apply("retain", func(_ context.Context, b *record.Batch) (*record.Batch, error) {
			if !have && b.Len() > 0 {
				if v, ok := b.Record(0).Field("name"); ok {
					stash, have = v, true // ILLEGAL: no CopyValue
				}
			}
			for i := range b.Len() {
				rec := b.Record(i)
				if _, ok := rec.Field("name"); ok && have {
					rec.SetIndex(indexOfField(rec, "name"), stash)
				}
			}
			return b, nil
		})
		out, err := batchtest.CopyAll(t.Context(), asSource(t, p))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return batchtest.Dump(out)
	}

	clean := run(ndjson.NewReader(strings.NewReader(in), ndjson.ReaderOptions{BatchRecords: 4}))
	poisoned := batchtest.Poisoning(ndjson.NewReader(strings.NewReader(in), ndjson.ReaderOptions{BatchRecords: 4}))
	dirty := run(poisoned)

	if poisoned.Retired() < 2 {
		t.Fatalf("only %d batch(es) retired", poisoned.Retired())
	}
	if clean == dirty {
		t.Error("an operator that retains a Value across batches produced identical output " +
			"with and without poisoning — the harness does not reach into a pipeline, so every " +
			"passing test above proves nothing")
	}
}

// indexOfField returns the position of name within a map record, or -1.
func indexOfField(v record.Value, name string) int {
	for i := range v.Len() {
		if string(v.KeyAt(i)) == name {
			return i
		}
	}
	return -1
}
