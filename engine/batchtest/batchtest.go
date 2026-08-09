// Package batchtest enforces the batch-lifetime contract in tests.
//
// The contract (ADR-0004, CLAUDE.md "Engine contracts to preserve"): a batch
// returned by Next is valid only until the NEXT Next or Close. Anything kept
// past that must be copied with record.CopyValue.
//
// Nothing in Go enforces it. A retained Value keeps its arena chunk alive, so
// the retaining code reads plausible data and every assertion passes — until
// production traffic makes the reuse pattern differ from the test's. Before
// this package the contract was documented in prose and checked by review,
// which is how the v0 prototype ended up buffering whole payloads: nothing
// ever said no.
//
// Wrapping a source in Poisoning makes it say no. The retired batch's memory
// is scribbled over at exactly the instant the contract declares it dead, so a
// retained Value reads the marker instead of its data and whatever the test
// already asserts about the output fails. It adds no assertions; it makes the
// existing ones mean what they claim.
//
//	src := batchtest.Poisoning(ndjson.NewReader(f, opts))
//	// ... drive the pipeline as usual, assert as usual ...
//	if src.Retired() < 2 {
//	    t.Fatal("fewer than two batches: the harness never had a chance to bite")
//	}
//
// The Retired check is not ceremony. A source that yields one batch is never
// poisoned in any way that matters, so a test that forgets to produce enough
// data proves nothing while looking like it proves something.
package batchtest

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// Source mirrors stream.Source structurally. Declared here rather than
// imported so this package depends on record alone — the format readers, which
// do not import stream, can be wrapped just as easily as an operator chain.
type Source interface {
	Next(ctx context.Context) (*record.Batch, error)
	Close() error
}

// Checkpointer mirrors stream.Checkpointer (ADR-0037). Forwarded so wrapping a
// resumable source in the harness does not silently disable resume — a test
// that lost checkpointing to the harness would pass for the wrong reason.
type Checkpointer interface {
	Checkpoint() []byte
}

// PoisonSource is a Source that destroys each batch as its lifetime ends.
type PoisonSource struct {
	inner Source
	last  *record.Batch
	n     int
}

// Poisoning wraps src so that every batch is scribbled over the moment the
// contract says it became invalid: immediately before the next Next, and on
// Close.
func Poisoning(src Source) *PoisonSource { return &PoisonSource{inner: src} }

// Next poisons the previously returned batch, then delegates.
//
// The order matters and is the whole design: poisoning BEFORE delegating means
// a source that reads its own previous output (a CSV header row, an XML
// element name, an EDI delimiter set) sees the marker, and a source that
// correctly copied such state out is unaffected.
func (p *PoisonSource) Next(ctx context.Context) (*record.Batch, error) {
	p.retire()
	b, err := p.inner.Next(ctx)
	p.last = b // nil at end-of-stream: retire handles that
	return b, err
}

// Close poisons the final batch, then closes. Without this the last batch of
// every stream would be the one batch a consumer could retain for free — and
// end-of-stream handling is where retention bugs concentrate.
func (p *PoisonSource) Close() error {
	p.retire()
	return p.inner.Close()
}

// Checkpoint forwards to the wrapped source when it is resumable.
func (p *PoisonSource) Checkpoint() []byte {
	if cp, ok := p.inner.(Checkpointer); ok {
		return cp.Checkpoint()
	}
	return nil
}

// Retired reports how many batches have been poisoned. A test asserting on
// output should also assert this is at least 2 — with fewer, nothing was ever
// invalidated while anything downstream could still be looking.
func (p *PoisonSource) Retired() int { return p.n }

func (p *PoisonSource) retire() {
	if p.last == nil {
		return
	}
	p.last.Poison()
	p.last = nil
	p.n++
}

// AssertPoisonSafe drives the same source twice — once normally, once with
// every retired batch scribbled over — and fails tb if the output differs.
//
// newSource must return a FRESH source over identical input each call; the two
// runs are only comparable if they read the same thing.
//
// This is the whole contract in one call. A reader or operator that depends on
// a batch it already handed on — a cached key, a slice into the last arena, a
// value kept for the next round — produces different output under poisoning
// and fails here. One that copies what it keeps is untouched.
func AssertPoisonSafe(tb testing.TB, newSource func() Source) {
	tb.Helper()
	AssertPoisonSafeChain(tb, newSource, func(s Source) Source { return s })
}

// AssertPoisonSafeChain is AssertPoisonSafe for code that sits DOWNSTREAM of
// the source: build receives the input and returns whatever consumes it (an
// operator chain, a pipeline as a source, a decorator).
//
// The poisoning goes on the input, which is the only placement that tests the
// consumer. Poisoning a pipeline's output would test whoever reads the
// pipeline; poisoning its input tests the operators inside it — and an
// operator holding a group key, a join build-side row, or a merge cursor
// across batches is the retention that actually happens.
func AssertPoisonSafeChain(tb testing.TB, newInput func() Source, build func(Source) Source) {
	tb.Helper()
	assertPoisonSafe(tb, newInput, build, false)
}

// AssertPoisonSafeChainUnordered is AssertPoisonSafeChain for code whose
// output ORDER is deliberately unspecified — the aggregate emits groups in
// hash/partition order, which differs between runs of the same input. Records
// are compared as a multiset instead.
//
// Use it only where the order genuinely carries no meaning. Everywhere else
// the ordered form is the stronger check, and a reordering under poisoning
// would itself be a finding.
func AssertPoisonSafeChainUnordered(tb testing.TB, newInput func() Source, build func(Source) Source) {
	tb.Helper()
	assertPoisonSafe(tb, newInput, build, true)
}

func assertPoisonSafe(tb testing.TB, newInput func() Source, build func(Source) Source, unordered bool) {
	tb.Helper()
	ctx := context.Background()

	clean, err := CopyAll(ctx, build(newInput()))
	if err != nil {
		tb.Fatalf("batchtest: clean run: %v", err)
	}
	poisoned := Poisoning(newInput())
	dirty, err := CopyAll(ctx, build(poisoned))
	if err != nil {
		tb.Fatalf("batchtest: poisoned run: %v", err)
	}
	if err := poisoned.Close(); err != nil {
		tb.Fatalf("batchtest: close: %v", err)
	}

	// Without at least two retirements nothing was ever invalidated while
	// anything could still be looking at it, and a green result would mean
	// nothing at all.
	if poisoned.Retired() < 2 {
		tb.Fatalf("batchtest: only %d batch(es) retired — feed more input, or lower "+
			"the reader's batch size, so retirement actually happens mid-stream", poisoned.Retired())
	}
	got, want := Dump(dirty), Dump(clean)
	if unordered {
		got, want = sortLines(got), sortLines(want)
	}
	if got != want {
		tb.Errorf("batchtest: output changed when retired batches were poisoned — "+
			"something is reading a batch whose lifetime had ended (ADR-0004).\n got: %s\nwant: %s", got, want)
	}
}

// sortLines makes a dump order-insensitive. The per-record index prefix that
// Dump emits is stripped first: with it, sorting would compare "0:" against
// "0:" and the whole point would be lost.
func sortLines(dump string) string {
	lines := strings.Split(strings.TrimRight(dump, "\n"), "\n")
	for i, l := range lines {
		if _, rest, ok := strings.Cut(l, ":"); ok {
			lines[i] = rest
		}
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// Dump renders a batch to a deterministic, total string — every kind, nested
// maps and lists included, map keys in their record order.
//
// Its purpose is comparison, not display. The generic form of "poisoning
// changed nothing" is `Dump(cleanRun) == Dump(poisonedRun)`, which needs no
// per-format expected fixture and cannot be satisfied by a reader that quietly
// drops a field: a missing key changes the dump, and so does a corrupted one.
func Dump(b *record.Batch) string {
	var sb strings.Builder
	for i := range b.Len() {
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte(':')
		dumpValue(&sb, b.Record(i))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func dumpValue(sb *strings.Builder, v record.Value) {
	switch v.Kind() {
	case record.KindNull:
		sb.WriteString("null")
	case record.KindBool:
		sb.WriteString(strconv.FormatBool(v.Bool()))
	case record.KindInt:
		sb.WriteString("i" + strconv.FormatInt(v.Int(), 10))
	case record.KindFloat:
		sb.WriteString("f" + strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case record.KindString:
		sb.WriteString("s" + strconv.Quote(v.String()))
	case record.KindBytes:
		sb.WriteString("b" + strconv.Quote(string(v.Bytes())))
	case record.KindDecimal, record.KindTimestamp, record.KindDate, record.KindTime:
		// Exact kinds keep their coefficient/scale or their instant; rendering
		// through the canonical text form would hide a change in either.
		coef, scale, _ := v.ExactDecimal()
		sb.WriteString("x" + strconv.Itoa(int(v.Kind())) + "/" +
			strconv.FormatInt(coef, 10) + "e" + strconv.Itoa(int(scale)))
	case record.KindList:
		sb.WriteByte('[')
		for i := range v.Len() {
			if i > 0 {
				sb.WriteByte(',')
			}
			dumpValue(sb, v.Index(i))
		}
		sb.WriteByte(']')
	case record.KindMap:
		sb.WriteByte('{')
		for i := range v.Len() {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.Quote(string(v.KeyAt(i))))
			sb.WriteByte('=')
			dumpValue(sb, v.Index(i))
		}
		sb.WriteByte('}')
	default:
		sb.WriteString("?kind" + strconv.Itoa(int(v.Kind())))
	}
}

// CopyAll drains src to exhaustion and returns every record copied into a
// batch the CALLER owns — the legal way to accumulate across batches. A clean
// end of stream (io.EOF) returns a nil error.
//
// Pair it with Poisoning to prove a reader's output survives its own batch
// reuse: copies are unaffected by poisoning, references are destroyed by it.
func CopyAll(ctx context.Context, src Source) (*record.Batch, error) {
	out := record.NewBatch()
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		if b == nil {
			return out, nil
		}
		for i := range b.Len() {
			out.Append(record.CopyValue(out, b.Record(i)))
		}
	}
}
