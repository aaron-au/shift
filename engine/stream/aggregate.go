package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"time"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// AggOp is an aggregate function.
type AggOp uint8

// Supported aggregate functions.
const (
	AggCount AggOp = iota // records per group (null-agnostic, COUNT(*))
	// AggSum accumulates exactly for KindInt and KindDecimal inputs (128-bit,
	// ADR-0051 §3) and in float64 only once a KindFloat has been seen.
	AggSum
	AggMin
	AggMax
)

// Agg computes one output column per group.
type Agg struct {
	Op   AggOp
	From record.Path // ignored for AggCount
	Out  string
}

// AggregateSpec configures a group-by aggregation.
type AggregateSpec struct {
	// Key locates the (scalar) group key in each record.
	Key record.Path
	// KeyName is the key's output field name (default: key path leaf).
	KeyName string
	Aggs    []Agg
	// Gov bounds in-memory state; exceeding it triggers a spill to scratch.
	// Required.
	Gov *mem.Governor
	// SpillDir hosts the scratch file ("" = OS temp dir). The store is
	// created lazily on first spill.
	SpillDir string
	// Partitions is the spill/merge fan-out (default 8): merge memory is
	// bounded by the largest partition, not total group cardinality.
	Partitions int
	// EmitBatchRecords sizes output batches (default 1024).
	EmitBatchRecords int
}

// Aggregate appends a blocking group-by operator. It consumes the entire
// upstream on first pull, spilling partial state to the scratch store
// whenever the governor's watermark is hit, then emits merged groups one
// partition at a time.
func (p *Pipeline) Aggregate(spec AggregateSpec) *Pipeline {
	if spec.Gov == nil {
		return p.fail(errors.New("stream: aggregate requires a governor"))
	}
	if spec.KeyName == "" {
		spec.KeyName = spec.Key.LeafName()
	}
	if spec.KeyName == "" {
		return p.fail(fmt.Errorf("stream: aggregate key %s needs KeyName", spec.Key))
	}
	if spec.Partitions <= 0 {
		spec.Partitions = 8
	}
	if spec.EmitBatchRecords <= 0 {
		spec.EmitBatchRecords = 1024
	}
	for i, a := range spec.Aggs {
		if a.Out == "" {
			return p.fail(fmt.Errorf("stream: aggregate output %d needs Out name", i))
		}
	}
	st := &OpStats{Name: "aggregate"}
	p.stats = append(p.stats, st)
	p.src = &aggSource{up: p.src, spec: spec, stats: st, seed: maphash.MakeSeed()}
	return p
}

// SpillBytes returns a reporter for the spill volume of the pipeline's most
// recently appended Aggregate (always 0 if the last operator isn't an
// aggregate). Call the returned func after Run.
func SpillBytes(p *Pipeline) func() int64 {
	if a, ok := p.src.(*aggSource); ok {
		return a.SpillBytes
	}
	return func() int64 { return 0 }
}

// accum is one aggregate function's running state. Each accum serves exactly
// one Agg, so the fields are effectively a union: count for AggCount, the sums
// for AggSum, ext for AggMin/AggMax.
//
// Sums keep two accumulators rather than one: an exact 128-bit total for
// KindInt and KindDecimal, and a float64 total used from the moment a
// KindFloat appears in the column (ADR-0051 §3).
//
// ext holds the running extreme as record.ScalarBits: the inline payload of a
// scalar, 16 bytes instead of a Value's 88. That is not a micro-optimisation
// here — one Value per group per agg moved the aggregate's peak RSS by nearly a
// factor of two at a million groups. It is also why the extreme is safe to
// retain across batch boundaries at all: a numeric scalar has no arena or slab
// pointers to dangle, and observe rejects every non-numeric kind before it can
// reach here.
type accum struct {
	count int64

	sum     record.ExactSum // exact while inexact is false
	fsum    float64         // takes over once a float has been seen
	inexact bool

	ext  record.ScalarBits // running MIN or MAX; numeric scalar only, see above
	seen bool
}

// accumBytes is the in-memory size of one accum, for governor accounting:
// count (8) + the exact sum's 128-bit state (24) + the float sum (8) + the
// extreme's inline bits (16) + flags and padding. Deliberately rounded up —
// under-reporting here means the watermark is crossed without anyone noticing,
// which is how a bounded-memory aggregate stops being one.
const accumBytes = 72

// groupCost approximates per-group state bytes for governor accounting.
func groupCost(keyLen, naggs int) int64 {
	return int64(keyLen) + 64 + int64(naggs)*accumBytes
}

// maxSpilledKeyBytes guards merge reads against corrupt segment data.
const maxSpilledKeyBytes = 1 << 20

type aggSource struct {
	up    Source
	spec  AggregateSpec
	stats *OpStats
	seed  maphash.Seed

	consumed bool
	parts    []map[string][]accum // partition -> encoded key -> accums
	segs     [][]spill.Segment    // partition -> spilled segments
	store    *spill.Store
	reserved int64

	// emission state
	emitPart  int
	emitQueue []emitGroup
	outBatch  *record.Batch

	// extBatch hosts extremes decoded from spilled segments. They are inline
	// scalars, so nothing is ever allocated in its arena and it never needs
	// resetting; readAccum rejects anything that would break that.
	extBatch *record.Batch

	// scratch
	keyBuf bytes.Buffer
	keyEnc *spill.Encoder
	keyRdr bytes.Reader
	keyBR  *bufio.Reader
}

type emitGroup struct {
	key  string
	accs []accum
}

// SpillBytes reports total spilled volume (0 when everything fit in
// memory).
func (a *aggSource) SpillBytes() int64 {
	if a.store == nil {
		return 0
	}
	return a.store.BytesWritten()
}

func (a *aggSource) Next(ctx context.Context) (*record.Batch, error) {
	if !a.consumed {
		if err := a.consume(ctx); err != nil {
			return nil, err
		}
		a.consumed = true
	}
	return a.emit()
}

func (a *aggSource) consume(ctx context.Context) error {
	a.parts = make([]map[string][]accum, a.spec.Partitions)
	for i := range a.parts {
		a.parts[i] = make(map[string][]accum)
	}
	a.segs = make([][]spill.Segment, a.spec.Partitions)
	a.keyEnc = spill.NewEncoder(&a.keyBuf)
	a.outBatch = record.NewBatch()
	a.extBatch = record.NewBatch()

	for {
		b, err := a.up.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		a.stats.Batches++
		a.stats.RecordsIn += int64(b.Len())
		start := time.Now()
		for _, rec := range b.Records() {
			if err := a.observe(rec); err != nil {
				return err
			}
		}
		a.stats.Nanos += time.Since(start).Nanoseconds()
	}
}

func (a *aggSource) observe(rec record.Value) error {
	kv, _ := a.spec.Key.Get(rec) // missing key aggregates under null
	switch kv.Kind() {
	case record.KindList, record.KindMap:
		return fmt.Errorf("aggregate: group key %s is a container", a.spec.Key)
	}
	a.keyBuf.Reset()
	if err := a.keyEnc.Encode(kv); err != nil {
		return err
	}
	keyBytes := a.keyBuf.Bytes()
	pidx := int(maphash.Bytes(a.seed, keyBytes) % uint64(len(a.parts))) //nolint:gosec // result < len(parts), fits int
	part := a.parts[pidx]

	accs, ok := part[string(keyBytes)] // alloc-free map lookup
	if !ok {
		cost := groupCost(len(keyBytes), len(a.spec.Aggs))
		if !a.spec.Gov.TryReserve(cost) {
			// Watermark hit: only spilling helps if we actually hold state.
			if a.reserved == 0 {
				return fmt.Errorf("aggregate: watermark %d too small for a single group", a.spec.Gov.Budget())
			}
			if err := a.spillAll(); err != nil {
				return err
			}
			part = a.parts[pidx]
			a.spec.Gov.Reserve(cost) // post-spill: account unconditionally
		}
		a.reserved += cost
		// No sentinels to seed: an extreme starts unset and `seen` says so,
		// which is what lets MIN over decimals stay a decimal rather than
		// beginning life as ±Inf.
		accs = make([]accum, len(a.spec.Aggs))
		part[string(keyBytes)] = accs
	}
	for i, ag := range a.spec.Aggs {
		if ag.Op == AggCount {
			accs[i].count++
			continue
		}
		v, ok := ag.From.Get(rec)
		if !ok || v.IsNull() {
			continue
		}
		if !v.IsNumeric() {
			return fmt.Errorf("aggregate: %s is %v, want numeric", ag.From, v.Kind())
		}
		if err := accs[i].observe(ag.Op, v); err != nil {
			return fmt.Errorf("aggregate: %s: %w", ag.From, err)
		}
	}
	return nil
}

// observe folds one non-null numeric value into the accumulator for op.
func (ac *accum) observe(op AggOp, v record.Value) error {
	switch op {
	case AggSum:
		if err := ac.addToSum(v); err != nil {
			return err
		}
	case AggMin, AggMax:
		if err := ac.extend(op, v); err != nil {
			return err
		}
	}
	ac.seen = true
	ac.count++
	return nil
}

// addToSum keeps the sum exact for as long as the column allows.
func (ac *accum) addToSum(v record.Value) error {
	if ac.inexact {
		return ac.addFloat(v.Float())
	}
	if v.Kind() == record.KindFloat {
		// The first float in the column: carry the exact total over and stay
		// in float64 from here. Mixing exact and inexact numbers is legal and
		// documented as lossy (ADR-0051 §4) — the alternative, refusing the
		// record, would fail a flow over a distinction its author did not make.
		ac.inexact = true
		ac.fsum = ac.sum.Float()
		return ac.addFloat(v.Float())
	}
	return ac.sum.Add(v)
}

func (ac *accum) addFloat(f float64) error {
	ac.fsum += f
	return nil
}

// extend moves the running extreme if v lies beyond it.
func (ac *accum) extend(op AggOp, v record.Value) error {
	bits, ok := record.ScalarBitsOf(v)
	if !ok {
		// Unreachable via observe, which checks IsNumeric first. Kept as a
		// guard because storing bits of an arena-backed value would leave the
		// extreme pointing into a recycled batch.
		return fmt.Errorf("%v cannot be held as an extreme", v.Kind())
	}
	if !ac.seen {
		ac.ext = bits
		return nil
	}
	c, cmpOK := record.Compare(v, ac.ext.Value())
	if !cmpOK {
		// Reached by NaN, which is ordered against nothing. Reporting it beats
		// emitting an extreme that silently depends on arrival order.
		return fmt.Errorf("cannot order %v against the running extreme (%v)", v.Kind(), ac.ext.Kind)
	}
	if (op == AggMin && c < 0) || (op == AggMax && c > 0) {
		ac.ext = bits
	}
	return nil
}

// spillAll writes every partition's in-memory state to scratch segments and
// releases the reserved memory.
func (a *aggSource) spillAll() error {
	if a.store == nil {
		s, err := spill.NewStore(a.spec.SpillDir)
		if err != nil {
			return err
		}
		a.store = s
	}
	for pidx, part := range a.parts {
		if len(part) == 0 {
			continue
		}
		w, err := a.store.StartSegment()
		if err != nil {
			return err
		}
		if err := writePartition(w, part); err != nil {
			return err
		}
		seg, err := a.store.FinishSegment()
		if err != nil {
			return err
		}
		a.segs[pidx] = append(a.segs[pidx], seg)
		a.parts[pidx] = make(map[string][]accum)
	}
	a.spec.Gov.Release(a.reserved)
	a.reserved = 0
	return nil
}

// Accumulator flags, as spilled.
const (
	flagSeen    byte = 1 << 0
	flagInexact byte = 1 << 1
)

// writePartition spills one partition's state. Every field of every accum is
// written regardless of the agg's op: the alternative — writing only the fields
// the op uses — couples the segment layout to the spec, and a mismatch between
// writer and reader would surface as a plausible wrong total rather than as an
// error.
func writePartition(w *bufio.Writer, part map[string][]accum) error {
	var scratch [binary.MaxVarintLen64]byte
	enc := spill.NewEncoder(w)
	putUvarint := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, err := w.Write(scratch[:n])
		return err
	}
	putF64 := func(f float64) error {
		binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(f))
		_, err := w.Write(scratch[:8])
		return err
	}
	for key, accs := range part {
		if err := putUvarint(uint64(len(key))); err != nil {
			return err
		}
		if _, err := w.WriteString(key); err != nil {
			return err
		}
		for _, ac := range accs {
			if err := putUvarint(uint64(ac.count)); err != nil { //nolint:gosec // count is never negative
				return err
			}
			flags := byte(0)
			if ac.seen {
				flags |= flagSeen
			}
			if ac.inexact {
				flags |= flagInexact
			}
			if err := w.WriteByte(flags); err != nil {
				return err
			}
			// The exact accumulator's 128-bit state, then the float one.
			if err := putUvarint(ac.sum.Hi); err != nil {
				return err
			}
			if err := putUvarint(ac.sum.Lo); err != nil {
				return err
			}
			neg := byte(0)
			if ac.sum.Neg {
				neg = 1
			}
			if err := w.WriteByte(neg); err != nil {
				return err
			}
			//nolint:gosec // int8 -> byte is lossless for all 256 values; readAccum reverses it
			if err := w.WriteByte(byte(ac.sum.Scale)); err != nil {
				return err
			}
			if err := putF64(ac.fsum); err != nil {
				return err
			}
			// The extreme rides as a value, through the codec that already
			// knows every numeric kind (an unset extreme spills as null).
			if err := enc.Encode(ac.ext.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

// readAccum reads one spilled accumulator, the inverse of writePartition.
func (a *aggSource) readAccum(r *bufio.Reader, dec *spill.Decoder, ac *accum) error {
	c, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	ac.count = int64(c) //nolint:gosec // written from a non-negative int64 by writePartition
	flags, err := r.ReadByte()
	if err != nil {
		return err
	}
	ac.seen = flags&flagSeen != 0
	ac.inexact = flags&flagInexact != 0

	if ac.sum.Hi, err = binary.ReadUvarint(r); err != nil {
		return err
	}
	if ac.sum.Lo, err = binary.ReadUvarint(r); err != nil {
		return err
	}
	neg, err := r.ReadByte()
	if err != nil {
		return err
	}
	ac.sum.Neg = neg == 1
	scale, err := r.ReadByte()
	if err != nil {
		return err
	}
	ac.sum.Scale = int8(scale) //nolint:gosec // reverses the byte written by writePartition
	var raw [8]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return err
	}
	ac.fsum = math.Float64frombits(binary.LittleEndian.Uint64(raw[:]))

	// The extreme comes back through the value codec. It is retained beyond
	// this batch, so it must be an inline scalar — a corrupt segment claiming a
	// string would otherwise leave ext pointing into a recycled arena.
	if err := dec.Decode(a.extBatch.Builder()); err != nil {
		return err
	}
	ext := a.extBatch.Builder().Finish()
	if !ext.IsNull() && !ext.IsNumeric() {
		return fmt.Errorf("aggregate: spilled extreme is %v, want a number (corrupt segment?)", ext.Kind())
	}
	bits, ok := record.ScalarBitsOf(ext)
	if !ok {
		return fmt.Errorf("aggregate: spilled extreme is %v, which is not an inline scalar (corrupt segment?)", ext.Kind())
	}
	ac.ext = bits
	return nil
}

// merge folds a spilled accumulator into this one.
func (ac *accum) merge(op AggOp, o accum) error {
	switch op {
	case AggSum:
		if o.seen {
			if err := ac.mergeSum(o); err != nil {
				return err
			}
		}
	case AggMin, AggMax:
		if o.seen {
			if err := ac.extend(op, o.ext.Value()); err != nil {
				return err
			}
		}
	case AggCount:
		// The count below is the whole state.
	}
	ac.count += o.count
	ac.seen = ac.seen || o.seen
	return nil
}

// mergeSum combines two sums, which may be in different modes: whichever side
// has already gone inexact drags the other with it, since a float64 total
// cannot be recovered into exact digits.
func (ac *accum) mergeSum(o accum) error {
	switch {
	case ac.inexact && o.inexact:
		ac.fsum += o.fsum
	case ac.inexact:
		ac.fsum += o.sum.Float()
	case o.inexact:
		ac.inexact = true
		ac.fsum = ac.sum.Float() + o.fsum
	default:
		return ac.sum.AddSum(o.sum)
	}
	return nil
}

// mergePartition folds spilled segments into the partition's in-memory map.
func (a *aggSource) mergePartition(pidx int) (map[string][]accum, error) {
	part := a.parts[pidx]
	for _, seg := range a.segs[pidx] {
		r := bufio.NewReaderSize(a.store.OpenSegment(seg), 256<<10)
		var keyScratch []byte
		for {
			klen, err := binary.ReadUvarint(r)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			if klen > maxSpilledKeyBytes {
				return nil, fmt.Errorf("aggregate: spilled key of %d bytes exceeds limit (corrupt segment?)", klen)
			}
			if cap(keyScratch) < int(klen) {
				keyScratch = make([]byte, klen)
			}
			keyScratch = keyScratch[:klen]
			if _, err := io.ReadFull(r, keyScratch); err != nil {
				return nil, err
			}
			incoming := make([]accum, len(a.spec.Aggs))
			dec := spill.NewDecoder(r, 0)
			for i := range incoming {
				if err := a.readAccum(r, dec, &incoming[i]); err != nil {
					return nil, err
				}
			}
			if have, ok := part[string(keyScratch)]; ok {
				for i := range have {
					if err := have[i].merge(a.spec.Aggs[i].Op, incoming[i]); err != nil {
						return nil, fmt.Errorf("aggregate: merging spilled state: %w", err)
					}
				}
			} else {
				part[string(keyScratch)] = incoming
				cost := groupCost(len(keyScratch), len(a.spec.Aggs))
				a.spec.Gov.Reserve(cost)
				a.reserved += cost
			}
		}
	}
	a.segs[pidx] = nil
	return part, nil
}

func (a *aggSource) emit() (*record.Batch, error) {
	for len(a.emitQueue) == 0 {
		if a.emitPart >= len(a.parts) {
			return nil, io.EOF
		}
		part, err := a.mergePartition(a.emitPart)
		if err != nil {
			return nil, err
		}
		for key, accs := range part {
			a.emitQueue = append(a.emitQueue, emitGroup{key: key, accs: accs})
		}
		// Release this partition's state as it drains to output.
		a.parts[a.emitPart] = nil
		a.spec.Gov.Release(a.reserved)
		a.reserved = 0
		a.emitPart++
	}

	a.outBatch.Reset()
	bld := a.outBatch.Builder()
	n := min(len(a.emitQueue), a.spec.EmitBatchRecords)
	for _, g := range a.emitQueue[:n] {
		bld.BeginMap()
		bld.KeyLiteral(a.spec.KeyName)
		// Decode the group key back into a value (reader machinery reused).
		a.keyRdr.Reset([]byte(g.key))
		if a.keyBR == nil {
			a.keyBR = bufio.NewReader(&a.keyRdr)
		} else {
			a.keyBR.Reset(&a.keyRdr)
		}
		if err := spill.NewDecoder(a.keyBR, 0).Decode(bld); err != nil {
			return nil, fmt.Errorf("aggregate: decode key: %w", err)
		}
		for i, ag := range a.spec.Aggs {
			bld.KeyLiteral(ag.Out)
			ac := g.accs[i]
			switch ag.Op {
			case AggCount:
				bld.Int(ac.count)
			case AggSum:
				switch {
				case !ac.seen:
					bld.Null() // no non-null input: no sum, not zero
				case ac.inexact:
					bld.Float(ac.fsum)
				default:
					// An int column sums to an int, a decimal column to a
					// decimal at the finest scale any input used.
					sv, err := ac.sum.Value()
					if err != nil {
						return nil, fmt.Errorf("aggregate %s: %w", ag.Out, err)
					}
					bld.Value(sv)
				}
			case AggMin, AggMax:
				if ac.seen {
					bld.Value(ac.ext.Value()) // keeps the input's kind and scale
				} else {
					bld.Null()
				}
			}
		}
		bld.EndMap()
		a.outBatch.Append(bld.Finish())
	}
	a.emitQueue = a.emitQueue[n:]
	a.stats.RecordsOut += int64(a.outBatch.Len())
	return a.outBatch, nil
}

func (a *aggSource) Close() error {
	err := a.up.Close()
	if a.store != nil {
		if cerr := a.store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if a.reserved > 0 {
		a.spec.Gov.Release(a.reserved)
		a.reserved = 0
	}
	return err
}
