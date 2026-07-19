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
	AggSum                // numeric sum (float64 accumulation)
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
		return p.fail(fmt.Errorf("stream: aggregate requires a governor"))
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

// accum is one aggregate function's running state.
type accum struct {
	count    int64
	sum      float64
	min, max float64
	seen     bool
}

// groupCost approximates per-group state bytes for governor accounting.
func groupCost(keyLen, naggs int) int64 {
	return int64(keyLen) + 64 + int64(naggs)*48
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
		accs = make([]accum, len(a.spec.Aggs))
		for i := range accs {
			accs[i].min = math.Inf(1)
			accs[i].max = math.Inf(-1)
		}
		part[string(keyBytes)] = accs
	}
	for i, ag := range a.spec.Aggs {
		switch ag.Op {
		case AggCount:
			accs[i].count++
			continue
		default:
		}
		v, ok := ag.From.Get(rec)
		if !ok || v.IsNull() {
			continue
		}
		var f float64
		switch v.Kind() {
		case record.KindInt, record.KindFloat:
			f = v.Float()
		default:
			return fmt.Errorf("aggregate: %s is %v, want numeric", ag.From, v.Kind())
		}
		acc := &accs[i]
		acc.seen = true
		acc.count++
		acc.sum += f
		acc.min = math.Min(acc.min, f)
		acc.max = math.Max(acc.max, f)
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

func writePartition(w *bufio.Writer, part map[string][]accum) error {
	var scratch [binary.MaxVarintLen64]byte
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
			if err := putF64(ac.sum); err != nil {
				return err
			}
			if err := putF64(ac.min); err != nil {
				return err
			}
			if err := putF64(ac.max); err != nil {
				return err
			}
			b := byte(0)
			if ac.seen {
				b = 1
			}
			if err := w.WriteByte(b); err != nil {
				return err
			}
		}
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
			for i := range incoming {
				c, err := binary.ReadUvarint(r)
				if err != nil {
					return nil, err
				}
				incoming[i].count = int64(c) //nolint:gosec // written from a non-negative int64 by writePartition
				var raw [8]byte
				for _, dst := range []*float64{&incoming[i].sum, &incoming[i].min, &incoming[i].max} {
					if _, err := io.ReadFull(r, raw[:]); err != nil {
						return nil, err
					}
					*dst = math.Float64frombits(binary.LittleEndian.Uint64(raw[:]))
				}
				sb, err := r.ReadByte()
				if err != nil {
					return nil, err
				}
				incoming[i].seen = sb == 1
			}
			if have, ok := part[string(keyScratch)]; ok {
				for i := range have {
					have[i].count += incoming[i].count
					have[i].sum += incoming[i].sum
					have[i].min = math.Min(have[i].min, incoming[i].min)
					have[i].max = math.Max(have[i].max, incoming[i].max)
					have[i].seen = have[i].seen || incoming[i].seen
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
				if ac.seen {
					bld.Float(ac.sum)
				} else {
					bld.Null()
				}
			case AggMin:
				if ac.seen {
					bld.Float(ac.min)
				} else {
					bld.Null()
				}
			case AggMax:
				if ac.seen {
					bld.Float(ac.max)
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
