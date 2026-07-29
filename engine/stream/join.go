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

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// JoinType selects unmatched-probe handling.
type JoinType uint8

// Join types (ADR-0029): inner drops unmatched probe records; left keeps them
// with a null match under the As field.
const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
)

// JoinSpec configures a keyed equi-join — the join mode of a v3 merge node
// (ADR-0029). It joins a streamed probe (left) input against a built (right)
// input on the linked element (LeftKey == RightKey).
type JoinSpec struct {
	// LeftKey/RightKey locate the (scalar) join key in probe and build records.
	LeftKey  record.Path
	RightKey record.Path
	// As names the output field the matched build record is nested under, so
	// probe and build field names never collide.
	As string
	// Type is inner or left.
	Type JoinType
	// Gov bounds the in-memory build table. Required. When the build side
	// exceeds the watermark, the join spills partitions to scratch and falls
	// back to a grace hash join — memory stays bounded, never traded for an OOM.
	Gov *mem.Governor
	// SpillDir hosts the scratch file ("" = OS temp). Created lazily on first spill.
	SpillDir string
	// Partitions is the build/probe partition fan-out (default 8): grace-mode
	// memory is bounded by the largest single partition, not total cardinality.
	Partitions int
	// EmitBatchRecords sizes output batches (default 1024).
	EmitBatchRecords int
}

// Join builds a hash table from the build (right) input, then streams the
// probe (left) input, emitting each probe record joined to every build record
// sharing its key. Output records carry the probe's fields plus the matched
// build record nested under As. inner drops unmatched probe records; left keeps
// them with a null As.
//
// It is a blocking operator on the build side. While the build table fits under
// the memory watermark it stays fully in memory and the probe streams straight
// through (zero-copy on the probe path). When the build exceeds the watermark
// the join switches to a grace hash join: build records are partitioned by key
// and spilled to scratch, the probe is buffered to scratch, and the second pass
// joins one partition at a time — so memory is bounded by the largest partition
// regardless of total build size. Keys use the spill Value codec; null keys
// never equi-match (SQL semantics).
func Join(probe, build Source, spec JoinSpec) Source {
	return &joinSource{probe: probe, build: build, spec: spec, seed: fixedSeed}
}

// fixedSeed keeps partition assignment deterministic across a run (a task is a
// single goroutine chain, so a process-global seed is unnecessary and would
// break the build/probe partitioning agreement if it differed).
var fixedSeed = maphash.MakeSeed()

// perBuildRowCost is a heuristic byte charge per retained build row for
// watermark accounting (key + record overhead); precise sizing is not needed,
// only a bound on table growth.
const perBuildRowCost = 256

type joinPart struct {
	rows  map[string][]record.Value // encoded key -> build records (in batch)
	batch *record.Batch             // owns this partition's retained records
	segs  []spill.Segment           // spilled build segments for this partition
}

// join execution states.
const (
	jsInit  = iota // nothing consumed yet
	jsFast         // build fit in memory; stream the probe with a partitioned lookup
	jsGrace        // build spilled; second-pass partition-at-a-time join
	jsDone
)

type joinSource struct {
	probe, build Source
	spec         JoinSpec
	seed         maphash.Seed

	state    int
	parts    []joinPart
	spilled  bool
	store    *spill.Store
	reserved int64
	keyBuf   bytes.Buffer
	keyEnc   *spill.Encoder
	outBatch *record.Batch

	// grace second-pass cursor
	probeSeg     spill.Segment
	haveProbeSeg bool
	gi           int            // current build partition being joined
	gdec         *spill.Decoder // decoder over probeSeg for the current partition
	gprobeBatch  *record.Batch  // holds the currently-decoded probe record
	gloaded      bool           // build partition gi loaded + probe scan open
}

func (j *joinSource) Next(ctx context.Context) (*record.Batch, error) {
	if j.state == jsInit {
		if err := j.init(ctx); err != nil {
			return nil, err
		}
	}
	switch j.state {
	case jsFast:
		return j.fastNext(ctx)
	case jsGrace:
		return j.graceNext(ctx)
	default:
		return nil, io.EOF
	}
}

func (j *joinSource) init(ctx context.Context) error {
	if j.spec.Gov == nil {
		return errors.New("stream: join requires a governor")
	}
	if j.spec.As == "" {
		return errors.New("stream: join requires an As field")
	}
	if j.spec.EmitBatchRecords <= 0 {
		j.spec.EmitBatchRecords = 1024
	}
	if j.spec.Partitions <= 0 {
		j.spec.Partitions = 8
	}
	j.parts = make([]joinPart, j.spec.Partitions)
	for i := range j.parts {
		j.parts[i] = joinPart{rows: make(map[string][]record.Value), batch: record.NewBatch()}
	}
	j.keyEnc = spill.NewEncoder(&j.keyBuf)
	j.outBatch = record.NewBatch()
	if err := j.buildTable(ctx); err != nil {
		return err
	}
	if j.spilled {
		if err := j.spillProbe(ctx); err != nil {
			return err
		}
		j.state = jsGrace
	} else {
		j.state = jsFast
	}
	return nil
}

func (j *joinSource) partIdx(key string) int {
	return int(maphash.String(j.seed, key) % uint64(len(j.parts))) //nolint:gosec // < len(parts), fits int
}

func (j *joinSource) encodeKey(kv record.Value) (string, error) {
	j.keyBuf.Reset()
	if err := j.keyEnc.Encode(kv); err != nil {
		return "", err
	}
	return j.keyBuf.String(), nil
}

// buildTable consumes the entire build input, partitioning records by their
// RightKey. Partitions stay in memory until the governor watermark is hit, at
// which point every partition spills to scratch (grace mode).
func (j *joinSource) buildTable(ctx context.Context) error {
	for {
		b, err := j.build.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		for _, rec := range b.Records() {
			kv, _ := j.spec.RightKey.Get(rec)
			if kv.IsNull() {
				continue // a null key never equi-matches (SQL semantics)
			}
			switch kv.Kind() {
			case record.KindList, record.KindMap:
				return fmt.Errorf("join: build key %s is a container", j.spec.RightKey)
			}
			key, err := j.encodeKey(kv)
			if err != nil {
				return err
			}
			cost := int64(len(key)) + perBuildRowCost
			if !j.spec.Gov.TryReserve(cost) {
				if j.reserved == 0 {
					return fmt.Errorf("join: watermark %d too small for a single build row", j.spec.Gov.Budget())
				}
				if err := j.spillBuild(); err != nil {
					return err
				}
				j.spec.Gov.Reserve(cost) // post-spill: account unconditionally
			}
			j.reserved += cost
			p := &j.parts[j.partIdx(key)]
			owned := record.CopyValue(p.batch, rec)
			p.rows[key] = append(p.rows[key], owned)
		}
	}
}

// spillBuild writes every partition's in-memory rows to a scratch segment,
// clears the maps, and releases the reservation. A partition may accumulate
// several segments over successive spills (plus any residual rows read after
// the last spill).
func (j *joinSource) spillBuild() error {
	if err := j.ensureStore(); err != nil {
		return err
	}
	for i := range j.parts {
		p := &j.parts[i]
		if len(p.rows) == 0 {
			continue
		}
		w, err := j.store.StartSegment()
		if err != nil {
			return err
		}
		if err := writeBuildPartition(w, spill.NewEncoder(w), p.rows); err != nil {
			return err
		}
		seg, err := j.store.FinishSegment()
		if err != nil {
			return err
		}
		p.segs = append(p.segs, seg)
		p.rows = make(map[string][]record.Value)
		p.batch = record.NewBatch() // discard spilled records; free their arena
	}
	j.spec.Gov.Release(j.reserved)
	j.reserved = 0
	j.spilled = true
	return nil
}

func (j *joinSource) ensureStore() error {
	if j.store != nil {
		return nil
	}
	s, err := spill.NewStore(j.spec.SpillDir)
	if err != nil {
		return err
	}
	j.store = s
	return nil
}

// writeBuildPartition serializes a partition's rows: for each key, the key
// bytes then the record count then each record via the Value codec.
func writeBuildPartition(w *bufio.Writer, enc *spill.Encoder, rows map[string][]record.Value) error {
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, err := w.Write(scratch[:n])
		return err
	}
	for key, recs := range rows {
		if err := putUvarint(uint64(len(key))); err != nil {
			return err
		}
		if _, err := w.WriteString(key); err != nil {
			return err
		}
		if err := putUvarint(uint64(len(recs))); err != nil {
			return err
		}
		for _, r := range recs {
			if err := enc.Encode(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// spillProbe buffers the entire probe input to one scratch segment (grace mode).
// The second pass re-scans it once per build partition; each probe record is
// considered only on its own partition's scan, so a left join emits each
// unmatched probe row exactly once.
func (j *joinSource) spillProbe(ctx context.Context) error {
	if err := j.ensureStore(); err != nil {
		return err
	}
	w, err := j.store.StartSegment()
	if err != nil {
		return err
	}
	enc := spill.NewEncoder(w)
	any := false
	for {
		b, err := j.probe.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		for _, rec := range b.Records() {
			if err := enc.Encode(rec); err != nil {
				return err
			}
			any = true
		}
	}
	seg, err := j.store.FinishSegment()
	if err != nil {
		return err
	}
	j.probeSeg, j.haveProbeSeg = seg, any
	j.gprobeBatch = record.NewBatch()
	return nil
}

// fastNext is the in-memory path: the whole build fit, so stream the probe and
// look each record up in its key's partition. Zero-copy on the probe path.
func (j *joinSource) fastNext(ctx context.Context) (*record.Batch, error) {
	for {
		pb, err := j.probe.Next(ctx)
		if err != nil {
			return nil, err // io.EOF included
		}
		j.outBatch.Reset()
		bld := j.outBatch.Builder()
		for _, prec := range pb.Records() {
			matches, err := j.lookup(prec)
			if err != nil {
				return nil, err
			}
			j.emitRow(bld, prec, matches, false)
		}
		if j.outBatch.Len() == 0 {
			continue
		}
		return j.outBatch, nil
	}
}

// graceNext joins one build partition at a time: load partition gi, scan the
// probe segment, and emit rows for probe records that belong to gi. Emits at
// most EmitBatchRecords per call, resuming across calls.
func (j *joinSource) graceNext(ctx context.Context) (*record.Batch, error) {
	for {
		if j.gi >= len(j.parts) {
			j.state = jsDone
			return nil, io.EOF
		}
		if !j.gloaded {
			if err := j.loadBuildPartition(j.gi); err != nil {
				return nil, err
			}
			if err := j.openProbeScan(); err != nil {
				return nil, err
			}
			j.gloaded = true
		}
		j.outBatch.Reset()
		bld := j.outBatch.Builder()
		partitionDone := false
		for j.outBatch.Len() < j.spec.EmitBatchRecords {
			prec, ok, err := j.nextProbeForPartition()
			if err != nil {
				return nil, err
			}
			if !ok {
				partitionDone = true
				break
			}
			matches, err := j.lookup(prec)
			if err != nil {
				return nil, err
			}
			j.emitRow(bld, prec, matches, true) // grace: probe from a reused batch → copy
		}
		if partitionDone {
			j.freeBuildPartition(j.gi)
			j.gi++
			j.gloaded = false
		}
		if j.outBatch.Len() > 0 {
			return j.outBatch, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// nextProbeForPartition decodes probe records from the current scan until one
// belongs to partition gi (skipping the rest), or the scan ends.
func (j *joinSource) nextProbeForPartition() (record.Value, bool, error) {
	for {
		j.gprobeBatch.Reset()
		bld := j.gprobeBatch.Builder()
		if err := j.gdec.Decode(bld); err != nil {
			if errors.Is(err, io.EOF) {
				return record.Value{}, false, nil
			}
			return record.Value{}, false, err
		}
		prec := bld.Finish()
		kv, _ := j.spec.LeftKey.Get(prec)
		if kv.IsNull() {
			// Null-keyed probe belongs to no partition; surface it once, on
			// partition 0, so a left join still emits it (inner drops it).
			if j.gi == 0 {
				return prec, true, nil
			}
			continue
		}
		key, err := j.encodeKey(kv)
		if err != nil {
			return record.Value{}, false, err
		}
		if j.partIdx(key) == j.gi {
			return prec, true, nil
		}
	}
}

func (j *joinSource) openProbeScan() error {
	if !j.haveProbeSeg {
		// No probe records: an empty decoder yields immediate EOF.
		j.gdec = spill.NewDecoder(bufio.NewReader(bytes.NewReader(nil)), 0)
		return nil
	}
	r := bufio.NewReaderSize(j.store.OpenSegment(j.probeSeg), 256<<10)
	j.gdec = spill.NewDecoder(r, 0)
	return nil
}

// loadBuildPartition merges partition i's spilled segments into its in-memory
// rows so the whole partition is resident for probing.
func (j *joinSource) loadBuildPartition(i int) error {
	p := &j.parts[i]
	for _, seg := range p.segs {
		r := bufio.NewReaderSize(j.store.OpenSegment(seg), 256<<10)
		dec := spill.NewDecoder(r, 0)
		for {
			klen, err := binary.ReadUvarint(r)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			key := make([]byte, klen)
			if _, err := io.ReadFull(r, key); err != nil {
				return err
			}
			count, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			ks := string(key)
			for range count {
				bld := p.batch.Builder()
				if err := dec.Decode(bld); err != nil {
					return err
				}
				v := bld.Finish()
				p.batch.Append(v)
				p.rows[ks] = append(p.rows[ks], v)
			}
		}
	}
	p.segs = nil
	return nil
}

func (j *joinSource) freeBuildPartition(i int) {
	j.parts[i].rows = nil
	j.parts[i].batch = nil
}

// lookup returns the build records matching a probe record's key (nil for a
// null key, which never matches).
func (j *joinSource) lookup(prec record.Value) ([]record.Value, error) {
	kv, _ := j.spec.LeftKey.Get(prec)
	if kv.IsNull() {
		return nil, nil
	}
	key, err := j.encodeKey(kv)
	if err != nil {
		return nil, err
	}
	return j.parts[j.partIdx(key)].rows[key], nil
}

// emitRow appends the joined output for one probe record: its fields plus each
// matched build record nested under As (or a null As for an unmatched left
// join; inner drops unmatched). copyProbe deep-copies the probe fields into the
// output batch — required in grace mode where the probe record lives in a
// reused decode batch.
func (j *joinSource) emitRow(bld *record.Builder, probe record.Value, matches []record.Value, copyProbe bool) {
	if len(matches) == 0 {
		if j.spec.Type == JoinTypeLeft {
			j.writeJoined(bld, probe, record.Value{}, true, copyProbe)
		}
		return
	}
	for _, b := range matches {
		j.writeJoined(bld, probe, b, false, copyProbe)
	}
}

func (j *joinSource) writeJoined(bld *record.Builder, probe, build record.Value, nullMatch, copyProbe bool) {
	bld.BeginMap()
	for i := range probe.Len() {
		bld.Key(probe.KeyAt(i))
		v := probe.Index(i)
		if copyProbe {
			v = record.CopyValue(j.outBatch, v)
		}
		bld.Value(v)
	}
	bld.KeyLiteral(j.spec.As)
	if nullMatch {
		bld.Null()
	} else {
		bld.Value(build) // build lives in the (stable) partition batch — referenceable
	}
	bld.EndMap()
	j.outBatch.Append(bld.Finish())
}

// Close closes both inputs, the scratch store, and releases any reservation.
func (j *joinSource) Close() error {
	err := j.probe.Close()
	if cerr := j.build.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if j.store != nil {
		if cerr := j.store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if j.reserved > 0 && j.spec.Gov != nil {
		j.spec.Gov.Release(j.reserved)
		j.reserved = 0
	}
	return err
}
