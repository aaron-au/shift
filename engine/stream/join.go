package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	// exceeds the watermark the join fails honestly (a spilling build table is
	// a later change — bounded memory is never traded for an OOM).
	Gov *mem.Governor
	// EmitBatchRecords sizes output batches (default 1024).
	EmitBatchRecords int
}

// Join builds a hash table from the build (right) input, then streams the
// probe (left) input, emitting each probe record joined to every build record
// sharing its key. Output records carry the probe's fields plus the matched
// build record nested under As. It is a blocking operator on the build side
// only: the probe streams, so a large-probe / small-build enrichment stays
// bounded by the build table.
//
// Ownership: build records are retained across batches, so each is copied into
// a table-owned batch (the one unavoidable copy, per ADR-0029). Probe records
// and matched build records are referenced directly when emitting — both stay
// valid for the lifetime of the returned batch (probe within its source
// batch, build in the stable table), so the join copies nothing on the probe
// path.
func Join(probe, build Source, spec JoinSpec) Source {
	return &joinSource{probe: probe, build: build, spec: spec}
}

type joinSource struct {
	probe, build Source
	spec         JoinSpec

	built    bool
	table    map[string][]record.Value // encoded key -> build records (in buildBatch)
	buildRec *record.Batch             // owns retained build-record copies
	reserved int64

	keyBuf bytes.Buffer
	keyEnc *spill.Encoder

	outBatch *record.Batch
}

// perBuildRowCost is a heuristic byte charge per retained build row for
// watermark accounting (key + record overhead); precise sizing is not needed,
// only a bound on table growth.
const perBuildRowCost = 256

func (j *joinSource) Next(ctx context.Context) (*record.Batch, error) {
	if !j.built {
		if j.spec.Gov == nil {
			return nil, errors.New("stream: join requires a governor")
		}
		if j.spec.As == "" {
			return nil, errors.New("stream: join requires an As field")
		}
		if j.spec.EmitBatchRecords <= 0 {
			j.spec.EmitBatchRecords = 1024
		}
		if err := j.buildTable(ctx); err != nil {
			return nil, err
		}
		j.outBatch = record.NewBatch()
		j.built = true
	}
	return j.probeNext(ctx)
}

// buildTable consumes the entire build input into an in-memory hash table
// keyed by the encoded RightKey, copying each build record into buildRec so it
// outlives its source batch.
func (j *joinSource) buildTable(ctx context.Context) error {
	j.table = make(map[string][]record.Value)
	j.buildRec = record.NewBatch()
	j.keyEnc = spill.NewEncoder(&j.keyBuf)
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
				return fmt.Errorf("join: build side exceeds memory watermark %d (a spilling join is not yet implemented)", j.spec.Gov.Budget())
			}
			j.reserved += cost
			owned := record.CopyValue(j.buildRec, rec)
			j.table[key] = append(j.table[key], owned)
		}
	}
}

// probeNext pulls one probe batch and emits its joined records, skipping empty
// results (all-inner-dropped) by pulling the next batch.
func (j *joinSource) probeNext(ctx context.Context) (*record.Batch, error) {
	for {
		pb, err := j.probe.Next(ctx)
		if err != nil {
			return nil, err // io.EOF included
		}
		j.outBatch.Reset()
		bld := j.outBatch.Builder()
		for _, prec := range pb.Records() {
			kv, _ := j.spec.LeftKey.Get(prec)
			var matches []record.Value
			if !kv.IsNull() {
				key, err := j.encodeKey(kv)
				if err != nil {
					return nil, err
				}
				matches = j.table[key]
			}
			if len(matches) == 0 {
				if j.spec.Type == JoinTypeLeft {
					j.emit(bld, prec, record.Value{}, true)
				}
				continue // inner: drop unmatched
			}
			for _, brec := range matches {
				j.emit(bld, prec, brec, false)
			}
		}
		if j.outBatch.Len() == 0 {
			continue // this batch produced nothing; pull the next
		}
		return j.outBatch, nil
	}
}

// emit builds one joined record: the probe's fields plus the matched build
// record (or null) under As. Probe and build values are referenced directly —
// both outlive the returned batch — so nothing is copied here.
func (j *joinSource) emit(bld *record.Builder, probe, build record.Value, nullMatch bool) {
	bld.BeginMap()
	for i := range probe.Len() {
		bld.Key(probe.KeyAt(i))
		bld.Value(probe.Index(i))
	}
	bld.KeyLiteral(j.spec.As)
	if nullMatch {
		bld.Null()
	} else {
		bld.Value(build)
	}
	bld.EndMap()
	j.outBatch.Append(bld.Finish())
}

func (j *joinSource) encodeKey(kv record.Value) (string, error) {
	j.keyBuf.Reset()
	if err := j.keyEnc.Encode(kv); err != nil {
		return "", err
	}
	return j.keyBuf.String(), nil
}

// Close closes both inputs and releases the build table's reservation.
func (j *joinSource) Close() error {
	err := j.probe.Close()
	if cerr := j.build.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if j.reserved > 0 && j.spec.Gov != nil {
		j.spec.Gov.Release(j.reserved)
		j.reserved = 0
	}
	return err
}
