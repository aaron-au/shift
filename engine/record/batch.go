package record

// Batch is a set of records plus the arena and slab allocators their memory
// lives in. Batches are the unit of flow through pipelines (ADR-0004):
// backpressure, metrics, and spill decisions happen at batch boundaries.
//
// A Batch is not safe for concurrent use.
type Batch struct {
	recs    []Value
	arena   byteArena
	vals    valSlab
	keys    keySlab
	builder Builder
}

// NewBatch returns an empty batch.
func NewBatch() *Batch {
	b := &Batch{}
	b.builder.batch = b
	return b
}

// Len returns the number of records in the batch.
func (b *Batch) Len() int { return len(b.recs) }

// Record returns the i-th record.
func (b *Batch) Record(i int) Value { return b.recs[i] }

// Records returns the record slice. Callers may reorder or truncate it in
// place (e.g. filters) but must not append values from other batches.
func (b *Batch) Records() []Value { return b.recs }

// SetRecords replaces the record slice; used by in-place operators after
// compaction. The values must belong to this batch.
func (b *Batch) SetRecords(recs []Value) { b.recs = recs }

// Append adds a record built in this batch.
func (b *Batch) Append(v Value) { b.recs = append(b.recs, v) }

// Builder returns the batch's builder for constructing values inside it.
// The builder is shared; do not interleave two constructions.
func (b *Batch) Builder() *Builder { return &b.builder }

// Reset drops all records but keeps the largest allocator chunks, so warmed
// batches reach zero steady-state allocation.
func (b *Batch) Reset() {
	b.recs = b.recs[:0]
	b.arena.reset()
	b.vals.reset()
	b.keys.reset()
	b.builder.reset()
}

// MemSize returns the approximate bytes of memory this batch is holding
// (allocator capacity, not just bytes in use). Used for watermark
// accounting.
func (b *Batch) MemSize() int64 {
	return b.arena.memSize() + b.vals.memSize() + b.keys.memSize() +
		int64(cap(b.recs))*int64(valueSize)
}

// ArenaBytes returns the number of payload bytes copied into the arena
// since the last Reset — a good proxy for the data size of the batch.
func (b *Batch) ArenaBytes() int64 { return b.arena.used }
