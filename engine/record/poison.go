package record

// Batch-lifetime enforcement (TC-001's sibling, TC-009 in
// docs/assurance/test-conformance.md).
//
// The contract every operator, reader and connector in this codebase is held
// to: a batch handed out by Next is valid only until the NEXT Next or Close.
// Anything you keep past that point must be copied out with CopyValue.
//
// Go cannot catch a violation on its own. A retained Value keeps its arena
// chunk alive, so a retaining operator reads plausible — often correct — data
// and every assertion passes. The bug only appears in production, once real
// traffic makes the reuse pattern differ from the test's. That is exactly how
// the v0 prototype accumulated whole-payload buffering: nothing said no.
//
// Poison says no. Scribbling over a retired batch's allocator memory turns a
// silent retention into a loud one: the retained Value now reads the marker
// instead of its data, and whatever assertion the test already makes about the
// output fails. It does not need a new assertion — it makes the existing ones
// mean what they claim.

// poisonPattern is repeated across retired arena bytes. Chosen to be readable
// in a failure diff: a test that prints the corrupted value shows the word
// rather than an unprintable byte run, which is the difference between "this
// is the lifetime harness talking" and half an hour of confusion.
var poisonPattern = []byte("!POISONED-BATCH!")

// poisonKey replaces retired object keys.
var poisonKey = []byte("!poisoned-key!")

// Poison overwrites the memory this batch's allocators own and then RELEASES
// it, so that any Value still pointing into the batch reads as garbage rather
// than as its old contents — and keeps reading garbage afterwards.
//
// For TEST USE. Call it on a batch whose lifetime has ended: right before the
// source that owns it produces the next one, which is exactly when the
// contract says every reference to it became invalid.
//
// Releasing the chunks is not an optimisation, it is the point. Sources reuse
// one batch, so scribbling alone is nearly useless: the source immediately
// resets and writes fresh data over the same arena, and a stale pointer reads
// the NEW record instead of the marker — plausible, wrong, and identical to
// what it would have read without any poisoning at all. Dropping the chunks
// means the refill allocates elsewhere, the stale pointer keeps the poisoned
// array alive all to itself, and the retention finally becomes visible.
//
// (Found by the harness's own non-vacuity test, which deliberately retains a
// value across batches and failed to be caught by the scribble-only version.)
//
// What it corrupts is what a Value can point INTO: string/bytes payloads (the
// arena), nested object and array elements (the value slab), and object key
// names (the key slab). Scalars — int, float, bool, decimal, the temporal
// kinds — live inline in the Value struct, so a copied scalar Value survives
// poisoning. That is correct and not a gap: copying a scalar out of a batch is
// legal precisely because it carries no pointer back into one.
//
// The batch stays fully usable — the next Reset simply starts from empty
// allocators and allocates fresh chunks.
func (b *Batch) Poison() {
	b.arena.poison()
	b.vals.poison()
	b.keys.poison()

	// Release: the next fill must not land on the memory just poisoned.
	b.recs = b.recs[:0]
	b.arena.chunks, b.arena.used = nil, 0
	b.vals.chunks, b.vals.used = nil, 0
	b.keys.chunks, b.keys.used = nil, 0
	b.builder.reset()
}

// poison fills every arena chunk to its capacity, not just its length: a Value
// handed out earlier may point past the current length after a reset.
func (a *byteArena) poison() {
	for _, c := range a.chunks {
		full := c[:cap(c)]
		for i := range full {
			full[i] = poisonPattern[i%len(poisonPattern)]
		}
	}
}

func (s *valSlab) poison() {
	dead := Value{kind: KindString, str: poisonPattern}
	for _, c := range s.chunks {
		full := c[:cap(c)]
		for i := range full {
			full[i] = dead
		}
	}
}

func (s *keySlab) poison() {
	for _, c := range s.chunks {
		full := c[:cap(c)]
		for i := range full {
			full[i] = poisonKey
		}
	}
}
