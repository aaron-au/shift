# 02 — The Streaming Engine (`engine/`)

The engine is the product's core thesis made code: transformations that are
memory-bounded regardless of payload size, disk-touching only above an
explicit watermark, with honest metrics. It is **stdlib-only** — adding a
dependency to `engine` requires an ADR.

## The record model (`engine/record`)

A `record.Value` is one node of a hierarchical, typed tree:

- Scalars: null, bool, int64, float64, string, bytes. Stored inline in an
  88-byte struct; string/bytes payloads are **views into a batch arena**.
- Exact scalars (ADR-0051): decimal, timestamp, date, time. Also inline, and
  they cost **no extra memory**: `kind` is a `uint8` followed by alignment
  padding, so a second `int8` (`aux`) fits for free — it carries a decimal's
  scale and a timestamp's zone offset. `TestValueStaysEightyEightBytes`
  asserts the struct has not grown.
- Containers: list and map. Children live in contiguous slices handed out
  by the batch's slab allocators; maps keep parallel key slices and
  **preserve field order**. Field lookup is a linear scan (records are
  narrow; it benchmarks faster than map overhead at typical widths).

A decimal is exactly `coefficient × 10^-scale`, which is why a `NUMERIC(12,2)`
or a CSV money column survives a round trip that `float64` would round.
`record.Compare` orders `int` and `decimal` **without float64 in the path** (128
bits where a rescale would overflow), so no comparison result depends on binary
rounding; a comparison involving `float` is documented as inexact. Two numeric
towers therefore coexist — exact (`int`, `decimal`) and inexact (`float`) —
and mixing them is legal and lossy.

JSON keeps its existing behaviour: a bare `10.10` in NDJSON stays a float. Only
a **declared** type produces a decimal (a CSV/fixed-width column type, a
database `NUMERIC`, or an explicit `coerce`), because silently promoting every
fractional JSON number would change the output of every existing flow.

There is deliberately **no `map[string]interface{}` anywhere** — that
pattern is what makes the incumbents (and our own v0 prototype) slow.

### Batches and the lifetime contract

`record.Batch` owns three chunk allocators (byte arena, `[]Value` slab,
key slab). Chunks never grow in place, so views stay valid until `Reset()`;
`Reset` keeps the largest chunk of each allocator, which is why a warmed
batch reaches **zero steady-state allocations** (259 ns to build a nested
record, 0 allocs — `record` benchmarks).

**THE contract** (violating it is the classic engine bug):

> A batch returned by `Source.Next` is valid only until the next
> `Next`/`Close` call. Sources reuse batches. Anything that retains data
> across batches must deep-copy via `record.CopyValue(dstBatch, v)`.

Construction goes through `batch.Builder()` — a stack machine
(`BeginMap`/`Key`/`Int`/.../`EndMap`, then `Finish()`). On container close
the children are copied from scratch into exact-size slab slices. The
builder panics on malformed sequences (value without key, mismatched ends)
— these are programmer errors, not data errors.

### Paths

`record.ParsePath("$.addr.city")` compiles once at pipeline build time into
steps; evaluation is ~42 ns with zero allocations. **Never** resolve string
paths per record.

## Pipelines (`engine/stream`)

Pull-based: a `Source` produces batches; operators wrap the upstream source
and transform batches **in place** (they may rebuild records using the
flowing batch's own builder — everything shares that batch's allocators);
a `Sink` consumes. One batch is in flight per pipeline, so memory ≈ batch
size + explicit operator state.

- `Project(fields...)` — rebuild records as flat maps of compiled-path
  values (referenced, not copied).
- `Filter(name, pred)` — compacts the record slice in place; fully-filtered
  batches are skipped without surfacing downstream.
- `Coerce(rules...)` — in-place top-level type conversion (uses
  `Value.SetIndex`, which works because child slabs are shared).
- `Flatten(sep)` — nested maps to dotted top-level keys; single shared key
  buffer, zero steady-state allocs.
- `Aggregate(spec)` — the blocking, spillable group-by (below).

Every operator gets an `OpStats` (batches, records in/out, nanoseconds of
its own work only). `Pipeline.Run` returns the report; the runner persists
it per task. Metrics are honest or absent — never wall-clock-as-CPU (a v0
sin).

## Memory governance and spill (`engine/mem`, `engine/spill`)

`mem.Governor` is watermark accounting: `TryReserve(n)` fails when the
budget would be exceeded — **a failed reservation is the spill signal**,
not an error. `Reserve` (unconditional) exists so accounting stays honest
when an allocation already happened.

`spill.Store` is the only sanctioned disk-touch: one `os.CreateTemp`'d file,
**unlinked immediately** (survives until close, vanishes on any process
death), append-only segments, `io.SectionReader` reads. Never a directory
of small files. `spill.Encoder/Decoder` is the compact binary codec for
values — also reused as the connector wire framing (ADR-0007).

That reuse has a consequence worth stating where the codec is described: the
spill file never outlives its process, but a **frame does cross to another
process**, and a signed connector version stays runnable for as long as a flow
pins it (ADR-0047). So codec tags are **append-only** — existing numbers and
payloads are never changed — and new kinds are gated by the negotiated
`sdk.ProtocolVersion` (2 since ADR-0051). Pushing to a connector that
negotiated 1 uses `spill.NewEncoderProtocol1`, which refuses the exact kinds
with an actionable message instead of letting the subprocess fail on an unknown
tag.

### The spillable aggregate, concretely

1. Group keys are encoded to bytes (the binary codec) and hash-partitioned
   (default 8 partitions, `maphash` seed per run).
2. Each new group `TryReserve`s its estimated cost. On failure: **all**
   partitions serialize their partial accumulators to one segment each,
   maps are cleared, memory released, and accumulation continues.
3. At emit time, partitions merge one at a time (in-memory state + their
   segments), bounding merge memory to the largest partition, and emit as
   normal batches.
4. Spilled vs unspilled results are byte-identical (tested), including the
   scale of a decimal and the zone offset of a timestamp — asserted on the
   canonical text, because a dropped scale still compares numerically equal.

Accumulators: count (`int64`), sum, and one running extreme for MIN/MAX.

`SUM` accumulates **exactly** in 128 bits for `int` and `decimal` inputs
(ADR-0051 §3, closing issue #4 — the old `float64` accumulator could not
represent `2^53+1`), and in `float64` only from the moment a `float` appears in
the column. So an int column sums to an `int`, a decimal column to a `decimal`
at the finest scale any input used, and a mixed column to a `float`. A total
that cannot be represented is an **error**, not a wrap and not a saturated
value: both of those are indistinguishable from a correct total downstream. The
exact accumulator is only consulted for an `AggSum`, so `MIN` over values too
large to add still works.

MIN/MAX keep the extreme as `record.ScalarBits` — a scalar's inline payload, 16
bytes rather than a `Value`'s 88 — so an extreme comes out with the kind and
scale of the input it came from rather than widened to a float. The compact form
is not a micro-optimisation: one `Value` per group per agg measured 317.9 MiB at
a million groups against 202.0 MiB for the bits (docs/bench-M1.md). Holding it
across batches is safe **only** because a numeric scalar has no arena or slab
pointers; `observe` rejects every non-numeric kind before it can reach there,
and the spill reader re-checks, so a corrupt segment cannot leave an extreme
pointing into a recycled arena.

## Multi-path execution — fan-out and fan-in (ADR-0029)

Everything above describes one pull chain. Flow model v3 makes the data path
a DAG, and the pull model is preserved *within* each linear segment — a
maximal one-in/one-out run of operators is exactly the fused `Pipeline`
above. The new machinery lives only at the seams between segments.

### Fan-out: `engine/stream/fanout.go`

One goroutine drives the upstream segment (a single producer — the source is
never pulled twice), and each branch runs as its own consumer goroutine
reading a **bounded queue** of batches.

The hard part is the batch-lifetime contract: a batch is valid only until the
next `Next`. Handing the same batch to two branches that pull at different
rates violates it immediately. The resolution is ownership, not aliasing:

- A branch marked `Shared` reads the snapshot with **no copy**, under a
  refcount; the arena returns to the pool when the last branch releases it.
  The caller sets `Shared` only where the branch provably never mutates — a
  tee straight to a sink, or a router branch that already owns its batch
  exclusively.
- Every other branch **copy-on-writes**: it copies the snapshot into its own
  batch and releases the shared one, so its operators may mutate freely.
  `Shared` is opt-in and `false` (⇒ COW) is always the safe default.

A `router` never copies at all — each record goes to exactly one branch, so
it *partitions* the input batch into per-branch batches (moves, not copies).

**Backpressure** is the bounded queues themselves. When a branch's queue is
full the driver blocks on that branch; it never skips ahead and never grows
the queue. The tee therefore runs at the pace of its slowest branch, and at
most `depth × branches` batches are ever in flight. Spill under sustained
backpressure (ADR-0029 §2) is deliberately **not** built: blocking is already
memory-bounded and correct, so spill would only decouple a slow branch — an
optimization, not a correctness fix.

A branch error becomes an `OpError` tagged with the branch's step id, cancels
the shared context, and tears down its siblings; `Close` joins every branch
goroutine before the node reports done, so no goroutine outlives the task.

### Fan-in: `engine/stream/merge.go`, `join.go`

- **`concat`** — a fair multiplex: forward batches from whichever input has
  one ready, under the same bounded-queue backpressure. Fully streaming,
  O(batch) memory, records **moved** not copied, interleaved order.
- **`join`** — a keyed equi-join, and the engine's second blocking operator.
  It reuses the aggregate's spill machinery exactly: the **build** (right)
  input is consumed fully into a hash table keyed by the encoded join value,
  each row `TryReserve`ing its cost and the table **grace-hash spilling
  partitions** on watermark. The **probe** (left) input then streams one
  batch at a time, loading the relevant spilled partition on demand — so
  merge memory is bounded by the largest partition, not total cardinality.
  Build rows are `record.CopyValue`'d in (they must outlive their source
  batch); probe rows are read within their batch and emitted immediately.

  Blocking on the build side only means the common enrichment shape — large
  left, small right — stays memory-bounded by the right input's table.

The runner compiles a v3 `Plan` onto these primitives in
`runner/internal/service/service_multipath.go`; see `docs/dev/04-runner.md`
for which topologies it currently supports (issue #59).

## Formats (`engine/format/...`)

- `ndjson`: hand-rolled within-line recursive-descent parser building
  directly into batch arenas. Unescaped strings copy exactly once
  (input → arena). Differential- **and fuzz-tested against
  `encoding/json`** — both parsers must agree on accept/reject and value;
  run the fuzzer when touching it (`go test -fuzz=FuzzDifferential`).
  Strictness notes: JSON number grammar enforced (stdlib's `ParseFloat`
  alone would accept `01`, `1.`); raw invalid UTF-8 passes through on read
  and is sanitized on write (documented divergence). **Reading is unchanged by
  ADR-0051** — a bare `10.10` is still a float — because promoting every
  fractional JSON number would move the output of every existing flow. Writing
  a decimal emits a bare JSON number with the exact digits (quoting it would
  change the field's JSON *type* and break schema-validating consumers);
  temporal values write as RFC 3339 / ISO 8601 strings, JSON having no
  temporal type.
- `csvf`: `encoding/csv` in `ReuseRecord` mode + per-column type hints
  (int/float/bool/decimal/timestamp/date/time; empty typed cells → null; cells
  are trimmed before typed parsing, since CSV exported from a fixed-width
  system carries padding). `TypeDecimal` is what makes a money column
  round-trip: read and written back, `10.10` is still `10.10`, where a float
  column returns `10.1` (both directions are asserted in
  `TestAMoneyColumnRoundTripsExactly`).
- `xmlf`: streaming reader over `encoding/xml`'s token stream (at most one
  record's subtree resident), plus a writer that is its **exact inverse** —
  attributes back to attributes, `#text` back to character data, a list back
  to the repeated element that produced it, so a document round-trips.
  Namespace prefixes deliberately do NOT survive: the reader strips them, so
  inventing one on the way out would be a guess.
- `edi`: X12 and EDIFACT, one record per **segment**
  (`{tag, elements:[[components]]}`). Structure, not semantics — no envelope
  grouping, no transaction-set validation. Delimiters are discovered from the
  interchange (X12's fixed-width ISA header, EDIFACT's optional UNA), never
  configured, because a configured separator disagrees with the file the moment
  a partner changes theirs. Every element is always a list of components, so a
  composite arriving where a partner previously sent a scalar does not change
  the shape downstream. Read-only.
- Writers stream; the NDJSON writer has an escape fast path and matches
  encoding/json's float formatting choices.

**Reaching a flow.** A format in `engine/format` is invisible until a connector
offers it. `connectors/internal/fileformat` is the single registry the
file-shaped connectors (fs, sftp, ftp, s3, azureblob) delegate to for the
enum, validation, and reader/writer construction — so adding a format is one
edit rather than one per connector, and an unknown format is an error rather
than a silent fall-through to NDJSON. `xmlf` shipped without that wiring and
was unusable from any node until it was added.

## The proof harness (`engine/cmd/shift-bench`)

Deterministic generator (no disk I/O in the measurement), scenarios
`transform|csv|aggregate|baseline`, and `-max-rss` which turns a run into
a pass/fail regression check — CI runs transform and aggregate this way on
every push. `baseline` is the naive buffered implementation, kept
deliberately: it quantifies what the engine saves (80×/2× at 256 MiB).

## Known follow-ups

Tracked as GitHub issues #1–#3: tokenizer is 63% of transform cost, float
parse allocates, aggregate merge accounting. Issue #4 (float sum precision) is
closed by the exact accumulator above.
