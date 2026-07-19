# 02 — The Streaming Engine (`engine/`)

The engine is the product's core thesis made code: transformations that are
memory-bounded regardless of payload size, disk-touching only above an
explicit watermark, with honest metrics. It is **stdlib-only** — adding a
dependency to `engine` requires an ADR.

## The record model (`engine/record`)

A `record.Value` is one node of a hierarchical, typed tree:

- Scalars: null, bool, int64, float64, string, bytes. Stored inline in an
  88-byte struct; string/bytes payloads are **views into a batch arena**.
- Containers: list and map. Children live in contiguous slices handed out
  by the batch's slab allocators; maps keep parallel key slices and
  **preserve field order**. Field lookup is a linear scan (records are
  narrow; it benchmarks faster than map overhead at typical widths).

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

### The spillable aggregate, concretely

1. Group keys are encoded to bytes (the binary codec) and hash-partitioned
   (default 8 partitions, `maphash` seed per run).
2. Each new group `TryReserve`s its estimated cost. On failure: **all**
   partitions serialize their partial accumulators to one segment each,
   maps are cleared, memory released, and accumulation continues.
3. At emit time, partitions merge one at a time (in-memory state + their
   segments), bounding merge memory to the largest partition, and emit as
   normal batches.
4. Spilled vs unspilled results are byte-identical (tested).

Accumulators today: count (int64), sum/min/max (float64). Known limits are
tracked as GitHub issues (#3 merge accounting, #4 float sum precision).

## Formats (`engine/format/...`)

- `ndjson`: hand-rolled within-line recursive-descent parser building
  directly into batch arenas. Unescaped strings copy exactly once
  (input → arena). Differential- **and fuzz-tested against
  `encoding/json`** — both parsers must agree on accept/reject and value;
  run the fuzzer when touching it (`go test -fuzz=FuzzDifferential`).
  Strictness notes: JSON number grammar enforced (stdlib's `ParseFloat`
  alone would accept `01`, `1.`); raw invalid UTF-8 passes through on read
  and is sanitized on write (documented divergence).
- `csvf`: `encoding/csv` in `ReuseRecord` mode + per-column type hints
  (int/float/bool; empty typed cells → null).
- Writers stream; the NDJSON writer has an escape fast path and matches
  encoding/json's float formatting choices.

## The proof harness (`engine/cmd/shift-bench`)

Deterministic generator (no disk I/O in the measurement), scenarios
`transform|csv|aggregate|baseline`, and `-max-rss` which turns a run into
a pass/fail regression check — CI runs transform and aggregate this way on
every push. `baseline` is the naive buffered implementation, kept
deliberately: it quantifies what the engine saves (80×/2× at 256 MiB).

## Known follow-ups

Tracked as GitHub issues #1–#4: tokenizer is 63% of transform cost, float
parse allocates, aggregate merge accounting, float sum precision.
