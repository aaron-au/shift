# M1 Benchmark Results — Streaming Engine Exit Criteria

Run 2026-07-19 on Apple M4 Max (darwin/arm64), Go 1.26.2, single pipeline
(single core — parallelism arrives with the runner, ADR-0005). Reproduce
with `engine/cmd/shift-bench`; the generator is deterministic.

## Exit criteria (ADR-0003) — all met

> Transform a 1 GB stream with peak RSS bounded by the watermark
> (~100 MB), zero disk writes below the watermark, honest metrics.

| Scenario (1 GiB input) | Peak RSS | Wall | Throughput | Spill |
|---|---|---|---|---|
| `transform` (NDJSON→flatten→filter→project→NDJSON) | **24.3 MiB** | 16.6 s | 62 MiB/s · 294k rec/s | 0 B |
| `csv` (typed CSV→filter→NDJSON, 13M records) | **13.5 MiB** | 22.0 s | 47 MiB/s · 591k rec/s | 0 B |
| `aggregate` (group-by, 1M groups, 64 MiB watermark) | **164 MiB**¹ | 16.4 s | 62 MiB/s · 298k rec/s | 337 MiB (single scratch file) |

¹ **This footnote was wrong and is corrected below** ("Small runners").
Aggregate RSS does not track the watermark and is *not* flat with
cardinality — it scales with distinct keys.

## Streaming vs naive buffered baseline (256 MiB input)

The baseline is the same workload implemented the way the incumbents' data
models behave: decode everything to `map[string]any`, transform, marshal.

| | Streaming engine | Buffered baseline | Factor |
|---|---|---|---|
| Peak RSS | 22.9 MiB | **1.83 GiB** | **80× less memory** |
| Wall time | 2.55 s | 5.10 s | 2× faster |
| Heap allocated/record | 74 B | 1,743 B | 24× |
| Allocs/record | 7.4 | 62.4 | 8× |

Streaming RSS is **flat with input size** (22–24 MiB at 64 MiB, 256 MiB, and
1 GiB inputs); the baseline grows linearly at ~7× input size, which is the
scaling wall the platform exists to remove.

## 10 GiB — ETL-sized loads (added 2026-08-04)

Three points make a curve, not a law, so the exit-criteria run was repeated
at 10× the data to see whether "flat with input size" survives an order of
magnitude. Same machine, same binary, `-max-rss 100MiB` enforced:

| Scenario (10 GiB input) | Peak RSS | RSS @ 1 GiB | Wall | Throughput | Spill |
|---|---|---|---|---|---|
| `transform` (48.5M records) | **24.30 MiB** | 24.3 MiB | 63.9 s | 160 MiB/s · 758k rec/s | 0 B |
| `csv` (127.0M records) | **12.97 MiB** | 13.5 MiB | 88.3 s | 116 MiB/s · 1.44M rec/s | 0 B |

**RSS is identical at 10× the input** — 24.30 vs 24.3 MiB; the CSV case is
marginally *lower*. Zero spill in both. Memory is a function of the
watermark and batch sizing, not of how much data flows through, which is
the property that makes ETL-sized loads a scheduling question rather than a
sizing question.

Do **not** compare the throughput columns against the 1 GiB table above:
those were measured in a different session and the machine state differs
(see `docs/dev/04-runner.md` on between-session drift). RSS is the
comparable figure — it is set by configuration, not by CPU.

**Reading dominates.** The per-op breakdown says where the time goes:

| | `transform` | `csv` |
|---|---|---|
| read (parse) | 40.2 s (63%) | 68.8 s (78%) |
| all transforms | 19.4 s | 1.9 s |
| write | 4.3 s | 17.6 s |

Two thirds to three quarters of wall time is parsing input. Transform depth
is close to free by comparison — the same conclusion the trigger-path shape
sweep reaches from the other direction. For batch sizing, the source is the
constraint.

**Not yet proven at this scale:** multi-hour sustained runs, multi-TB
volumes, and real warehouse targets. And there is no checkpoint/resume
anywhere in the tree — a load that fails part-way restarts from zero, which
is the actual gap between "handles ETL volumes" and "is an ETL tool".

## Small runners: what actually bounds memory (added 2026-08-04)

Can a customer run on a 1 GiB box? For streaming flows, comfortably — and
for aggregates, the answer is set by **cardinality, not configuration**,
which is not what the M1 footnote claimed.

Squeezing the watermark 32× barely moves RSS (1 GiB input, 1M groups):

| Watermark | Peak RSS | Spill | Wall |
|---|---|---|---|
| 64 MiB | 177.4 MiB | 337 MiB | 7.27 s |
| 8 MiB | 163.2 MiB | 399 MiB | 7.10 s |
| 2 MiB | 153.2 MiB | 408 MiB | 7.05 s |

So ~150 MiB is not governor-tracked at all. Holding the watermark at 2 MiB
and varying distinct keys shows what it actually is:

| Groups | Peak RSS | Spill | Wall |
|---|---|---|---|
| 1,000,000 | 154.4 MiB | 408 MiB | 7.53 s |
| 100,000 | 33.0 MiB | 387 MiB | 6.58 s |
| 10,000 | 22.2 MiB | 112 MiB | 5.24 s |
| 1,000 | 17.7 MiB | 0 B | 4.78 s |

**Sizing rule: aggregate RSS ≈ 18 MiB + ~150 B per distinct key.**

Spilling *appears* close to free — the 2 MiB watermark spills 20% more than
64 MiB and runs marginally *faster*. **Do not generalise that.** These runs
are on an Apple M4 Max's local NVMe, and ~400 MiB of sequential writes on a
machine with plenty of free RAM is very likely absorbed by the page cache
and never fsynced before the process exits. The figure may be measuring
memory, not storage.

The trade is genuinely good where scratch is local NVMe. It is unproven on
throttled cloud block storage, a container overlay filesystem, or anything
network-attached — precisely the environments a memory-constrained runner
tends to live in. Re-measure with `-spill-dir` on the target volume before
sizing a small runner around spilling, and treat "spill instead of RAM" as
a hypothesis until then.

Streaming-only flows (filter/project/flatten/map) never touch the governor
and stay at ~24 MiB regardless of input size or runner size.

### Storage: what actually needs a fast disk

The intuitive split is "real-time needs no disk, batch needs NVMe". That is
the wrong axis, and following it would over-provision most deployments while
under-provisioning the one case that matters.

Two independent questions decide it.

**1. Does the flow spill at all?** Only *blocking* operators (aggregate,
join) hold state; streaming operators (filter, project, map, flatten,
coerce) hold none. A streaming-only flow writes **zero bytes of scratch at
any volume** — the 10 GiB transform above spilled 0 B, same as the 1 GiB
run. Volume is irrelevant; operator shape decides. A blocking operator whose
state fits under the watermark also never spills (1,000 groups: 0 B).

**2. If it spills, does the spill exceed free RAM?** The spill store
(`engine/spill`) writes through a 256 KiB `bufio.Writer` and **never
fsyncs** — deliberately, since durability is meaningless for scratch. The
file is also `os.Remove`d the instant it is created, so it exists only via
its fd. Spill that fits in free page cache may therefore never reach the
platter at all: the extent is released when the fd closes.

| Workload | Spills? | Storage needs |
|---|---|---|
| Any volume, streaming operators only | Never | Irrelevant — HDD is fine |
| Blocking operator, state under watermark | Never | Irrelevant |
| Blocking operator, spill fits free RAM | To page cache | Modest — HDD likely fine |
| Blocking operator, spill exceeds free RAM | To the device | NVMe/SSD earns its cost |

So the recommendation is per **flow shape**, not per workload type. A
real-time webhook flow doing high-cardinality dedup spills; a nightly
filter-and-load of 500 GB does not. Most integration work — map, filter,
route, load — never touches scratch at any size.

Note the interaction with chunked loads (ADR-0036): chunking reduces
per-task state, so it can move a job from the bottom row to the top one.
Splitting a load is a storage-sizing lever as well as a restartability one.

Only the last row is unverified — see the page-cache caveat above. Whether
it spills is measured directly (0 B vs 408 MiB); what spilling *costs* on
non-NVMe storage is not.

### The admission-accounting gap (risk, not yet fixed)

Admission (ADR-0005) charges every task `TaskWatermark + TaskOverhead`
(80 MiB by default) and the governor tracks only the watermarked portion. A
1M-group aggregate really costs ~154 MiB, of which the governor sees 2 MiB
at a 2 MiB watermark. **Admission therefore under-charges high-cardinality
aggregates**, and a small runner can admit several of them and exceed the
memory it was told to respect.

Practical guidance until accounting improves: on a memory-constrained
runner set `-mem-budget` to roughly half of physical RAM, and size
high-cardinality aggregate work against the rule above rather than against
the watermark. `max_concurrent_by_mem` in the capacity report is derived
from the 80 MiB figure and is optimistic for this shape.

## Micro-benchmarks (`go test -bench`)

| | Result |
|---|---|
| Record build (nested, 9 values) | 259 ns, **0 allocs** steady-state |
| Compiled path get (`$.addr.city`) | 42 ns, 0 allocs |
| NDJSON parse vs `encoding/json` | 1.8× faster, ~0 vs 44 allocs/record |

## Known follow-ups
- Float parsing allocates (`strconv.ParseFloat` needs a string); ~1 alloc/record on float-heavy schemas.
- NDJSON read dominates pipeline cost (63%); tokenizer SIMD-style batching is the next lever.
- Aggregate merge-phase memory should reserve against the governor with partition-size feedback.
- Sum accumulates in float64 (precision on large int sums); decimal/int128 accumulation later.
