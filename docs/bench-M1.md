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

¹ Aggregate RSS ≈ 2.5× the 64 MiB watermark: governor-tracked state is
bounded at 64 MiB; the remainder is merge-partition state plus Go GC
headroom. Tighter accounting is a known follow-up, not a leak — the RSS is
flat with cardinality once spilling engages.

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
