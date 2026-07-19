# ADR-0003: Milestone 1 is the streaming engine + benchmark harness

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
SHIFT's core differentiator is transformation performance: memory-efficient, disk-light, streaming. The v0 prototype deferred this and ended up with a fully-buffered engine that contradicted the product thesis and could not be retrofitted. The riskiest, most load-bearing design in the platform is the inter-step data contract — every connector, transform, and SDK surface conforms to it.

## Decision
Build and **prove** the streaming engine before any distributed machinery:

**M1 deliverables:**
1. `engine/` module: the record/batch data model (ADR-0004), pull-based streaming pipeline (source → transforms → sink), pooled buffer management, spill-over-watermark to a single scratch store.
2. Streaming format readers/writers for the four target workloads (JSON/NDJSON tokenizer first, then CSV/fixed-width; XML/EDI and DB cursors may land as M1.5).
3. Core transform primitives: map/rename/project, filter, type coercion, flatten/nest, simple aggregate.
4. **`shift-bench`**: a benchmark harness measuring records/sec, bytes allocated per record, and peak RSS across payload sizes, with a naive-buffered baseline for comparison. Run in CI to catch regressions.

**M1 exit criteria (the thesis, falsifiable):**
- Transform a 1 GB JSON/NDJSON or CSV stream with peak RSS under ~100 MB (bounded by watermark config, not payload size).
- Zero disk writes below the memory watermark; above it, spill goes to one embedded scratch store (never many small files).
- Honest metrics plumbed from the start: per-step CPU time, allocations, records in/out.

## Consequences
- Nothing user-visible ships in M1; the payoff is that M2+ (connector SDK, runner, hub) build on a proven contract instead of designing it under delivery pressure — the exact failure mode of v0.
- Benchmark results become both the engineering guardrail and eventual marketing collateral vs disk-bound incumbents.
