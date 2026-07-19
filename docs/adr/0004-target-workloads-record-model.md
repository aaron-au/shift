# ADR-0004: Target workloads and the record data model

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
The streaming engine's record representation determines what the platform handles well forever. Aaron selected **all four** first-class workloads: JSON APIs (REST/webhooks), flat files (CSV/fixed-width), XML/EDI (EDIFACT/X12 — the webMethods-replacement market), and database sync/CDC.

## Decision
- **Records are hierarchical, not tabular.** The internal model is a typed, nested record (scalars, lists, maps with typed fields) — flat/tabular data is the degenerate case, so CSV and DB rows fit naturally while JSON/XML/EDI keep their structure. Purely columnar/tabular models (Arrow-first) were rejected as a primary representation because EDI/XML hierarchies would fight it; columnar encodings can be added later as an optimization for tabular segments.
- **No `map[string]interface{}`.** Records use an internal value representation designed for pooling and low allocation (shredded fields / arena-backed values — exact design in M1). v0 lesson: reflection-heavy generic maps on the hot path.
- **Batches are the unit of flow.** Streams move batches of records (size-bounded by count and bytes) through pull-based iterators; batch boundaries are where backpressure, metrics, checkpointing, and spill decisions happen.
- **Parsers are streaming from byte one:** JSON/NDJSON tokenizer, CSV/fixed-width reader, SAX-style XML, EDI segment reader, DB cursor pagination — all produce record batches incrementally; none require the document/result-set in memory.
- **Schema is carried, not assumed:** batches reference a schema (possibly inferred, possibly declared) so transforms can compile field paths once per stream instead of resolving strings per record. Connector SDK exposes schema negotiation.
- **CDC/DB sync implications:** the SDK includes cursor/offset state as a first-class concept (persisted via the hub, since runners are stateless — ADR-0002).

## Consequences
- M1 order of implementation: JSON/NDJSON → CSV/fixed-width → XML/EDI → DB cursors (EDI correctness is deep domain work; schedule accordingly).
- The gRPC connector protocol (ADR-0001) serializes record batches — wire encoding must be settled in M1/M2 boundary (candidate: length-prefixed batch frames with a compact binary encoding; decision recorded when made).
