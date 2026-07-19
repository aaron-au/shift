# ADR-0012: HA scheduler — Postgres-driven, exactly-once per tick

Date: 2026-07-19
Status: Accepted

## Context

M4b's exit criterion: "schedule fires exactly once" — with any number of
hub replicas running, each of which must be able to fire schedules (no
leader election infrastructure, per doctrine: stateless services over HA
Postgres). ADR-0009 deferred the background sweep; this adds it.

## Decision

1. **The database owns correctness; the loop is just a heartbeat.**
   Every replica ticks (default 5s); each pass calls `store.FireDue`,
   one transaction with four independent layers:
   - `pg_try_advisory_xact_lock(823401)` — at most one replica per pass
     (liveness optimization only; adjacent to migration lock 823400);
   - `FOR UPDATE ... SKIP LOCKED` on due schedule rows — row-level
     exclusivity even without the advisory layer;
   - task INSERT + `next_fire_at` advance **commit atomically** — a
     crash mid-pass rolls both back and the next pass (any replica)
     retries the *same* tick;
   - idempotency key `sched:<schedule_id>:<stored tick, RFC3339Nano UTC>`
     rides the existing unique index — even operator replay or restored
     backups collapse to one task. Full stored precision (µs after the
     Postgres round-trip), not second-truncated: two distinct
     `next_fire_at` values are distinct ticks and must not collide (real
     crons are minute-aligned, but a manually-set schedule can tick
     sub-second). User-supplied keys with the `sched:` prefix are
     rejected at the API (422) to protect the namespace.
2. **Clocks never enter correctness.** Due-ness is `next_fire_at <=
   now()` evaluated in Postgres; the next tick is seeded from the DB's
   `now()`. Replica clock skew only shifts which replica notices first.
   After downtime a schedule fires once and jumps forward — no
   catch-up storm (documented policy).
3. **Cron parsing = `robfig/cron/v3`, parser only** (zero transitive
   deps; its goroutine runner machinery is unused — the DB drives
   firing). Isolated behind `ParseCron`/`NextAfter` so it is trivially
   replaceable; UTC only in M4b (no DST ambiguity; timezone column is a
   later add). An adhoc parser was rejected: the DOM/DOW union and
   range/step grammar are historically the buggy part of cron clones,
   and a mis-parse is a correctness bug in the flagship criterion.
4. **The same loop runs the periodic lease sweep** (`ReapExpired`,
   previously claim-time only) so runner crashes surface on the
   dashboard without waiting for claim traffic.
5. **Only published flow versions fire.** Version 0 resolves to
   `flows.published_version` everywhere (publish workflow, this
   milestone); a schedule on an unpublished flow parks with
   `last_error` and keeps advancing rather than wedging the pass.
   Unparseable cron disables the schedule with `last_error`.

Proof: `TestFireDueExactlyOnceUnderContention` (two stores hammering
FireDue over one DB — N schedules, exactly N tasks) and the
`TestScheduleFiresExactlyOnce` e2e (two full replicas + scheduler loops,
one killed mid-race; every tick = exactly one task).

## Consequences

- No leader election, no extra infrastructure; horizontal hub scale
  keeps working.
- A firing pass is serialized cluster-wide (advisory lock). At M4b
  scale that is a feature; if pass volume ever demands parallelism, the
  SKIP LOCKED layer already permits dropping the advisory gate.
- `robfig/cron` joins the hub's dependency tree (parser only, pinned).
