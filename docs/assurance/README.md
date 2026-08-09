# Assurance registers

A **register** records, for a class of claim SHIFT makes about itself, whether
that claim is actually *proven by something that fails when the claim breaks* —
and tracks the gaps as checked / todo / done.

A register is **not an ADR**. An ADR records a decision and its context; nothing
in a register decides anything. Registers are the audit trail *underneath* the
ADRs: an ADR asserts a property, the register says whether that assertion is
enforced or merely stated.

| Register | Question it answers |
|---|---|
| [`test-conformance.md`](test-conformance.md) | Does each ADR invariant have a test that would fail if the invariant broke? |

## How a register differs from the other docs

| Doc | Axis | Cadence |
|---|---|---|
| `docs/adr/` | What did we decide, and why | Once per decision, then immutable |
| `docs/test-plan.md` | Does each **feature** have a test + release evidence | Per release |
| `docs/assurance/*.md` | Does each **invariant** have a test that can *fail* | Per review sweep |
| `docs/reviews/` | What did an external checklist say, and what did we do | Per external review |

The overlap with `test-plan.md` is deliberate and thin: a feature can be fully
covered there (happy path, guard path, evidence captured) while the doctrine
invariant it rests on is enforced only by convention. That is the gap class a
register exists to make visible.

## Rules (these are what make a register worth keeping)

1. **DONE requires naming the test.** A row moves to ✅ only when it cites a
   test function or gate that *fails* if the invariant regresses. "Covered by
   code review", "enforced by convention", or "the code obviously does it" are
   ⬜ or 🟡 — never ✅.
2. **Checked-and-adequate is recorded too.** The point is that a later sweep
   does not re-litigate what a previous one already cleared. §3 is as load-
   bearing as the gap table.
3. **Deliberately-untested is a first-class state (➖), and carries its reason.**
   Same discipline as the comments in `coverage.thresholds`: writing a test to
   move a number rather than to establish a behaviour is the failure mode being
   avoided, and saying so out loud is cheaper than re-deciding it annually.
4. **Rows are append-only and IDs are stable.** A closed row stays, with the
   commit that closed it. Superseded rows are struck through, not deleted —
   the history of what was once unproven is the useful part.
5. **Every sweep is logged** (§1) with its date, commit, and the method used, so
   the next one is a repeat rather than a re-invention.
