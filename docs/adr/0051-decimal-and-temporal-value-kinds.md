# ADR-0051: Exact decimals and native temporal values, paid for by padding

Date: 2026-08-08

Status: **Accepted; building now.** Extends ADR-0004 (hierarchical typed record
model) and closes issue #4 (`AggSum` accumulates in `float64`). Precedes
`engine/format/fixedw`, which is the first format that needs these kinds to
exist.

## Context

`record.Value` has eight kinds, and money is not one of them. A monetary amount
arriving from any of our first-class workloads lands as `KindFloat`:

- JSON `{"amount": 10.10}` — the ndjson tokenizer parses it with
  `strconv.ParseFloat`.
- CSV with a `TypeFloat` column hint — same.
- A Postgres `NUMERIC(12,2)` — `database/sql` hands us a `float64` and
  `dbconn/rows.go` calls `bld.Float`.

`10.10` is not representable in binary floating point. Neither is `0.1`. The
platform's job is to move a value from one system to another *without changing
it*, and today a 2-decimal currency amount round-trips through the engine as
`10.099999999999999645`. Nothing warns, because nothing is wrong from
`float64`'s point of view. It re-renders as `10.1` most of the time, which is
what makes it dangerous: the defect is invisible until a sum over a few hundred
thousand rows disagrees with the source system by cents, and then it is a
reconciliation investigation rather than a bug report.

Temporal data has the same shape of problem for a different reason. `dbconn`
maps every `time.Time` to
`bld.StringLiteral(t.UTC().Format(time.RFC3339Nano))`, so a `TIMESTAMPTZ`
becomes a **string** the moment it enters the engine. The consequences are
visible in our own code: `sync.go` carries a `cursorArg` helper whose entire
purpose is parsing that string back into a `time.Time` so it can be bound as a
query parameter. We stringify a value and then pay to un-stringify it, and any
comparison in between is lexical rather than chronological.

Fixed-width files — the next format to build, and the last item on ADR-0004's
first-class list — are where both problems are unavoidable rather than merely
present. A zoned decimal (`0001010{` = `+101.0`) and a packed date (`20260808`)
are the native vocabulary of the format. Without exact types, `fixedw` would
have to emit stringly-typed placeholders, and every flow reading one would need
a coerce step to undo the loss the reader had just introduced.

## Decision

### 1. The aux byte is free, so exactness costs no memory

`record.Value` is:

```go
type Value struct {
    kind Kind      // uint8
    num  uint64
    str  []byte
    kids []Value
    keys [][]byte
}
```

`kind` is one byte followed by seven bytes of **padding**, because `num` aligns
to 8. `unsafe.Sizeof(Value{})` is 88 with and without a second `int8` field —
measured, not assumed, and a test now asserts it so a future field addition
that *does* grow the struct fails loudly rather than quietly costing memory on
every value in every batch.

That free byte is what makes exact decimals possible without breaking the
zero-alloc steady state. Four new kinds, all inline scalars — no arena, no
allocation, no pointer chasing:

| Kind | `num` | `aux` |
|---|---|---|
| `KindDecimal` | coefficient (int64) | scale — digits after the point |
| `KindTimestamp` | unix nanoseconds | zone offset, 15-minute units |
| `KindDate` | days since the Unix epoch | unused |
| `KindTime` | nanoseconds since midnight | unused |

A decimal's value is exactly `coefficient × 10^-scale`. Scale is signed: a
negative scale means multiples of a power of ten, which is how "amount in
thousands" fields in fixed-width and EDI declare themselves.

`KindTime` is a time of day with no date, because that is what a bare `HHMMSS`
field in a fixed-width or EDI record actually is. Modelling it as a timestamp
on some arbitrary day would invent information the source did not provide.

### 2. Zone offset is a display fact; the instant is always exact

`KindTimestamp` stores the instant as unix nanoseconds — never a wall-clock
reading plus a zone — so two timestamps compare correctly regardless of where
they came from. The `aux` byte carries the original zone offset in 15-minute
units so that a value read as `+10:00` renders back as `+10:00` rather than
being silently normalised to UTC.

±127 units covers ±31.75 hours, which is every zone that exists. Offsets that
are not a multiple of 15 minutes (historical LMT offsets, e.g. Liberia's
−00:44:30 before 1972) are **rounded to the nearest 15 minutes for display
only**; the instant is unaffected. This is stated because it is a real, if
small, loss, and a reader should find it here rather than discover it.

### 3. An `int64` coefficient overflows, so `SUM` accumulates in 128 bits

A coefficient overflows around 9.2×10¹⁸ — at scale 2, about 9.2×10¹⁶ currency
units. Ample for any single value; **not** ample for a `SUM` over a large
batch, which is precisely the operation a finance-shaped flow performs.

So exact aggregation accumulates in 128 bits (`math/bits.Add64`/`Mul64`) and
reports overflow as an error rather than wrapping. This is the same defect as
issue #4, where `AggSum` accumulates in `float64` and loses precision on large
integer sums, so the two are fixed together: the aggregate keeps an exact
accumulator for `KindInt` and `KindDecimal` inputs and the existing `float64`
path only for `KindFloat`.

A sum that cannot be represented is an error. It is not a saturated value and
not a silent wrap, because both of those are indistinguishable from a correct
total downstream.

### 4. Exact kinds compare exactly

`EqualScalar` and every ordering comparison compare `KindInt` and `KindDecimal`
**without going through `float64`** — scales are aligned in 128-bit space, so
`0.1` equals `0.1` and no comparison result depends on binary rounding.

Cross-comparison with `KindFloat` still goes through `float64` and is
documented as inexact. The alternative — refusing to compare a decimal with a
float — would fail flows for a reason their author cannot act on.

### 5. JSON keeps its current behaviour; declared types opt in

JSON has no decimal type. A bare `10.10` in an NDJSON payload therefore stays
`KindFloat`, exactly as today. Only a **declared** type produces a decimal: a
CSV column hint, a fixed-width column spec, a database `NUMERIC` column, or an
explicit `coerce` step.

This is deliberate, and the reasoning is worth keeping: silently promoting
every fractional JSON number to a decimal would change the output of every
existing flow that reads JSON and writes JSON, in a release whose headline is
"we made numbers more correct". Correctness improvements that arrive
unannounced are indistinguishable from regressions. The opt-in is one config
field, and the flows that care about exactness are the ones that already
declare their types.

### 6. The codec appends tags, and the connector protocol carries the version

`engine/spill`'s codec tags values from an `iota` block. For the spill store
itself, versioning is genuinely unnecessary: it is a single scratch file
**unlinked immediately on creation**, so it never outlives its process and no
reader can meet a file written by a different build.

**But this codec is not only the spill format.** `sdk/frame.go` and
`sdk/host/adapters.go` already use it as the **connector wire framing**
(ADR-0007), so two independently built *processes* do have to agree on tag
numbers — and they can genuinely disagree, because a signed connector version
stays resolvable and runnable for as long as a flow pins it (ADR-0047). A
current runner pushing to a connector published before this ADR is an ordinary
situation, not a corner case.

Two properties make that safe:

- **Appending is the only change.** Existing tag numbers and their payloads are
  untouched, so a frame containing no new kinds is byte-identical to what
  previous builds produced (asserted by a test). Renumbering a tag, or changing
  an existing tag's payload, would not be safe and is not on the table.
- **The mismatch is negotiated, not discovered.** `sdk.ProtocolVersion` goes to
  **2**, and a host offers `{1, 2}` at handshake so older connectors still
  attach and report 1. When a sink's connector negotiated 1, the host encodes
  with a restricted encoder that **refuses** the new kinds and says why:
  *"cannot send a decimal value over connector protocol 1 — the connector was
  built before exact decimal and temporal kinds existed (rebuild it against the
  current SDK, or coerce the field to a string first)."*

Without that gate the failure would still be closed rather than silent — an
unknown tag is an error, not a misparse — but it would surface as
`spill: unknown tag 9` from inside a subprocess, which tells an operator
nothing about what to do. Version 1 stays on the offered list deliberately:
dropping it would retire every older signed build at once.

## Consequences

**Good.** A monetary amount survives a round trip. Database timestamps stop
being strings, which deletes `sync.go`'s `cursorArg` and makes cursor
comparison chronological. `SUM` becomes exact for integers and decimals, closing
issue #4. `fixedw` can emit zoned decimals and packed dates as the types they
are. All of it fits in memory that was previously padding.

**Costs, honestly.** Four kinds means every `switch` over `Kind` grows: the
codec, `coerce`, `Filter`, the aggregate, four formats, `engine/schema`, and
three connectors. A switch that silently falls through to a default is the
failure mode to watch for, so the default arms return errors rather than
guessing — a format that cannot represent a decimal must say so, not write a
float. And two numeric towers now coexist: exact (`int`, `decimal`) and
inexact (`float`). Mixing them is legal and documented as lossy, which means
"why is this a float?" becomes a question flow authors can ask.

The aggregate's per-group state also grows: an accumulator now holds a 128-bit
sum and a `record.Value` extreme rather than three `float64`s, so `groupCost`
rises from 48 to 144 bytes per aggregate. A bounded-memory aggregate spills
sooner rather than exceeding its watermark, which is the correct direction for
that trade, but it is a real change in when spilling starts.

The protocol bump is the other ongoing cost: every connector rebuild is now
also a protocol-version decision, and the offered-version list only ever grows
until someone decides an old version may finally be dropped.

**Not in scope.** Arbitrary-precision decimals (an `int64` coefficient is
enough for money and measurement; a bignum needs the arena and breaks the
zero-alloc steady state), interval/duration kinds, and time zones as *names*
rather than offsets — a zone name cannot be stored inline and is a property of
a calendar, not an instant.
