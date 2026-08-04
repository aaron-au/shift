# ADR-0037: Replay and resume — restart without a payload lake

Date: 2026-08-04

Status: **Designed**

## Context

Aaron's call (2026-08-04): replay and resume are "an awesome feature missing
from just about every iPaaS", and worth a concerted push.

He is right about the gap, and the reason it exists is instructive. The
incumbents *do* ship reprocessing — Boomi has document tracking with
reprocess, webMethods has message archives — and they ship it by keeping a
**copy of every document that flows through the platform** in a central store.
That is what makes it work, and it is also why customers hate it: the vendor's
infrastructure ends up holding the customer's payload, retention is a
compliance argument, and the store becomes the platform's largest liability and
its largest bill. Aaron flagged exactly this instinct when ADR-0036 was written
— a central store where runners stage data "seems risky on data exposure."

So the feature is genuinely valuable and the standard implementation is
genuinely disqualifying under our doctrine (ADR-0016: the hub is a control
plane and payload never touches it). This ADR delivers the capability without
the lake.

### Three different properties, routinely conflated

"Restart" is three separate guarantees, and mixing them produces designs that
are simultaneously over-engineered and insufficient:

| Property | Question it answers | What it needs |
|---|---|---|
| **Chunk** | "The load died at hour five — must I redo hour one?" | Nothing new: N tasks instead of one (ADR-0036) |
| **Resume** | "The runner died mid-stream — can I continue?" | A source **cursor**: bytes of metadata |
| **Replay** | "That run was wrong — can I re-run *the same input*?" | The **input itself** |

Only the third needs payload, and only for a subset of sources. That
distinction is the whole design.

### What exists today

- **At-least-once dispatch** (ADR-0002) with idempotency keys. A lost lease
  **replays the task from the source**, and idempotent sinks dedupe the side
  effects. This is correct and stays.
- **Chunked bulk loads** (ADR-0036) — designed, not built. Splits a load into
  `job:<id>:chunk:<n>` tasks over the existing queue.
- **Cursor checkpoint-resume** (issue #13) — designed, not built, with the
  boundary constraint already recorded: resume must not be implemented by
  storing payload or by runner-to-runner state handoff.
- **Test-mode capture** (ADR-0014) — bounded, redacted, runner-only,
  ephemeral. Deliberately *not* a replay store, and must not quietly become
  one.

A grep of `runner/`, `engine/` and `pkg/` for checkpoint or resume still
returns nothing.

## Decision

**Three mechanisms, chosen by what the source can actually do.** Every flow
gets the strongest one its source supports, and the model says plainly which
one is in play rather than promising a uniform guarantee it cannot keep.

### 1. The ladder

```
partitionable source   → CHUNK   (ADR-0036)  restart granularity, no state at all
sequential + resumable → RESUME  (cursor)    metadata only, no payload
anything else          → REPLAY  (archive)   payload, in storage the CUSTOMER owns
re-readable source     → REPLAY  is free     re-run against the same source range
```

The ladder is descending in preference, not in capability: chunking is best
*because it stores nothing*. A design that reached for the payload archive
first would be the incumbent design.

### 2. Resume — a cursor is control metadata, not payload

A **resumable source** can be restarted from an opaque position: a paginated
API's page token, an object byte offset, a CDC LSN, a Kafka offset, a
`WHERE id > ?` high-water mark. That position is bytes of metadata and is
therefore allowed on the hub, exactly as issue #13 scoped it.

**SDK contract** — additive, so every existing connector keeps working:

- `PullRequest.resume_from` (bytes, opaque to the runner and the hub) — the
  position to restart at; absent means "from the beginning".
- `Frame.checkpoint` (bytes, opaque) — a position the connector asserts is
  safe to resume from, riding alongside a batch.

On the author side, a source action opts in by implementing an optional
interface; `SourceAction` itself is unchanged:

```go
// ResumableSource is an optional SourceAction capability. A connector that
// implements it can restart a stream from a position it previously emitted.
type ResumableSource interface {
    // Resume positions the stream at cur before the first Next. A nil or
    // empty cursor means "from the beginning" — identical to not resuming.
    Resume(ctx context.Context, cur []byte) error
    // Checkpoint returns a position that is safe to resume from given that
    // every batch returned so far has been fully processed downstream. It
    // returns nil when no safe position exists yet.
    Checkpoint() []byte
}
```

**Where a checkpoint is safe.** Only after the terminal sink has confirmed the
records it covers. A cursor recorded when the *source* produced a batch would,
on resume, skip records that were read but never written — silent data loss,
which is strictly worse than the duplicate work resume exists to avoid. The
runner therefore advances the durable cursor from the **sink-confirmed** count,
never from source progress, and a flow whose sink cannot confirm does not
resume: it replays.

**Persistence.** A `checkpoint` column on the hub task row, written on
heartbeat (the control-plane call that already exists) and read on
re-dispatch. Bytes of opaque metadata; the hub never interprets it.

**Semantics stay at-least-once, not exactly-once.** Resume narrows the
duplicate window from "the whole task" to "since the last checkpoint". Sink
idempotency keys remain the mechanism that makes duplicates safe (ADR-0002),
and nothing here weakens that. Claiming otherwise would be the ADR lying about
its own guarantee.

**Non-resumable sources are fine.** A webhook body, a non-seekable stream, a
one-shot command — these replay rather than resume, which is the behaviour
today. The model reports which mode a flow is in so an operator is never
guessing.

### 3. Replay — the archive is the customer's, not ours

Replay re-runs a past execution against **its original input**. For most
integrations no storage is needed at all, and that is the first thing the
design should exploit:

- **Re-readable source** (an SFTP file, an S3 object, a DB query over a stable
  range, a paginated API with a recorded range): replay is a **new task with
  the same source configuration and range**. Nothing was stored, because the
  data is still where it came from. This covers the large majority of real
  flows and it is free.
- **Ephemeral push input** (`@webhook` body, a synchronous `POST /api/flows/run`
  body): the payload existed only for the duration of one request. Replay is
  impossible unless it was captured.

For that second class only, and **opt-in per flow**, SHIFT writes the inbound
payload to a **replay archive**.

**The archive is storage the customer configures and owns** — their S3 bucket,
their Azure container, their mounted path, their SFTP target. Not a
SHIFT-managed store and not the hub. It must be reachable by every runner that
may execute the flow, since the runner that wrote it is usually gone by the
time anyone replays; §5a covers where it lives and why a shared mount is a
legitimate choice rather than a doctrine violation.

- Written **runner-side**, by the same connector stack as any sink, so payload
  never crosses the control plane (ADR-0016 holds unchanged).
- **Envelope-encrypted** with a per-archive DEK wrapped by the account's KEK
  (ADR-0010, reused as-is), so an object read directly out of the bucket is
  ciphertext.
- The hub stores a **reference only**: archive URI, object key, size, digest,
  content type, retention deadline. Metadata, in the task row, alongside the
  cursor.
- **Retention is a policy on the flow**, enforced by the runner at write time
  (the object carries its expiry) and by the customer's own lifecycle rules.
  SHIFT does not become the retention authority for data it does not host.
- On-prem stays on-prem: a self-hosted deployment points the archive at local
  storage and no payload ever leaves the customer's network. This is the
  hub-and-spoke differentiator applied to the one feature that usually
  destroys it.

**Replay is a first-class operation, not a resubmit.** `POST
/api/flows/{flow}/replay/{task_id}` creates a **new task** that references the
original's archive object or source range, records `replay_of: <task_id>`, and
runs under a **fresh idempotency key derived from the replay task id** — not
the original's.

That last point is load-bearing and easy to get backwards. Reusing the original
key would make replay a no-op at every idempotent sink: the sink would
correctly recognise the key it has already applied and discard the work. An
operator asking for a replay is asking for the side effects to happen *again*,
having decided the first outcome was wrong. Replay is therefore **deliberately
not deduped**, while automatic at-least-once re-dispatch **is** — same
machinery, opposite intent, distinguished by which key is injected.

### 4. What replay does and does not guarantee

Stated plainly, because the honest limits are what make the feature trustworthy:

- **Same input, not same output.** Replay re-runs the flow *as it exists now*.
  If the flow was edited since, the replay uses the new version — which is
  usually the point (you are replaying *because* you fixed something). The
  task records both `replay_of` and the flow version actually used.
- **Re-readable sources may have moved.** Replaying a `WHERE updated_at > x`
  query returns today's rows, not that day's. The model marks such a replay
  **re-execute** rather than **replay**, and says so in the API response.
  Silently returning different data under the word "replay" would be the worst
  possible failure.
- **Downstream systems have moved on.** Replaying an order into an ERP that
  already processed it is the operator's call, which is exactly why replay
  bypasses idempotency rather than pretending it is safe.
- **No cross-branch rollback.** Unchanged from ADR-0031 §5: replay re-runs a
  flow, it does not undo the previous run's side effects.

### 5. Runner replacement is the normal case, not the exception

Aaron, 2026-08-04: "Replay and resume would need to function even if the runner
is replaced."

That is the correct requirement and it is the load-bearing one, because runners
are disposable by design (ADR-0002) — the runner that started a long load is
*more* likely than not to be gone by the time it matters. A resume that only
worked on the same process would be a feature that fails exactly when it is
needed. Both mechanisms are therefore **runner-agnostic by construction**:

- **Resume** already is. The cursor lives on the hub task row and travels back
  out with the lease, so whichever runner claims the re-dispatched task
  receives it. No runner-local state, no handoff.
- **The cursor pins the connector that produced it.** A cursor is opaque bytes
  only its producer understands, and the replacement runner may hold a
  different connector build. So the stored cursor carries `connector` and
  `version` alongside it, and a mismatch **refuses to resume** and falls back
  to a full replay. Silently handing a v0.3 cursor to v0.4 risks it parsing
  into a different position, which resumes at the wrong place with no way to
  notice. (This settles an open question the first draft left open.)
- **Replay** requires the archive to be readable by **any runner that may
  execute the flow** — in practice, every runner in the target group
  (ADR-0030).

### 5a. The archive is a destination, not new infrastructure

An earlier draft of this section framed the archive as needing "shared
storage", which was the wrong framing and made the feature sound like it
demanded infrastructure nobody has. It does not. **The archive is an ordinary
sink destination**, written through the same connector stack as any other sink,
and every customer already has at least one: an S3 bucket, an Azure container,
a database, an SFTP target. Nothing is provisioned for SHIFT's benefit.

The only requirement is a property, not a medium: **every runner eligible to
run the flow must be able to reach the destination.** That is already true of
every sink a flow writes to — a flow whose SFTP sink only one runner could
reach would be broken today, for the same reason.

The archive is therefore addressed by a **URI**, and the deployment picks
whatever it already runs:

```
s3://bucket/prefix          object storage (also azure://, gs://)
sftp://host/path            an existing file transfer target
file:///mnt/shift/archive   a mounted path — NFS/SMB, or a k8s PVC
```

`file://` on a shared mount is offered as a **convenience for deployments that
already have one**, not as the design centre. It is the degenerate case, and a
customer with no shared mount loses nothing: they point at the object store or
database they already run.

**Even as a convenience, it sits near a locked doctrine and needs saying out
loud.** CLAUDE.md states: *no shared filesystems for runner clustering.* That
rule is about **coordination** — it exists because v0 tried to coordinate
distributed work through per-runner private state, and because a shared
filesystem used as a work queue is a lock-contention and split-brain generator.
It is not "a runner may never read a path".

A `file://` archive stays on the right side of that line only if it is used as
a **content store addressed by explicit reference**, never as a coordination
medium. Concretely, the following are forbidden and are what would turn this
into the thing the doctrine bans:

- **No discovery by listing.** A runner never enumerates the archive to find
  work. It reads exactly the object the hub named in the task. The hub remains
  the only place that knows what exists.
- **No locking, no leases, no rendezvous through the store.** Leases live in
  Postgres (ADR-0002). A runner never waits on a file, and never takes a lock
  on one.
- **No runner-to-runner handoff.** An archive object is written by one task and
  read by a *later, separately dispatched* task. It is never a channel between
  two live runners, and never partial in-flight state.
- **Keys are derived, not negotiated.** An object key is a pure function of
  the task id, so two runners never race to name the same object and never need
  to agree on one.

Under those constraints the store is exactly as "shared" as an S3 bucket is,
and the coordination plane is unchanged: Postgres, leases, `SKIP LOCKED`.

**Not the hub.** The obvious-looking alternative — park the archive on the hub,
since a self-hosted hub is the customer's own machine anyway — is rejected, and
not on data-custody grounds. It is rejected because payload storage is the
single thing most likely to stop the hub being lightweight. A hub holding
payload needs blob storage or a bloated Postgres, its backup/restore and HA
failover start moving bytes instead of rows, and its disk becomes a capacity
planning problem that scales with customer data rather than with customer
count. The hub is a control plane because control planes are small; an archive
is the opposite shape.

### 5b. Configured as a connection, and off by default

The archive is **feature-gated and disabled by default** — no archive
configured means no payload is ever written anywhere, which is today's
behaviour and the safe default for anyone who has not asked for this.

Enabling it is one setting: an archive **connection** (ADR-0034). That is not a
new configuration concept — it is the existing reusable, account-scoped
connector configuration, which means the archive gets, for free:

- credentials as `{"$secret":…}` references, resolved runner-side per task and
  never at rest on the runner (ADR-0010, ADR-0035);
- one definition reused across every flow that opts in, rather than a path
  restated per flow;
- the existing hub CRUD, validation and deploy-time reference checks.

A flow opts in with `replay: {archive: "<connection name>", retain: "30d"}`.
Absent that key, nothing is captured. An operator may set a deployment-wide
default archive so "every flow is replayable" is a policy choice, mirroring the
hub-wide dead-letter toggle in ADR-0031 §2 — and, like that one, it is an
operator decision rather than an engine default.

### 6. Interaction with the existing model

- **Chunking (ADR-0036) composes with both.** A chunk is an ordinary task, so a
  chunk resumes from its own cursor and replays from its own archive reference
  or key range. A job's progress is the count of its completed chunks — already
  durable, already in the queue.
- **Idempotency keys.** `sched:` and `job:` are reserved prefixes (ADR-0012,
  ADR-0036); this adds **`replay:`**. A replay task's key is
  `replay:<replay_task_id>`, distinct by construction from the original's, which
  is what makes replay re-apply.
- **Capture (ADR-0014) stays what it is.** Bounded, redacted, ephemeral,
  runner-only, for test mode. It is *not* the replay archive and must not be
  repurposed into one — its samples are truncated and redacted, so replaying
  from them would silently run against mutilated input.
- **Secrets never archived.** The archive holds the inbound payload. Resolved
  secret values are already redacted from anything leaving the runner
  (ADR-0010); the archive path reuses that redactor rather than adding a second
  one to keep in sync.

## Doctrine held

- **Hub never touches payload (ADR-0016).** Cursors and archive references are
  metadata. The archive is written runner-side to customer-owned storage. The
  hub gains two opaque columns and one route; it gains no data plane.
- **Runners stay stateless and disposable (ADR-0002).** Neither resume nor
  replay stores anything on the runner between tasks, and both survive the
  runner being replaced — a cursor lives on the hub task row, an archive object
  lives in customer storage every eligible runner can reach. No
  runner-to-runner handoff, no state carried in a process.
- **No shared filesystem for CLUSTERING (CLAUDE.md).** A `file://` archive on a
  shared mount is a content store addressed by explicit reference, not a
  coordination medium: no listing to discover work, no locks, no rendezvous,
  keys derived from the task id. Leases stay in Postgres. §5a states the
  constraints that keep this true, because it is the one place this ADR comes
  close to a locked rule.
- **At-least-once is unchanged.** Resume narrows the duplicate window; it does
  not promise exactly-once. Sink idempotency remains the safety mechanism.
- **Encrypted by default (ADR-0010).** Archive objects are envelope-encrypted
  with the account's KEK, reusing the existing secrets machinery rather than a
  parallel scheme.
- **Never silently dropped / never silently different (ADR-0005).** A source
  that cannot resume says so; a replay that cannot reproduce its input is
  reported as a re-execute, not passed off as a replay.

## Consequences

- SHIFT gains the property the category is weakest at, and gains it **without
  operating a payload lake** — which is simultaneously the compliance argument,
  the cost argument, and the on-prem argument. "Your data never lands in our
  infrastructure, and you can still replay it" is a sentence no incumbent can
  say.
- The connector SDK grows an **optional** capability. Existing connectors are
  unaffected; the two proto fields are additive and absent-by-default.
- The hub grows a `checkpoint` column, an `archive_ref` column, a `replay_of`
  column and a replay route. No payload, no new plane.
- Resumability becomes a **connector quality axis** — a visible reason to
  prefer a first-party connector, and a natural item in the ADR-0025 Discover
  descriptor.
- Build order is the ladder itself, cheapest and most broadly useful first:
  **resume cursor** (SDK + engine + runner + hub column) → **chunked loads**
  (ADR-0036 planner) → **replay archive** (archive sink + replay route). Each
  is independently shippable and independently valuable.

## Open questions

- **Checkpoint cadence.** Every batch is too chatty for the heartbeat; every N
  batches or every T seconds needs a default that does not make a cheap flow
  pay for a feature it will never use.
- **Which connectors get resumability first.** `db` (keyset pagination), `s3`
  and `sftp` (byte offset), `http` (recorded page token) are the obvious
  candidates; `fs` and `ftp` follow. Ordering should follow measured customer
  shapes, not guesses.
- **Archive format.** Raw inbound bytes are the most faithful to "the same
  input" but lose the parse; NDJSON of the parsed records is easier to inspect
  but is already one transformation away from what arrived. Faithfulness
  probably wins, but it should be a decision, not a default.
- **Replay of a chunked job.** Replaying one chunk is well-defined; replaying a
  whole job means re-planning, and the plan may enumerate differently against
  moved data. Likely a job-level `re-execute` rather than a true replay.
- **Who may replay.** Replay deliberately re-applies side effects, so it is a
  higher-privilege action than submitting a flow. This wants a real permission,
  which lands on the deferred RBAC work (issue #16).
