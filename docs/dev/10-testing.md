# 10 — Testing: what we learned the hard way

This is the practice guide. It is not a register (`docs/assurance/` tracks
whether invariants are enforced) and not an ADR (nothing here decides anything).
It records **how to write tests that can actually fail**, and **what to check
next**, drawn from three review sweeps that found 19 real bugs.

Read `05-conventions.md` first for the gate and the make targets. This document
is about the part a gate cannot enforce: whether the tests mean anything.

---

## 0. The one-line version

**A green test proves nothing until you know what turns it red.**

Everything below is a corollary.

---

## 1. Non-vacuity is not optional

Twice in one sweep, a test written *specifically* to prove a property passed
while proving nothing:

- `FuzzIngressNeverForwardsAClientIdentity` passed with the identity strip
  **deleted from production code**. Its route allowlisted `10.0.0.0/8` while the
  test's peer was `192.0.2.10`, so every request 404'd at the allowlist and
  never reached the code under test. A timeout branch swallowed the miss.
- A "long-running task is stopped by the ceiling" test used a 100ms ceiling
  against work that finished in 0.12s. Nothing was ever interrupted.

Earlier, the batch-poisoning harness passed against a **deliberately planted
retainer**, because scribbling alone was useless — a source reuses one batch, so
the refill landed on the same arena and the stale pointer read the *new* record.

### The rule

> Before a test counts as evidence, **observe it fail**. Revert the fix, plant
> the bug, or break the invariant by hand; watch the test go red with a message
> that names the real problem; restore.

Record the observation in the commit message or the test's own comment. A test
whose failure has never been seen is a comment with a `func` keyword.

### Tells that a test is vacuous

| Tell | What it usually means |
|---|---|
| Suspiciously **fast** | The setup failed and the assertion never ran. |
| Suspiciously **slow** (8 seeds, 14 seconds) | Every case is hitting a timeout branch. |
| A `t.Skip` on the interesting path | The property is skipped exactly when it matters. |
| Only asserts `err != nil` | Any bug produces *an* error; you have not tested *which*. |
| A `select` with a `time.After` that does nothing | The miss is being swallowed. |
| Assertion inside an `if` that may never be true | Guard the guard: fail if the branch never ran. |

For anything load-bearing, add a companion test that asserts the harness reaches
the code — `TestTheIdentityFuzzTargetActuallyReachesARunner` exists solely for
that.

---

## 2. Pick the instrument the failure would actually move

A gzip bomb in `azureblobconn` streamed **7,780,738 records from 1,822,535 wire
bytes** and allocated **0 MiB of retained heap**, because the streaming
architecture worked exactly as designed. A memory assertion called it safe.

The honest measures were **record count** and **wire-to-output ratio**.

| Property | Wrong instrument | Right instrument |
|---|---|---|
| Amplification through a streaming path | `HeapAlloc` | `TotalAlloc`, records out, output/input ratio |
| Transient buffering | live heap after the call | cumulative allocation across it |
| CPU cost | wall clock | actual CPU accounting (never wall-clock-as-CPU) |
| Admission behaviour | task count | peak simultaneous residency, governor `Used` |
| Latency histograms | that a value was recorded | that the **unit** matches the buckets |

Ask: *if this failed in the worst way, which number moves?* Assert on that one.

---

## 3. Test the quantifier, not the examples

ADR-0029 claims **any** validated topology executes. Six hand-built topologies
were green. Thirty-two *generated* ones found **five** defects — one of which
was a coin flip on Go's map iteration order, and one of which silently emitted
wrong records.

Where a claim contains "any", "every", or "never", hand-picked cases test the
wrong thing. Reach for:

- **Generative tests** over the input space, with an exact invariant (record
  conservation, round-trip identity, order preservation) and a fixed seed so a
  failure is reproducible from the printed seed alone.
- **Differential tests** against a reference implementation where one exists —
  `encoding/json` for ndjson, `math/big` for 128-bit decimal, `time` for
  temporal. Three of five injected decimal mutants, including `add128` dropping
  its carry, were caught *only* this way.
- **Fuzzing** for absence-shaped properties ("never panics", "always
  terminates", "never leaks payload"). See §7.
- **AST-driven enumeration** when the set under test is a registry: the hub's
  route sweep parses `api.go` with `go/ast` and asserts *swept set == registered
  set*, so adding a route without a case fails the test. A sweep that can silently
  omit a member is not a sweep.

**Watch for suppressions.** The generative corpus was capped at 9 records so a
deadlocking shape would fail rather than hang. That cap was load-bearing and
undocumented as such for a while. If a constant exists to keep a suite green,
say so in a comment and link the row that will remove it.

---

## 4. Bound the unit, not the stream

Every memory bomb found in this codebase was the same shape: the *stream* was
bounded, and one **unit** inside it was not.

| Unit | Reader | Bound |
|---|---|---|
| Line | `ndjson`, `fixedw` | `MaxLineBytes` |
| Field / record | `csvf` | `MaxRecordBytes` |
| Segment | `edi` | `MaxSegmentBytes` |
| Value | `ndjson` JSONReader | value budget |
| Depth | `ndjson`, `xmlf`, `soapconn` | `MaxDepth` |
| **Width** | `soapconn` | `maxXMLElements` |
| Expansion | `engine/schema` | `$ref` memoisation |
| Inflation | HTTP-ish connectors | decompression **ratio** |

When adding a reader or a parser, enumerate its units and ask which of them an
attacker chooses the size of.

### Ratio or absolute cap? Follow the consumer

- **Streaming consumer** → **ratio**. Volume is not the threat; ADR-0003's exit
  criterion is a 1 GB stream at bounded RSS, so a byte cap refuses the work the
  product exists to do. Amplification is the threat, and a ratio charges an
  attacker real bandwidth per byte of our work.
- **Buffering consumer** → **absolute cap**, applied to the *decoded* stream.
  `soapconn` buffers a whole response, so `max_response_bytes` is right there.
- **Cost that is per-item rather than per-byte** → **a count**. SOAP width could
  not be a ratio because an element is only four bytes; the honest bound was
  `maxXMLElements`.

### Every bound needs three things

1. A **safe default**. A limit that only helps when configured protects nobody.
2. A **companion test that ordinary large input still works** — a bound that
   breaks a legitimate 1 GB transfer is worse than the bug it closed.
3. Application **as the stream is read**, not after. Assert that too: a bound
   applied at the end has already spent the memory.

---

## 5. A property that holds by accident is not a property

`s3conn` and `azureblobconn` build **identical** `http.Transport`s. One never
inflates a gzip response; the other streamed 512 MiB from 1.8 MB. The difference
is that the AWS SDK sets `Accept-Encoding` itself and the Azure SDK does not.

That is a coincidence in someone else's code, not a security property.

When a guarantee depends on a dependency's behaviour:

- **Take the decision back** where you can (here: `DisableCompression`, then ask
  for gzip deliberately and meter it).
- **Assert it anyway**, so an SDK bump breaks the test rather than the product.
  `s3conn`'s percent-encoding test exists for exactly this reason and says so.
- **Share one implementation.** Three copies of a bound drift; the finding *was*
  that they had drifted.

---

## 6. Re-measure after the fix, not just before it

The obvious cause of SOAP's 421 MiB was a map pre-sized to the child count
rather than the distinct-name count — a real bug, on the shape SOAP returns
most. Fixing it moved 421 MiB → **391 MiB**.

**It was 7% of the problem.** A code reading would have closed the row there.

Re-run the measurement after every fix and check the number moved *as much as
the theory predicts*. If it did not, the theory is wrong and the bug is still
there.

---

## 7. Let the tool correct the specification

Two of three fuzz findings in one sweep were over-broad **assertions**, not
bugs:

- `X-Forwarded-For` legitimately becomes `X-Shift-Client-Ip` for a trusted proxy
  (ADR-0038). "Derived from" and "forwarded verbatim" are different claims.
- A schema compile error may name a script identifier, because the *script* is
  authored configuration the hub already stores, whereas a *record's* contents
  are payload it must never see.

Narrowing a claim is a real outcome. Record it as "the specification was wrong",
not as "no bug found" — the next reader needs to know the property changed.

---

## 8. Hunt the failures that succeed

The most dangerous defects here produced **no error**:

| Defect | What it looked like |
|---|---|
| Duplicate branch operator (TC-034) | `{"nid":null}` instead of `{"nid":0}` — delivered |
| Unbounded inflation | 7.8M records, clean `io.EOF` |
| Unbounded XML width | call succeeded, 421 MiB |
| Latency histogram in the wrong unit | dashboard looked fine |

None would ever arrive as a bug report. Techniques that find them:

- **Assert the value, not the absence of an error.** `err == nil` is not a
  passing outcome; the right records are.
- **Assert conservation.** Records in vs records out, per topology, exactly.
  Multiplicity too — a self-join's output count is a fact you can predict.
- **Assert provenance.** Not just "a value arrived" but "the value the gateway
  stamped, not one the caller sent".
- **Assert the unit and the shape of telemetry**, not merely that it was emitted.
- **Round-trip.** Write then read (xmlf, spill codec, fixedw) and compare bytes.

---

## 9. Concurrency, lifetime, and resources

- **Goroutine leaks:** `engine/leaktest`, wired as `TestMain`. It is an
  *identity diff* (alive at end, not at start), not a stack denylist, so it needs
  no per-package tuning and cannot be silenced by a name change. Runtime-owned
  creators are excused; `net/http` persistConn and `database/sql` openers
  deliberately are **not** — an unclosed client is a finding.
- **Batch lifetime:** `engine/batchtest` destroys each batch the instant its
  lifetime ends. Note the lesson inside the tool: poisoning must **release the
  chunks as well as scribble them**, because reuse alone hides the bug.
- **Reservations:** a task that terminates must return its admission slot
  *however* it ended. Assert both endings — cut off by the ceiling and finishing
  normally. A task that ends but keeps its slot is a leak with extra steps.
- **Deadlock:** where a bounded queue sits between a producer and a consumer
  that may not read yet, backpressure cannot help. Test above the queue depth,
  and give the test a **hard deadline** so a regression fails instead of hanging
  the suite.

---

## 10. Make failures say the true thing

Two distinct bugs of the same family:

- A runner with a valid credential got **401** when the hub's database was down.
  The fleet recovered on its own (retry with backoff) — while sending an
  operator to hunt a credential problem that did not exist. Now: only
  `store.ErrUnauthorized` is 401; anything else is 503.
- A gzip bomb surfaced as `ndjson: line 179179: column 11: expected ':'` — a
  parse error for a size problem, sending the reader after a data bug that was
  not there.

**Test the taxonomy, not just the status.** Distinguish:

- "no" from "I cannot answer" (401 vs 503),
- a size problem from a syntax problem,
- a client mistake from a server failure (a malformed UUID is 404, not 500).

And test **both directions**: turning every auth failure into 503 is the same
bug facing the other way, because a hub that never says "not you" cannot be
diagnosed either.

---

## 11. Gate discipline

- **Run every gate before claiming green.** `make check` = `lint` + `cover` +
  the rest. Running `lint` and `test` and reporting "green" is how a branch got
  pushed that failed CI twice at 19 minutes.
- **Never pipe a gate through `grep`.** A filtered `make leaks` hid a real
  gitleaks failure. Run gates bare and read the tail.
- **`golangci-lint cache clean` before `make check`** — a stale cache hides
  findings the pre-push hook then rejects.
- **A coverage failure is a question, not a number.** `engine/stream` dropped
  below its floor because `SpillBuffer`'s **spill path had never executed in any
  test** — the only thing exercising it stayed under the governor budget. The
  fix was nine tests, not a lower floor. Ask what the uncovered lines *do* before
  touching `coverage.thresholds`.
- **Watch what your tests cost.** Bomb tests generating 512 MiB streams added
  ~4.7 minutes to every CI run; 64 MiB proved the same property in 1.6 seconds.

---

## 12. What to check next

A checklist for the next sweep. Each item is phrased as the question to ask; the
ones already answered carry their register row so they are not re-litigated.

### Data path

- [ ] **Encoding hostility** (TC-024, open) — what does a customer *receive* for
      invalid UTF-8, mixed encodings, BOMs, lone surrogates? Degrade or refuse,
      never silent mojibake that is later trusted.
- [ ] **Numeric edge cases end to end** — does a value survive source → transform
      → sink without a float round-trip? (Exact decimals: TC-004 ✅.)
- [x] Unit bounds on every reader — TC-019 ✅
- [x] Decompression bounds — TC-020 ✅
- [x] Structural depth and width — TC-022 (soapconn ✅; `$ref` TC-018 ✅)
- [ ] **Spill exhaustion** — what happens when the scratch volume fills mid-join
      or mid-buffer? Is the error attributable, and is the slot returned?
- [ ] **Governor accounting drift** — after N thousand tasks, does `Used()`
      return to zero? A slow leak in reservation accounting shrinks admission
      capacity for the process lifetime.

### Control plane

- [ ] **Migrations against a populated database** (TC-007b, open) — `Store.Migrate`
      has only ever run on an empty schema. Also: are released migration files
      immutable?
- [ ] **Metric-name conformance** (TC-002, open) — mirror `shiftlog`'s AST
      vocabulary test. A rename silently breaks dashboards; alert on `event` and
      metric names, never on `msg`.
- [ ] **Clock and timezone** — UTC crons, DST transitions, the "Postgres `now()`
      is the only clock" rule. Does a schedule fire once across a DST boundary?
- [ ] **Idempotency under concurrency** — two runners racing the same key; the
      key stable across re-dispatch (TC-010 ✅) *and* under contention.
- [ ] **Certificate rotation and expiry** — mTLS control plane: what happens at
      renewal, and at expiry mid-lease?
- [x] Error envelope on every surface — TC-012 ✅, TC-030 ✅
- [x] Store-failure error arms — TC-008 ✅

### Security

- [ ] **Secrets in error strings** — a resolved secret appearing in an error,
      a log line, or an error-handler record. Redaction exists; is it asserted on
      every path that can carry a value?
- [ ] **Payload never at the hub** — TC-013 ✅ for capture; re-ask it whenever a
      new report field is added. Field names and rule ids are metadata; values
      are not.
- [ ] **Authorisation, once RBAC lands (issue #16)** — every route, every role,
      including the negative cases. The route sweep is the mechanism.
- [x] Path/name handling from remote listings — TC-023 ✅
- [x] SSRF guards on outbound connectors — `pkg/httpsec`, per-connector dial guards
- [x] Untrusted-input fuzzing — TC-003 ✅, TC-017 ✅
- [ ] **Supply chain** — signature verification fail-closed is tested; is
      *downgrade* (an older signed version) refused where it should be?

### Execution

- [x] Any validated topology executes — TC-005 ✅ (five defects found)
- [x] Goroutine and batch lifetime — TC-001 ✅, TC-009 ✅
- [x] Admission governed by resources, not counts — TC-011 ✅
- [ ] **Resume cursors across connector versions** — the refusal is implemented;
      is it tested that a mismatched build refuses rather than resuming at the
      wrong position?
- [ ] **Error routing under fan-out** — which step owns a failure when two inputs
      converge (ADR-0029's own open question)?

### Suite health

- [ ] **TC-035** — the e2e suite timed out twice under load, never reproduced
      since. If it recurs, the fix is condition-based waits or serialising the
      process-spawning tests, not a longer timeout.
- [ ] **Runtime budget** — the suite is the thing everyone waits on. Periodically
      ask which tests cost the most and whether they still earn it.

---

## 13. When you find something

1. **Measure it before you fix it**, with the instrument from §2, and put the
   number in the commit message. "421 MiB from 1.6 MB, and the call succeeded"
   is worth more than "unbounded width".
2. **Write the test first**, watch it fail for the right reason.
3. **Fix, then re-measure** (§6).
4. **Check the same shape elsewhere.** Every bug in this codebase had a sibling:
   one unbounded reader implied four more; one unbounded transport implied two
   more; one edge-key convention implied three functions.
5. **Record it in `docs/assurance/test-conformance.md`** with the test named. A
   row is ✅ only when it cites something that fails if the invariant breaks.
6. **Bump the connector version** if behaviour changed, and declare
   `behaviour-change` (ADR-0047 §6) when input previously accepted is now
   refused — even if the config surface is unchanged.
