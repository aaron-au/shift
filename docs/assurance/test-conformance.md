# Test conformance register (living document)

**Question this answers:** for every property an accepted ADR asserts, is there
a test that would **fail** if the property regressed?

Not "is there a test near that code", and not "is the package above its coverage
floor" — those are `docs/test-plan.md` and `coverage.thresholds`. This register
tracks the narrower, harsher thing: *the invariant is enforced, not merely
stated.* A package can sit at 95% coverage while the one contract whose
violation would sink the product is held up by a comment.

See [`README.md`](README.md) for the rules. The load-bearing one:

> **A row is ✅ only when it names a test that fails if the invariant breaks.**
> "Code review", "convention", "depguard covers it" ⇒ ⬜ or 🟡, never ✅.

**Status legend:** ⬜ todo · 🟡 partial (something tests it, but not the claim) ·
✅ done (test named) · ➖ deliberately not tested (reason recorded in §4)

---

## 1. Sweep log

| # | Date | Commit | Scope | Method |
|---|---|---|---|---|
| 1 | 2026-08-09 | `4e908b5` | All 7 modules vs every ADR marked accepted/built | Enumerated 1519 `func Test*`, 5 fuzz targets, 7 bench files, 5 e2e scenarios; cross-read against the 40 built ADRs; checked `Makefile`/`ci.yml`/`coverage.thresholds` for what actually gates. Findings TC-001…TC-016. |

**Open / closed:** see the status column. Rows TC-017/TC-018 were opened by the
sweep that closed TC-003 — a register that never grows is a register nobody is
using.

**Repeating a sweep.** Dump the surface first, then read it against the ADR list
rather than reading packages one at a time — the gaps are absences, and absences
only show up against a claim:

```sh
grep -rhn "^func Test" --include='*_test.go' . | grep -v _archive \
  | sed 's/^[0-9]*:func //;s/(.*//' | sort > /tmp/tests.txt
grep -rn "^func Fuzz\|^func Benchmark" --include='*_test.go' . | grep -v _archive
for f in docs/adr/0*.md; do printf "%-52s %s\n" "$(basename "$f")" "$(grep -m1 -i '^Status:' "$f")"; done
```

Then for each **built** ADR, ask: which line in `/tmp/tests.txt` fails if this
ADR's central claim stops being true? No line ⇒ a row below.

---

## 2. Register

### 2a. Missing test *types* — whole categories absent

| ID | Claim / ADR | Evidence today | Gap | Status |
|---|---|---|---|---|
| TC-001 | Every task gets its own goroutine(s); branches run concurrently and terminate (ADR-0005, ADR-0029) | `engine/leaktest` (+ its verbatim gateway copy), wired as `TestMain` in 10 packages — see the closure note below | **Closed 2026-08-09.** Found and fixed one production leak (`sdk/host.connect` stranding a gRPC `ClientConn`) and three test-hygiene leaks | ✅ |
| TC-002 | Alert on metric names; they are an operator contract (ADR-0020) | None. Both `*/internal/telemetry` packages have zero tests and are gate-excluded by ADR-0022 | `pkg/shiftlog` has an AST-level suite pinning `event` names and key spelling (`vocabulary_test.go`). Metrics are the same contract with none of the discipline — a rename silently breaks dashboards. Mirror the vocabulary test for metric names + labels | ⬜ |
| TC-003 | Parsers eating untrusted bytes never panic/hang/over-allocate (ADR-0022 §5, ADR-0042) | 7 new targets across `edi`, `fixedw`, `xmlf`, `csvf`, `record` (ParsePath + scalars), `schema` (Compile + Validate), wired into `make fuzz` | **Closed 2026-08-09. Found a real memory-exhaustion bug in `edi` — fixed.** Gateway ingress and starlark script text remain unfuzzed (⇒ TC-017) | ✅ |
| TC-004 | Exact decimal/temporal semantics: 128-bit `Compare`, `ExactSum` (ADR-0051) | `engine/record/{decimal_diff_test.go,temporal_diff_test.go}` — generated inputs checked against `math/big` and `time`, plus 3 fuzz targets | **Closed 2026-08-09.** No bug — but 3 of 5 injected mutants were caught ONLY by these tests, which is the hand-picked-cases weakness demonstrated rather than asserted | ✅ |
| TC-005 | **Any** validated topology executes — nested fan-out, mixed fan-out/fan-in included (ADR-0029) | `runner/internal/service/topology_gen_test.go` — 32 generated valid DAGs from a fixed seed at 12,000 records, asserting exact record conservation | **Closed 2026-08-09; the claim was FALSE and is now true.** Five real executor bugs found (TC-027, TC-028, TC-033, TC-034, TC-029) and all five fixed. The last suppression — a 9-record corpus, kept only so the enrichment shape failed instead of hanging — was lifted on 2026-08-10 with TC-029 | ✅ |
| TC-006 | 0-alloc steady state on the hot path (ADR-0003, ADR-0004) | `alloc_test.go` in `engine/{stream,record,format/ndjson,format/csvf}` — every hot path measured, 0 asserted exactly where it is 0 | **Closed 2026-08-09.** Found and **fixed** three per-record allocations on hot paths (coerce-to-text, JSONReader, csvf typed columns); corrected an inaccurate package doc | ✅ |
| TC-007 | Protocol + artifact versioning stays backward-compatible (ADR-0007, ADR-0047); migrations are production code (ADR-0006) | `sdk/host/legacy_connector_test.go` — a real protocol-1 connector subprocess driven through the current Launch/handshake/Pull path | 🟡 **Connector half closed 2026-08-09.** The hub-schema half is still open: `Store.Migrate` is only ever applied to an EMPTY database — no populated v(N−1) upgrade, no immutability check on released migration files | 🟡 |
| TC-008 | Store-failure error arms behave (ADR-0009, ADR-0049) | `hub/internal/api/{faultinject_test.go,storefault_test.go}` — faults injected at the DATABASE (row triggers raising a chosen SQLSTATE on a chosen firing), so real transactions really roll back | **Closed 2026-08-09.** No partial-write bug found: every multi-statement operation rolled back cleanly. api coverage 87.3% → 89.7%, so the apologetic 87 floor can go back to 88 | ✅ |

**TC-001 closure note (2026-08-09).**

*What was built.* `engine/leaktest` — a stdlib-only detector (~250 lines of
`runtime.Stack` parsing). Not `go.uber.org/goleak`: `engine/go.mod` and
`gateway/go.mod` both have zero `require` lines, and for the gateway that is
enforced by `TestGatewayModuleStaysDependencyFree` — a security property, since
the gateway is the one component in a DMZ. The gateway therefore holds a
**verbatim copy** under `gateway/internal/leaktest`, the same arrangement
ADR-0046 §2 uses for logging, held byte-identical from the package clause down
by `TestTheGatewayLeaktestCopyHasNotDrifted` in `pkg/shiftlog`.

The check is a goroutine-**identity diff** (alive at the end, not alive at the
start), not a stack denylist, so pre-existing framework goroutines never
register and no per-package tuning is needed. Runtime-owned creators (GC
workers, signal goroutines) are ignored; `net/http` persistConn loops and
`database/sql` connection openers deliberately are **not** — an unclosed client
is a finding, not noise.

*Wired into* (`TestMain`): `engine/stream`, `sdk/host`, `runner/internal/`
`{service,gwclient,connpool,api,leaseloop}`, `hub/internal/scheduler`,
`gateway/internal/{ingress,runners}`. Excluded from the coverage gate
(`scripts/coverage.sh` `EXCLUDE_RE`) as a test helper, per ADR-0022.

*Findings, all fixed:*

| Kind | Where | What |
|---|---|---|
| **Production** | `sdk/host.connect` | The gRPC `ClientConn` was closed only on the handshake-timeout arm. The connector-exited and context-cancelled arms returned with it open, and both callers (`Launch`, `Attach`) discard the `Process` on error — so nothing downstream could ever close it. A runner whose connector kept dying leaked a connection and its goroutines **per relaunch attempt**, for the life of the process (ADR-0005). Now closed once, in a `defer`, on every failure path. Regression test: `TestAFailedConnectDoesNotStrandTheConnection` — verified to fail (4 and 5 stranded goroutines) with the fix reverted |
| Test hygiene | `runner/internal/gwclient` | `service.New` starts a connpool reaper; two construction sites never closed it (one a helper shared by 3 tests) |
| Test hygiene | `sdk/host.serveRaw` | A test proving Attach *refuses* still left a live gRPC server behind. Cleanup now stops it the way a host would |

*Also improved:* the report tallies leaks by creator frame **before** the capped
stacks — with 5 of 40 stacks printed, the counts were the only place the shape
of the leak survived.

*Not yet wired:* the `connectors/*` packages and `hub/internal/api`. Neither is
goroutine-centric, so they were left for a later pass rather than done thinly.

**TC-003 closure note (2026-08-09) — real bug found in `engine/format/edi`.**

`MaxSegmentBytes` was bypassed on the EDIFACT release-character path.
`readSegment` appended the escape pair and `continue`d, skipping the size check
every other byte passed through — so an interchange of `?a?a?a…` with no
segment terminator buffered **the entire file**, whatever the limit said. `?`
is the *default* EDIFACT release character, so no unusual UNA header is needed:
a trading partner sending a 1 GB file of escape pairs put 1 GB in the runner.

That is simultaneously the failure `DefaultMaxSegmentBytes`' own doc comment
says it exists to prevent ("reading it to EOF would buffer the whole file — the
one thing this package exists not to do"), the streaming doctrine's central
rule, and TC-003's stated property. It is reachable from untrusted partner
input with no authentication.

The fuzzer found it in **five seconds**, via the bound assertion rather than a
crash — which is why the targets assert declared limits and not merely "did not
panic". Fixed by restructuring the loop as a `switch` with ONE bound check on
every path that grows the buffer, so a future branch cannot reintroduce the
bypass by forgetting to fall through. Regression test:
`TestAnEndlessRunOfEscapesIsBoundedLikeAnyOtherSegment`, verified to fail
without the fix. The fixed reader then survived 14M executions.

Targets assert real bounds, not just absence of panic: `edi` MaxSegmentBytes,
`xmlf` MaxDepth (re-measured per record), `fixedw`/`csvf` batch and field
counts, `ParsePath` compiled steps ≤ input bytes, `schema` MaxViolations and
`Valid`/`Validate` agreement.

**Also reported, not a fuzz finding:** `schema.Compile` inlines each `$ref` by
recompiling its target per reference site, and its cycle guard only covers the
current stack — so a diamond of `$defs` compiles in 2^n nodes from ~1 KB of
schema. Only reachable by an authenticated flow author today. Logged as TC-018.

**TC-014 closure note (2026-08-09).** The package had no tests at all; its 86%
came entirely from `service`/`api` exercising it in passing, which evaporates
the moment a caller changes. Four defects surfaced:

- **Fixed — in-flight gauge leak.** `Waiting`/`Running` are gauges; `Submitted`/
  `Completed`/`Failed` are lifetime counters. Eviction deleted a *non-terminal*
  task without retiring its gauge, and the later `Update` that would have
  decremented finds no record and returns. On a long-lived runner the counts
  only ever climb — a dashboard reading "37 running" on an idle box.
- **Fixed — terminal tasks could be re-terminated,** counting in both
  `Completed` and `Failed`. Both are monotonic, so the double count is
  permanent. `service.run`'s panic guard was the *only* thing preventing it;
  it is now a second line of defence. The outcome is immutable; other fields
  (a late error string, a capture) stay writable.
- **Recorded, not fixed — `Get`/`Recent` return shallow copies.** A caller can
  write through `Ops`, `Captured` or `Checkpoint` into the stored task with no
  lock held. No current caller does; latent rather than live.
- **Recorded, not fixed — a duplicate id would nil-deref `Recent`.**
  Unreachable while ids are 16 random bytes, but an unguarded invariant.

**TC-010 closure note (2026-08-09).** Tested at **both** levels, and the reason
is the finding: four deliberate breaks were injected, and the fourth — the
runner's `leaseloop` appending `:retry` to the key on attempt 2 — **passed
every store-level test** and was caught only by the e2e. The store proves the
column survives; only an end-to-end run proves what the *sink* receives.

The e2e observes the `Idempotency-Key` headers a real HTTP destination actually
received across a SIGKILL redispatch, and asserts attempt 2's key is byte-
identical to attempt 1's. The store tests cover the lease-expiry path, the
separate `Fail`-and-requeue path (different SQL, so separately breakable), and
that a keyless task stays keyless. **No bug found: the key is stable.**

One nuance worth knowing: a task with no idempotency key is *not* keyless at
the sink — the runner substitutes the task id, which is equally stable. "No key"
at the API means "task-id-keyed" at the sink.

### 2d. Opened by a later sweep

Rows raised while closing an earlier one. Numbered from where §2a left off.

| ID | Claim / ADR | Evidence today | Gap | Status |
|---|---|---|---|---|
| TC-017 | The remaining untrusted-input surfaces are fuzzed (ADR-0022 §5) | 6 new targets in `make fuzz`: `gateway/internal/ingress` (whole request line, identity forwarding, status-path split, X-Forwarded-For) and `runner/internal/starlarkop` (Compile, Run) | **Closed 2026-08-10.** ~21M executions, **no production bug** — and three defects in the *tests*, which is the finding. See the closure note | ✅ |
| TC-018 | Schema compilation is bounded (ADR-0042) | `engine/schema/expansion_test.go` — a 40-level `$defs` diamond, plus guards for ordinary reuse and for indirect recursion | **Closed 2026-08-10.** Measured: **3,694 bytes of schema text had not finished compiling after 20 seconds** (2^40 nodes). `$ref` targets are now memoised — compiled once, node shared — which is sound because a compiled node is immutable and compilation is context-free. Now 0.00s and 0 MiB. The recursion guard is unaffected: the memo is written only AFTER a successful compile, so a target still on the stack can never be served from it (pinned by its own test) | ✅ |

**TC-012 closure note.** The 401 is the response a client meets more often than
any other, and it was the one shape no client could parse the same way as the
rest. Now routed through `writeErr` like everything else; the MESSAGE stays
deliberately uninformative (one opaque failure per path, no oracle) — shape and
detail are separate decisions. Verified: the tightened sweep fails against the
old code with *"Content-Type = text/plain … body is not the ADR-0023 envelope"*.

Enumeration is the part worth copying: the stdlib mux does not expose its
patterns, so the sweep parses `api.go` with `go/ast` and asserts the swept set
EQUALS the registered set. Adding a route without adding a case fails the test —
which is the only version of this sweep that keeps working.

Three further findings from TC-008/TC-012, recorded not fixed:

- ~~**405 Method Not Allowed is router-generated `text/plain`**~~ — **FIXED
  2026-08-10 (TC-030).** Decided in favour of holding ADR-0023's promise as
  written rather than narrowing it: `routerErrors` middleware rewrites a
  ROUTER-generated 404/405 into the envelope, distinguished from a handler's own
  answer by the content type writeJSON has already set. The mux's `Allow` header
  survives, since it is the useful part of a 405. Reachable by ordinary client
  error, not only by probing — it was found by a test of ours that requested
  `/api/v1/runners/lease` instead of `/api/v1/lease`. Non-vacuity: removing the
  middleware fails three tests (`hub/internal/api/routererr_test.go`).
- ~~**A runner gets 401 when the hub's DATABASE is down**~~ — **FIXED
  2026-08-10 (TC-031).** `authRunner` is a DB round-trip and any error became an
  opaque 401. Checked the consequence rather than assuming it: `hubclient` does
  **not** treat 401 as terminal, and `leaseloop` retries with backoff, so this
  was a **diagnosability** bug, not an availability one — the fleet recovered
  while telling the operator the wrong story, and every runner reporting
  "unauthorized" at once looks like a credential problem. Only
  `store.ErrUnauthorized` now means 401; anything else is 503. Both halves are
  pinned (`hub/internal/api/dbdown_test.go`) — a bad secret must still be 401,
  or a hub that never says "not you" cannot be diagnosed either. Non-vacuity:
  restoring the blanket 401 fails the outage test with the exact envelope
  `{"error":{"status":401,"message":"unauthorized"}}`.
- `POST /api/v1/connectors/collect` ignores its request body entirely (200 for
  `{`); it is driven by `?apply=1`. Malformed input silently accepted.

**TC-023 closure note (2026-08-09) — the most serious security finding of the sweep.**

**FTP command injection (CWE-93), proven on the wire.** `jlaffaye/ftp` builds
`"DELE %s"` and `net/textproto` terminates it with CRLF; neither validates the
argument. One configured `path` became **two FTP commands** — transcript from
the reverted-fix run:

```
["USER u" "PASS " "FEAT" "TYPE I" "DELE a.txt" "DELE /etc/passwd"]
```

The second command runs with the connection's credentials, outside the verb and
path the flow author was granted. Worse, an injected `PORT`/`EPRT` makes the
**server** dial an address of the attacker's choosing — egress this connector's
own network guard can never see, because it is not the process connecting.
Fixed by rejecting CR/LF/NUL in `path`/`from`/`to`. Nothing legitimate is lost:
FTP pathnames travel inside a CRLF-delimited line, so spaces, unicode, dots and
long paths all still pass.

**Zip-slip in the `list` verbs (sftp and ftp).** Both emitted
`path.Join(dir, entryName)`, and that emitted path exists to be fed into a
following get/delete/rename node. `path.Join` **cleans** a hostile name, so the
next node acted outside the listed directory:

```
entry "../../etc/passwd"      → emitted "/etc/passwd"
entry "data/.."               → emitted "/incoming"
```

Reachable even through `pkg/sftp`, which drops `.`/`..` — it applies `path.Base`
to the RAW name, so a server replying `"x/.."` delivers `".."`. FTP is worse:
entry names are parsed from free-form LIST output, so `/` survives intact. Fixed
by failing the whole listing — not a silent skip, not a rewrite: a rewritten
hostile name produces a file the operator cannot correlate with its source.

**fsconn** — the jail was already correct against every `../` spelling, absolute
paths, symlink escapes and over-long paths. One gap: NUL passed `resolve`
(containment stops at the deepest EXISTING ancestor and never reached the NUL
component), and `put` created the destination's parent directories *before* the
syscall refused — a rejected path with a filesystem side effect. Fixed.

**s3conn / azureblobconn — already safe, now asserted.** Keys never become local
paths; `/` is a display convention within a bucket. Deliberately NOT "fixed":
cleaning a key would address a *different object*, which is the same mistake
inverted. Tests pin that keys reach the API byte-identical.

**Deliberately still accepted** in listings: spaces, NFC *and* NFD unicode,
non-Latin scripts, Cyrillic homoglyphs, RTL-override names, leading `-`,
trailing dots. Display-spoofing is an audit-trail hazard, not a traversal, and
rejecting it would reject legitimate non-Latin filenames.

**Versions bumped and declared `behaviour-change` (ADR-0047 §6):** ftp 0.4.0 →
0.5.0, sftp 0.5.0 → 0.6.0, fs 0.4.0 → 0.5.0. These refuse inputs previously
accepted, and the config surface is unchanged — precisely the case §6 exists
for, and `TestSurfaceStaysCompatible` correctly refused the bump until the
surface was re-recorded.

**~~Reported, not fixed~~ — FIXED 2026-08-10 (TC-032).** S3 `..` reached the
request line unescaped (`/b/../../etc/passwd`). Real S3 treats keys opaquely, so
there is nothing to traverse; the risk is a *normalising reverse proxy* in front
of an S3-compatible endpoint, which resolves `..` by default.

The rule is scoped to exactly that case: a `..` **path segment** is refused only
when a custom `endpoint` is configured. AWS proper accepts the same keys, so no
technically legal AWS key is refused, and a key that merely CONTAINS dots
(`report..final`, `v1.2..3/data`) is untouched. The refusal is loud and explains
both conditions — silently cleaning the key would read a DIFFERENT object and
report success, which is the same mistake inverted. **s3 0.5.0 → 0.6.0,
`behaviour-change`.**

### 2f. DAG executor defects (opened 2026-08-09 by TC-005)

ADR-0029's central claim — *any validated topology executes* — was **not true**.
The generative test found five ways it failed. All five were in
`runner/internal/service/dag.go`; none was in validation, so `pkg/flowdoc`
accepted every one of these flows and the runner then failed or hung on them.

**All five are now fixed** (TC-027/028/033/034 on 2026-08-09, TC-029 on
2026-08-10), and the corpus that found them runs unsuppressed.

These are the most serious findings of the whole sweep, and they are exactly
what six hand-built topologies could never have surfaced.

**Closed 2026-08-09 — TC-027 and TC-028 fixed, and the hunt found two more.**
All four share one root cause: `edgeIn` is keyed by the plan edge a pipe
crosses, and **three** functions used that key with two different conventions.
`buildFanOut` registered at the branch's *last* node; `segmentTo` looked up at
its *first*; `inputEdge` at the fan-out. The key now has a documented meaning
and all three agree.

| ID | Defect | Repro | Severity |
|---|---|---|---|
| TC-027 | A fan-out branch that reaches a merge **with no intervening operator** always fails at run time. `buildFanOut` registers the branch pipe under `edgeKey(last, endID)` — which is `edgeKey(mergeID, mergeID)` when the branch is empty — while `inputEdge` looks it up under `edgeKey(fanOutID, mergeID)` | `src → tee[a,m]; a: filter → m; m: merge concat [a, tee] → sink` ⇒ `service: fan-out "t" has no branch feeding "m"`. Confirmed for both tee and router | Deterministic failure; validated flow, refused at run time |
| TC-028 | A fan-out **downstream of a merge that is itself fed by a fan-out** fails **non-deterministically**. `compile()` iterates `plan.Nodes`, a Go **map**, so fan-out build order is random. If the downstream one compiles first it recurses through the merge before the upstream one has registered its branch pipes, falls back to `streamLeaving`→`segmentTo`, and hits an incompatible edge-key convention | `src → tee → 2 filters → merge(concat) → tee → 2 sinks` — **failed 9 of 20 runs of the identical document**, `service: node "a" reads a fan-out branch that terminates elsewhere` | **Non-deterministic.** Same flow, same runner, coin-flip. This is ADR-0029's named enrichment shape |
| TC-029 | ~~The enrichment shape **deadlocks permanently** above ~10 batches~~ — **FIXED 2026-08-10.** A fan-out branch that a blocking merge will not read yet now ends in a governed `stream.SpillBuffer` whose writer never blocks, instead of a bounded pipe (ADR-0029 amendment) | `src → tee → [probe, build] → join → sink` at 12k records: **hung forever**, now completes in ~0.02s with all 12,000 records joined. `runner/internal/service/enrichment_test.go`, deadline-guarded so a regression fails rather than hangs | ✅ **The TC-005 corpus cap is lifted with it**: `topologyRecords` 9 → 12,000, so all 32 generated topologies now exercise multi-batch flow |

| TC-033 | **Nested fan-out with ≥2 operators between parent and child** fails deterministically — same key mismatch, third convention | `node "t2" reads a fan-out branch that terminates elsewhere` | Deterministic refusal of a validated flow |
| TC-034 | **Nested fan-out with exactly ONE operator between: silent data corruption.** The two keys coincidentally collide, so the branch operator is applied a SECOND time on top of a pipe that already carries it | A rename project emitted `{"nid":null}` instead of `{"nid":0}`. **No error.** Filters and pass-through projections mask it entirely | **The worst defect of the sweep.** Not a refusal — wrong records delivered to a customer's system with no signal at all |

TC-033 and TC-034 were not on anyone's list; they surfaced while probing the
shapes around TC-027/TC-028. TC-034 is the one that matters: every other defect
in this register fails loudly. This one succeeds and lies.

**Two consequences worth separating from the bugs themselves:**

1. TC-029 is a flow-control *design* problem (bounded pipe + bounded tee queue +
   a blocking operator that must drain one side first), not a typo. It needs a
   design decision — spill the build side, unbounded-buffer it, or reject the
   topology at validation — and probably an ADR-0029 amendment.
2. **`TaskTimeout` defaulting to 0 turns any executor hang into a permanent
   leak.** Independent of TC-029, a runner should not be able to hold a
   reservation forever because one flow wedged. Worth its own decision.

**Suppressions removed, and replaced with positive coverage.** The generator no
longer avoids these shapes; it *tallies* them (`empty-merge-leg`,
`fanout-below-merge`) and the corpus assertion fails if a future generator
change stops producing them. Absence of a shape can no longer masquerade as 32
green cases.

Measured failure rate for TC-028, the non-deterministic one: **9/20** unfixed
(matching the original report), **110/200** with the other fixes but the
ordering reverted, **0/200** fixed. The regression test runs 20 in-test
iterations, so at ~50% a clean pass is a one-in-a-million accident rather than
luck.

**TC-029 remains open** — it is a design change (spill the join build side), not
a fix, and nothing in the join path was touched.

### 2g. Test-suite reliability

| ID | Claim | Evidence | Status |
|---|---|---|---|
| TC-035 | The suite gives the same answer under load as it does idle | `hub/e2e`'s two crash-redispatch tests (`TestCrashRecovery`, `TestTheSinkSeesTheSameIdempotencyKeyAfterACrashRedispatch`) **timed out** once during a full `make test` — 155s and 103s against a 20–72s norm — then passed five consecutive times (isolated, package, `-race`, module-wide `-shuffle=on`, full `make test`). Both spawn real `runnerd` and connector subprocesses, and `go test ./...` runs the hub's packages in parallel against one Postgres | ⬜ |

Recorded rather than shrugged off. A test that fails under load is a test people
learn to re-run, and a suite people learn to re-run is one where a real failure
gets waved through. The fix is not a longer timeout — it is either isolating the
process-spawning e2e tests from the parallel package run, or making their waits
condition-based rather than wall-clock. Deciding which needs a reproduction
first, which is why this is a row and not a patch.

### 2e. Hostile and malformed user data (opened 2026-08-09)

**Why this is its own section.** In an iPaaS effectively 100% of the bytes are
user-driven — authored by a customer, or fetched by a connector from a system
neither we nor the customer controls. "The parser is correct on good input" is
not the property that matters; "the platform survives, and says something
useful, on any input" is. The register's existing rows ask whether ADR claims
are enforced. These ask a harsher question: **what happens when the data is
actively trying to break us.**

TC-003 already demonstrated the value: one fuzz target found an unbounded-
buffering bomb in `edi` reachable from ordinary partner traffic, in five
seconds. That was one parser out of five, on one of several ingestion paths.

**The shape of the defence, and where it holds.** The streaming architecture is
itself the primary mitigation: data moves in bounded batches, so a large input
is not a large allocation. The bombs that get through are the ones where a
single *unit* is unbounded — one line, one field, one segment, one element.
That is precisely what the `edi` bug was, and the audit below found the same
shape elsewhere.

| ID | Property | Status today | Status |
|---|---|---|---|
| TC-019 | **Every reader bounds a single unit.** One record/line/field/segment cannot be unbounded | `ndjson` MaxLineBytes + MaxDepth ✅ · `xmlf` MaxDepth ✅ · `edi` MaxSegmentBytes ✅ (TC-003) · `csvf` MaxRecordBytes ✅ · **`fixedw` MaxLineBytes ✅** (`bomb_test.go`) | **Closed 2026-08-10. The last one was a real bug, not an audit.** `fixedw` was recorded as "bounded by its layout"; in fact `readLine` fell back to unbounded accumulation whenever a line exceeded the bufio buffer, on the record path as well as `SkipLines`. A source that never emits a newline ran `go test` to its **180-second timeout still buffering**. Now: 256 MiB offered, 1 MiB consumed, 4 MiB allocated against a 1 MiB bound — cost tracks the limit, not the input. `Unseparated` reads a fixed-length record and is bounded by construction | ✅ |
| TC-020 | **Decompression is bounded.** A compression bomb cannot exhaust the runner | `connectors/internal/decompress` (ratio bound, 12 tests) wired into `httpconn`, `azureblobconn` and `s3conn`; `soapconn` bounded separately by `max_response_bytes` applied to the *decompressed* stream | **Closed 2026-08-10. Found a second unbounded connector — `azureblobconn` streamed 7,780,738 records out of 1,822,535 wire bytes, to completion, with no error.** See the closure note | ✅ |
| TC-021 | **A stream that never ends is bounded** — total bytes, total records, wall clock. A source that trickles forever must not pin a runner slot indefinitely | `DefaultTaskTimeout` (6h, no "off") + `runner/internal/service/endless_test.go` — a task is interrupted by the ceiling, and the admission reservation comes back **whichever way it ends** | ✅ **Closed 2026-08-10, and two thirds of the row were the wrong property.** A total-bytes or total-records cap would refuse the work the product exists to do (ADR-0003's exit criterion is a 1 GB stream at bounded RSS) — the same argument the decompression bound settled: volume is not the threat. What matters is the slot, which is precisely what TC-029 leaked | ✅ |
| TC-022 | **Structural depth/width cannot exhaust the stack or the arena** — deep nesting, a record with a million fields, XML entity expansion (billion laughs), pathological schemas | Depth: `MaxDepth` on `ndjson`/`xmlf`, `maxXMLDepth` on `soapconn`. **Width: `maxXMLElements` (`soapconn/width_test.go`)** | 🟡 **soapconn closed 2026-08-10 — 1,600,101 wire bytes allocated 421 MiB and SUCCEEDED; now 35 MiB and refused.** Residual: TC-018's `$ref` 2^n expansion is the same class in `engine/schema`, and `ndjson`/`xmlf` width is bounded only indirectly by the per-line/segment caps | 🟡 |
| TC-023 | **Path/name handling from a remote listing is safe** — `../`, absolute paths, symlinks, NUL/control characters, zip-slip | `hostilenames_test.go` in all five file/object connectors | **Closed 2026-08-09. Found an FTP command injection (CWE-93) and zip-slip in two listings — all fixed.** See the closure note | ✅ |
| TC-024 | **Encoding hostility degrades, never corrupts** — invalid UTF-8, mixed encodings, BOMs, NUL bytes, lone surrogates. The record model must not silently produce mojibake a customer later trusts | Partly covered by the TC-003 fuzz seeds; no explicit assertion about what a customer *receives* | ⬜ |
| TC-025 | **A single bad record does not destroy the run.** The customer's stated need: "recover/ignore invalid data" | **ADR-0053 written 2026-08-09** — `@verify` is a router whose predicate is a schema; rejects route down an ordinary edge to a destination the developer owns | Designed, not built. No test can close this until the node exists | ⬜ |
| TC-026 | **The customer is told what was rejected and why.** Assurance that delivered data was valid | **ADR-0053 §6** — per-step counts and rejection reasons (field path + failed rule). Field names and rule ids are metadata; values never appear, so it rides the execution report without touching the two-plane split | Designed, not built | ⬜ |

**TC-022 note (2026-08-10) — the third structural dimension.**

`soapconn` bounded total bytes (`max_response_bytes`) and nesting
(`maxXMLDepth`). Width was invisible to both: cost is O(number of elements) and
an element costs four bytes on the wire. Measured, **1,600,101 bytes of `<a/>`
allocated 421 MiB — 256-276x — and the call succeeded.** Same shape as TC-020,
with structure amplifying instead of gzip.

Two fixes, and the order they were found in is the point:

1. The obvious suspect was `make(map[string][]*xmlNode, len(n.children))` in
   `build`, which pre-sizes buckets to the CHILD count rather than the
   distinct-NAME count — a 400,000-entry map to hold one key, on exactly the
   shape SOAP returns most (a list of identically-named entries). Fixing it
   saved 30 MiB. **That was 7% of the problem**, and assuming it was the answer
   would have closed the row on a real but minor improvement.
2. The rest is irreducible per-element cost across the decoder's tokens, the
   node tree and the record built from it. Unlike decompression this cannot be
   a ratio — elements are only a few bytes each — so the honest bound is a
   count: `maxXMLElements` (100,000, `max_response_elements`), checked as the
   document is read rather than after.

Result: 421 MiB → 35 MiB, refused by name, with a 5,000-element list still
accepted. **soap 0.1.0 → 0.2.0, `behaviour-change`** (ADR-0047 §6).

**TC-017 closure note (2026-08-10) — the fuzzing found no product bug and three test bugs.**

~21M executions across six targets left the gateway's request parsing and the
starlark compiler standing. What it did break was this register's own work:

1. **A vacuous target.** `FuzzIngressNeverForwardsAClientIdentity` passed with
   the identity strip DELETED from `forwardable`. The route it configured had an
   allowlist of `10.0.0.0/8` while the test's peer was `192.0.2.10`, so every
   request 404'd at the allowlist and never reached the forwarding path, and a
   timeout branch swallowed it. The tell was in plain sight — 8 seeds took 14
   seconds — and only the non-vacuity probe caught it. A property test that
   cannot reach the code it targets is worse than no test, because it reports
   success. `TestTheIdentityFuzzTargetActuallyReachesARunner` now guards it, and
   with the strip removed three tests fail.
2. **An over-broad property (gateway).** The target claimed no client header
   value may reach the runner under an `X-Shift-*` name. The fuzzer produced
   `X-Forwarded-For: 10.1.0.0` and the value duly appeared as
   `X-Shift-Client-Ip` — which is ADR-0038's trusted-proxy design working, not a
   leak. "Derived from" and "forwarded verbatim" are different claims; the
   property was narrowed to headers the CALLER named `X-Shift-*`.
3. **An over-broad property (starlark).** `FuzzCompile` asserted that no 64-byte
   run of the input may appear in an error, and the fuzzer produced a 64-byte
   identifier, yielding `undefined: AAAA…`. That is legitimate diagnosis: the
   *script* is authored configuration the hub already stores in the flow
   document, whereas a *record's* contents are payload it must never see.
   ADR-0052 §8 is about the latter. The verbatim check now applies to `Run`
   only; both targets still forbid a backtrace.

Two of those three were the fuzzer correcting the specification rather than the
code — which is the honest outcome to record, and the reason the closure note
says "no product bug" rather than "passed".

**TC-020 closure note (2026-08-10) — a property that held by accident of a dependency.**

The register said this row was closed for `httpconn` in the 2026-08-09 sweep. It
was not closed as a *row*: the audit fixed the connector that had been measured
and did not ask whether the same shape existed elsewhere. It did.

`soapconn`, `s3conn` and `azureblobconn` build the same plain `*http.Transport`.
Go's transport advertises `Accept-Encoding: gzip` on its own and transparently
inflates the reply, so all three owned a decompressor none of them wrote and
none could see in its own source. Whether that was *exposed* came down to
whether the SDK underneath happened to set the header first:

- `soapconn` — **safe, and deliberately so.** `max_response_bytes` is applied to
  the decompressed stream. Measured: 1,043,738 wire bytes claiming 1 GiB cost 41
  MiB and stopped in 99 ms.
- `s3conn` — **safe by accident.** The AWS SDK sets its own `Accept-Encoding`,
  so nothing inflated. Measured: 0 records, 3 MiB, ending in `ndjson: line 1:
  column 1: unexpected character '\x1f'` — gzip's magic number reported as a
  data error.
- `azureblobconn` — **not safe.** The Azure SDK does not set the header.
  Measured: **1,822,535 wire bytes became 512 MiB and 7,780,738 records,
  streamed to completion, ending in a clean `EOF`.** No error, no bound, ~294x
  amplification, and the flow *succeeds*.

Note what the measurement says about instruments: azureblob's bomb allocated
**0 MiB** of retained heap, because the streaming architecture worked exactly as
designed. A memory-based assertion would have called this safe. The cost is
7.8M records of downstream work bought with 1.8 MB of upload, so the honest
measure is records and wire-to-output ratio, not RSS.

Two identical transports, opposite outcomes, decided by a dependency neither
connector controls. That is not a security property; it is a coincidence that
currently holds. All three now disable the transport's compression, ask for gzip
deliberately where they want it, and meter what they get through one shared
`connectors/internal/decompress` — one implementation, because the whole finding
is that three copies drift.

**Versions bumped `behaviour-change` (ADR-0047 §6):** azureblob 0.4.0 → 0.5.0
(refuses what it previously accepted), s3 0.4.0 → 0.5.0 (a gzip object now reads
instead of failing at byte one).

**TC-025/TC-026 were product gaps; they now have a design.** ADR-0053 settles
it, and the shape it does NOT take is the point: a quarantine facility owned by
the platform was designed and rejected, because every version of it grows a
store, a retention policy and an eviction story — a queue holding customer
payload, in a system whose hub must never see payload and whose runners are
disposable. That is the Kafka lesson arriving by a different road.

Instead: verification is a **router whose predicate is a schema**. Rejects take
an ordinary edge to a sink the developer already owns, so retention, encryption
and access control are answered by a destination that already has them. The
platform stores nothing. Opt-in is structural — no node, no change.

The schema is **author-owned and pinned in the flow**, with connector
declarations seeding the editor rather than governing it: a schema supplied by
the source cannot detect that source changing, which is the event verification
exists to catch. Discovery is design-time only; at run time the compiled
validator is built with the plan, so there is no fetch and the per-record cost
is evaluation alone.

Still todo, and only closable once the node is built.

### 2b. ADR-specific invariants

| ID | Claim / ADR | Evidence today | Gap | Status |
|---|---|---|---|---|
| TC-009 | A batch from `Source.Next` is valid only until the next `Next`/`Close`; retaining requires `record.CopyValue` (ADR-0004, CLAUDE.md "Engine contracts to preserve") | `engine/batchtest` over `record.(*Batch).Poison()`, wired as `lifetime_test.go` in `engine/stream` and all five format readers (`ndjson`, `csvf`, `edi`, `xmlf`, `fixedw`) | **Closed 2026-08-09.** No production retainer found. The harness caught a defect in **itself** first: scribbling alone did not catch a deliberately-planted retainer, because a source reuses one batch, so the refill landed on the same arena and the stale pointer read the NEW record — plausible, wrong, and identical to the unpoisoned run. `Poison` now also releases the chunks, and that non-vacuity test is permanent. `docs/test-plan.md`'s ✅-on-code-review corrected | ✅ |
| TC-010 | Idempotency keys are **stable across re-dispatched attempts** (ADR-0002) | `hub/e2e/idempotency_test.go` (sink-visible, across a SIGKILL redispatch) + `hub/internal/store/idempotency_test.go` (both re-dispatch paths, byte-for-byte) | **Closed 2026-08-09.** No bug — the key is genuinely stable. See the closure note | ✅ |
| TC-011 | Admission is governed by real resource signals, **never fixed task-count caps** (ADR-0005) | `runner/internal/service/admission_test.go` — 40 tasks must be resident at a shared barrier simultaneously; peak residency asserted | **Closed 2026-08-09.** No bug. Non-vacuity: a 4-slot semaphore in the admission loop fails it with "admission is gating on a count, not on resources" | ✅ |
| TC-012 | Every API error carries the machine-readable envelope + version (ADR-0023) | `hub/internal/api/routesweep_test.go` — routes enumerated from `api.go` by `go/ast`, swept set asserted **==** registered set, 89 routes / 169 subtests | **Closed 2026-08-09. Found a real ADR-0023 gap — fixed:** every 401 answered `{"error":"unauthorized"}` (a string where the ADR specifies an object) as `text/plain` | ✅ |
| TC-013 | Test-mode capture is runner-only; the hub never sees payload (ADR-0014) | `hub/e2e/capture_test.go` — capture on, marker swept from every row of every hub table (raw, hex, base64), hub logs and runner output, with positive controls | **Closed 2026-08-09.** No payload reaches the hub. Ephemerality proven by ring eviction (capture 404s after 520 filler tasks) | ✅ |
| TC-014 | `runner/internal/task` ring store: bounded eviction, no aliasing, accurate totals | `runner/internal/task/task_test.go` — 13 behaviour-named tests, 86% → 98% and no longer incidental | **Closed 2026-08-09.** Found four defects; **two fixed** (in-flight gauge leak on eviction, terminal tasks re-counted), two recorded. See the closure note | ✅ |

### 2c. Process / gate integrity

| ID | Claim / ADR | Evidence today | Gap | Status |
|---|---|---|---|---|
| TC-015 | `check` depends on `cover`, **not** `test` (ADR-0022 §1 as written) | ADR-0022 amended 2026-08-09 with the reasoning; `Makefile` unchanged | **Closed — resolved in favour of the code.** `cover` sets `SHIFT_COVERAGE=1`, which skips the connector-subprocess and `hub/e2e` tests. Implemented as the ADR read, the gate would never have run the proofs of the doctrine (crash recovery, exactly-once, signed artifacts, secrets-never-at-rest, payload-never-to-hub). The double unit run is the price | ✅ |
| TC-016 | Every gated package meets a floor; floors only rise (ADR-0022 §1) | `coverage.thresholds` — `default 80`, with the reasoning recorded in-file | **Closed 2026-08-09.** Every existing package already carries an explicit floor, so the default governs exactly one thing: a NEW package, which used to land ungated and stay that way. A package that genuinely cannot reach 80 still may carry a lower floor — as an explicit line with a reason, not by default | ✅ |

---

## 3. Checked and adequate — do not re-litigate

Swept in pass #1 and found genuinely enforced. Listed so a later sweep skips
them; if one of these regresses, the row moves back into §2 with the date.

| Area / ADR | Why it clears |
|---|---|
| Starlark code step (ADR-0052, ADR-0017 tier 1) | Strongest suite in the repo. Off-by-default, sandbox escape, fuel exhaustion, recursion refusal, unbounded result, determinism, cross-record state leakage, payload-free errors, no config/secret reach, `print` sinkholed, deadline abandonment, panic containment, exact decimal edges/overflow, full script-visible surface pinned. The one absence is fuzzing the script text itself (⇒ TC-003) |
| Runner↔hub mutual TLS (ADR-0044) | mTLS-only refuses a bearer secret and refuses to *issue* one; identity persistence, reuse, unregistered-token path, cost, end-to-end |
| Secrets never at rest / never in logs (ADR-0010, ADR-0035) | `hub/e2e/TestSecretsNeverAtRest` + `TestNoLogKeyNamesACredentialOrPayload` (AST-level) + redactor unit tests + `TestSecretRedactedFromTaskError` |
| Exactly-once scheduling (ADR-0012) | `hub/e2e/TestScheduleFiresExactlyOnce` against real Postgres, plus `TestPassConcurrentExactlyOnce` and `TestConcurrentClaimExclusive` at the store layer |
| Crash recovery / lease integrity (ADR-0002, ADR-0009) | kill-9 e2e; `TestATaskCannotBeTouchedByARunnerThatDoesNotHoldItsLease`, `TestCheckpointFromAnExpiredLeaseIsRejected`, `TestWorkHandedOverAtTheInstantOfTimeoutIsNotLost`, `TestDeliveryAtMostOnceCapsAttempts` |
| Payload never touches the hub (ADR-0016, ADR-0038) | `TestWebhookIngressReportedAsMetadataOnly` asserts the distinctive payload appears in nothing the hub stored (capture path excepted ⇒ TC-013) |
| Operational logging (ADR-0046) | `pkg/shiftlog` conformance suite: gateway copy holds the same schema, gateway module stays dependency-free, every log call carries an `event`, canonical key spelling, nothing else writes stdout, no `log.Fatal` in binaries |
| Connector versioning, yank, retention, pins (ADR-0047) | Dedicated `retention_test.go` / `pins_test.go` / `eol_test.go` at both store and api layers; yank names the flows still pinned |
| Signed-artifact supply chain (ADR-0011, ADR-0018) | `hub/e2e/TestSignedArtifactPath` end-to-end incl. v2 descriptor; `consign.Verify` fuzzed for never-false-accept |
| Test tier / test-only shapes (ADR-0048) | `pkg/flowdoc/testonly_test.go` + test-marked dispatch |
| Rate limiting (ADR-0021) | `TestRateLimitRejectsFloods`, `TestRunnerRealmRateLimited`, `TestConcurrentAllowIsRaceClean` (both limiters) |
| Race + order dependence | `-race` always on; `make test` runs `-shuffle=on -count=1` (added 2026-07-21) |

## 4. Deliberately not tested (➖)

Nothing yet. Same discipline as the reasoned comments in `coverage.thresholds`:
when a row lands here it carries *why* a test would move a number rather than
establish a behaviour, so the decision is made once rather than annually.

## 5. Suggested order

Not a schedule — the order that buys the most per unit of work.

Rows 1-6 below are the original ordering, all now done. What remains is listed
after them, in the order agreed 2026-08-10.

1. ~~**TC-001** goroutine leaks~~ — **done 2026-08-09**; found a production leak on the first run, as expected
2. ~~**TC-009** batch-lifetime poisoning harness~~ — **done 2026-08-09**
3. ~~**TC-003** fuzz the format readers~~ — **done 2026-08-09**; found the `edi` memory bomb
4. ~~**TC-011** + **TC-010**~~ — **done 2026-08-09**; both claims now falsifiable
5. ~~**TC-008** fault injection~~ — **done 2026-08-09**; unblocked TC-012 and restored the hub/api floor
6. ~~**TC-005** generative topologies~~ — **done 2026-08-09**; found 5 DAG defects, one of them silent

Remaining, in agreed order:

7. **TC-029** — the join build side spills; the last thing between ADR-0029's central claim and it being true
8. **TC-017** — fuzz gateway ingress (the DMZ component) and starlark script text
9. **TC-022** — bound structural width; SOAP amplifies 24 KB to 885 MiB
10. **TC-018** — memoise `$ref` compilation; a 1 KB schema expands to 2^n nodes
11. **TC-031**, **TC-019** residual, **TC-021** — small and contained
12. **TC-002** metric-name conformance, **TC-024** encoding hostility, **TC-035** e2e load sensitivity
13. ~~**TC-030**, **TC-032**~~ — **decided and done 2026-08-10**
14. **TC-025**/**TC-026** — build ADR-0053, whose three open questions are now
    resolved (see the ADR's "Resolved" section)
