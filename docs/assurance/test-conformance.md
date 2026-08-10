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
| TC-005 | **Any** validated topology executes — nested fan-out, mixed fan-out/fan-in included (ADR-0029) | `runner/internal/service/topology_gen_test.go` — 32 generated valid DAGs from a fixed seed, asserting exact record conservation | **Closed 2026-08-09 — and the claim is FALSE.** Three real executor bugs found (TC-027, TC-028, TC-029). The generator suppresses the two broken shapes so the suite is green; remove the suppressions when they are fixed | ✅ |
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
| TC-017 | The remaining untrusted-input surfaces are fuzzed (ADR-0022 §5) | TC-003 closed the format readers, `record.ParsePath` and `engine/schema` | Still unfuzzed: **gateway ingress** route/header/auth parsing — the one component in a DMZ, so its parser eats bytes straight off the public internet — and **starlark script text** (ADR-0052; scripts are authenticated but the compiler is not hardened against them). Raised while closing TC-003 | ⬜ |
| TC-018 | Schema compilation is bounded (ADR-0042) | None | `schema.Compile` inlines each `$ref` by recompiling its target per reference site, and its cycle guard (`c.resolving`) only blocks cycles on the *current stack*. A diamond of `$defs`, each level referencing the next twice, compiles in 2^n nodes from ~1 KB of schema text. Authenticated-author-only today, so not urgent — but it is a compile-time bomb the moment schema text arrives from anywhere less trusted, and ADR-0042 §4 leans on Compile being cheap. Raised while closing TC-003 | ⬜ |

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

- **405 Method Not Allowed is router-generated `text/plain`**, outside the
  envelope. Mux-owned surface; needs a decision rather than a patch (⇒ TC-030).
- **A runner gets 401 when the hub's DATABASE is down** — `authRunner` is a DB
  round-trip and any error becomes an opaque 401. Checked the consequence
  rather than assuming it: `hubclient` does **not** treat 401 as terminal, and
  `leaseloop` retries with backoff, so this is a **diagnosability** bug, not an
  availability one — an operator is sent hunting a credential problem that does
  not exist. A 503 would say the true thing (⇒ TC-031).
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

**Reported, not fixed:** S3 `..` reaches the request line unescaped
(`/b/../../etc/passwd`). Real S3 treats keys opaquely, but a *normalising
reverse proxy* in front of an S3-compatible endpoint could resolve it. Rejecting
`..` would reject technically legal keys ⇒ TC-032.

### 2f. DAG executor defects (opened 2026-08-09 by TC-005)

ADR-0029's central claim — *any validated topology executes* — is **not true
today**. The generative test found three ways it fails. All three are in
`runner/internal/service/dag.go`; none is in validation, so `pkg/flowdoc`
accepts every one of these flows and the runner then fails or hangs on them.

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
| TC-029 | The enrichment shape **deadlocks permanently** above ~10 batches. The join blocks building its right side while the probe branch backs up into a 4-deep pipe behind a 4-deep tee queue, so the tee can never finish feeding the build side | `src → tee → [probe, build] → join → sink`: completes at 5k records, **hangs forever** at 12k and 50k | **Worst of the three.** `TaskTimeout` defaults to 0, so the task stays `running` indefinitely **holding its admission reservation** — a permanent resource leak on the runner (ADR-0005) |

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
| TC-019 | **Every reader bounds a single unit.** One record/line/field/segment cannot be unbounded | `ndjson` MaxLineBytes + MaxDepth ✅ · `xmlf` MaxDepth ✅ · `edi` MaxSegmentBytes ✅ (fixed under TC-003) · **`csvf` has NO size bound — measured**: an unclosed quoted field buffered 64 MiB and failed only at column 67108866, i.e. when the *input ran out*, not because of any limit. Against an endless stream it grows without limit · `fixedw` bounded by its layout, but `SkipLines`/`Unseparated` unaudited | ⬜ |
| TC-020 | **Decompression is bounded.** A compression bomb cannot exhaust the runner | No explicit decompression anywhere — but Go's `http.Transport` transparently inflates `Content-Encoding: gzip`, so `httpconn` HAS an unbounded decompression path it never opted into. Per-line bounds limit the damage for ndjson; nothing bounds total inflated bytes or duration | ⬜ |
| TC-021 | **A stream that never ends is bounded** — total bytes, total records, wall clock. A source that trickles forever must not pin a runner slot indefinitely | Unaudited | ⬜ |
| TC-022 | **Structural depth/width cannot exhaust the stack or the arena** — deep nesting, a record with a million fields, XML entity expansion (billion laughs), pathological schemas | `MaxDepth` on `ndjson`/`xmlf` only. TC-018 (the `$ref` 2^n expansion) is an instance of this class | ⬜ |
| TC-023 | **Path/name handling from a remote listing is safe** — `../`, absolute paths, symlinks, NUL/control characters, zip-slip | `hostilenames_test.go` in all five file/object connectors | **Closed 2026-08-09. Found an FTP command injection (CWE-93) and zip-slip in two listings — all fixed.** See the closure note | ✅ |
| TC-024 | **Encoding hostility degrades, never corrupts** — invalid UTF-8, mixed encodings, BOMs, NUL bytes, lone surrogates. The record model must not silently produce mojibake a customer later trusts | Partly covered by the TC-003 fuzz seeds; no explicit assertion about what a customer *receives* | ⬜ |
| TC-025 | **A single bad record does not destroy the run.** The customer's stated need: "recover/ignore invalid data" | **ADR-0053 written 2026-08-09** — `@verify` is a router whose predicate is a schema; rejects route down an ordinary edge to a destination the developer owns | Designed, not built. No test can close this until the node exists | ⬜ |
| TC-026 | **The customer is told what was rejected and why.** Assurance that delivered data was valid | **ADR-0053 §6** — per-step counts and rejection reasons (field path + failed rule). Field names and rule ids are metadata; values never appear, so it rides the execution report without touching the two-plane split | Designed, not built | ⬜ |

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
| TC-009 | A batch from `Source.Next` is valid only until the next `Next`/`Close`; retaining requires `record.CopyValue` (ADR-0004, CLAUDE.md "Engine contracts to preserve") | Prose comments — `engine/stream/pipe_test.go:20`, `engine/format/fixedw/fixedw_test.go:42`. `docs/test-plan.md` marks this ✅ with evidence *"code review + lint (depguard)"* | **The one contract whose violation reintroduces v0's failure mode, held up by convention.** Needed: a shared harness that scribbles/poisons the batch after `Next`, run over *every* format reader and *every* operator. Catches a retaining operator mechanically. Correct the ✅ in `test-plan.md` to 🟡 when this lands as todo | ⬜ |
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

1. ~~**TC-001** goroutine leaks~~ — **done 2026-08-09**; found a production leak on the first run, as expected
2. **TC-009** batch-lifetime poisoning harness — highest doctrine risk, and it downgrades a false ✅
3. **TC-003** fuzz the format readers — cheap, and `edi`/`fixedw` are externally-fed
4. **TC-002** metric-name conformance — mechanical, mirrors an existing suite
5. **TC-011** + **TC-010** — the two doctrine claims currently unfalsifiable
6. **TC-008** fault injection — unblocks TC-012 and the deferred hub/api floor
7. Remainder
