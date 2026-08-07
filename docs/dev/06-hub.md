# 06 — Hub: the durable control plane

> Code: `hub/`. Decisions: ADR-0002 (topology/durability), ADR-0009
> (control API), ADR-0010 (OIDC/tenancy/secrets), ADR-0011 (registry),
> ADR-0012 (scheduler). Status: M4a + M4b complete.

## What the hub is (and is not)

The hub owns **all durable state**: runner identity, users, flow versions
(+ publish state), the task queue, attempt history, schedules, secrets
(encrypted), the connector registry, audit. It never touches payload
data — flow documents and result summaries are control-plane metadata;
records stream only through runners and their connector subprocesses.
Secrets are control-plane config: the hub decrypts them transiently for
the runner resolve path and nothing else.

`hubd` itself is **stateless over Postgres**. Run N replicas behind any
LB: the queue coordinates claims via `FOR UPDATE SKIP LOCKED`, migrations
serialize on advisory lock 823400, scheduler passes on 823401. HA
Postgres is the HA story (per ADR-0002).

## Module layout

```
hub/
  cmd/hubd/            flags/env, migrate-at-boot, HTTP(S) serving, scheduler loop
  cmd/shift-bootstrap/ compose-bundle one-shot: certs+KEK ("certs"), seed ("seed")
  internal/store/      pgx pool + embedded migrations + all SQL
    migrations/00{01..11}_*.sql  schema (v5 core + 0006 direct_executions,
                                 0007 webhooks, 0008 connector descriptor,
                                 0009 audit account, 0010 usage_events,
                                 0011 connections)
    runners.go         registration tokens, runner identity, secret auth
    users.go           OIDC users (JIT upsert by issuer+subject), roles
    flows.go           flow upsert, monotonic versions, publish workflow
    queue.go           enqueue/claim/heartbeat/complete/fail + reaping (enqueueTx shared with FireDue)
    schedules.go       cron schedules + FireDue (the exactly-once core)
    secrets.go         envelope blob CRUD (no crypto here)
    connections.go     reusable connector config + published-flow usage lookup
    registry.go        publisher keys, content-addressed blobs, versions
    stats.go           dashboard overview counters
  internal/api/        HTTP handlers; admin(OIDC/break-glass) + runner realms
    auth.go            identity context, middlewares, /auth/* browser login
    secrets.go connections.go registry.go schedules.go dashboard.go ui.html
  internal/oidcauth/   go-oidc wrapper (Verify + code-flow Exchange) + oidctest fake IdP
  internal/kek/        KEK Provider interface + local key-file impl
  internal/secrets/    envelope service (DEK seal/open, KEK rotate)
  internal/scheduler/  tick loop + robfig/cron parser isolation (ParseCron/NextAfter)
  internal/pgtest/     test-only: SHIFT_TEST_PG or throwaway pg_ctl cluster
  e2e/                 crash recovery, exactly-once schedules, signed artifacts,
                       secrets-never-at-rest (real processes, real Postgres)
```

Dependency direction: `hub → pkg/flowdoc + pkg/consign → engine/record`
(path parsing for deploy-time validation only), plus vetted control-plane
deps `go-oidc` (JWKS validation) and `robfig/cron` (parser only). The hub
never imports `sdk`, `stream`, or anything that moves records.

## Schema v6 (migrations/0001–0011)

v1: `accounts` → `runner_registration_tokens` / `runners` (secrets stored
as SHA-256 only) → `flows` + `flow_versions` → `tasks` → `task_attempts`
→ `audit_log`. v2: `users` (OIDC identity = UNIQUE(issuer,subject), role
admin|viewer), `secrets` (ciphertext + wrapped_dek + kek_id; AAD binds
ciphertext to the secret name). v3: `flow_versions.status`
draft|published + `flows.published_version` — **version 0 resolves to the
published version everywhere**. v4: `schedules` (cron, next_fire_at,
last_* bookkeeping; UNIQUE(flow_id)). v5: `publisher_keys`,
`connector_blobs` (content-addressed by SHA-256, deduped),
`connectors`/`connector_versions` (Ed25519 signature + key ref, yankable).
v6 adds, incrementally: `connector_versions.descriptor` (ADR-0018),
`audit_log.account_id`, `direct_executions`, `webhooks`, `usage_events`,
and `connections` (reusable connector config, ADR-0034 — references only,
never a credential).

Task states: `queued → leased → completed | failed` (requeue loops
`leased → queued`). Partial indexes: the claim scan, idempotency
uniqueness, and `schedules_due (next_fire_at) WHERE enabled`.

## Tenancy

Every auth middleware calls `store.WithAccount(ctx, id)`; every store
query filters on it (`accountID(ctx)` falls back to the seed account for
unscoped callers like bootstrap). Runners carry their account from
`AuthRunner`; users from their row; break-glass maps to the default
account. Cross-account isolation is regression-tested (two-account store
test covering Claim/Tasks/GetTask/Runners/secrets).

## The lease lifecycle

```
enqueue      INSERT … ON CONFLICT (idempotency) DO NOTHING → existing id on replay
claim        reap expired ⭢ UPDATE oldest queued (same account) via SKIP LOCKED
             ⭢ attempt++, lease_expires_at = now()+TTL ⭢ task_attempts row
heartbeat    extend lease iff still held AND not expired (409 otherwise)
complete     terminal iff leased_by matches (zombie results → 409)
fail         requeue while attempt < max_attempts, else terminal
reap         at claim time AND every scheduler tick (crash visibility)
```

Both reap statements (terminal-fail and requeue) carry the
`attempt < max_attempts` guard: they are separate round-trips with independent
`now()`, so without the guard on the requeue path a lease expiring in the
window between them could exceed `max_attempts` by one (issue #12).

**Delivery policy (flow-level, ADR-0002).** A flow document's `delivery` field
sets its dispatch intent for hub-queued triggers: `at_least_once` (default —
re-dispatch a dead runner's task; idempotency keys dedupe side effects) or
`at_most_once` (non-idempotent flow — cap `max_attempts` at 1 so a lost runner
fails terminally rather than risk a double effect). The cap is applied at
enqueue (`effectiveMaxAttempts`, shared by `Enqueue` and `FireDue`) and a
trigger cannot raise it — the flow's safety intent wins. Webhooks are
runner-direct (ADR-0016), not hub-queued, so this policy does not apply to them.

Timing knobs (api.Options / hubd flags): `LeaseTTL` 30s default —
runners heartbeat at TTL/3. Long-poll `wait_seconds` capped at 30s.

## Scheduler (ADR-0012)

Every replica ticks (`-sched-interval`, 5s default); `store.FireDue` is
one transaction: try-advisory-lock 823401 (liveness) → `SELECT … FOR
UPDATE SKIP LOCKED` on due rows → per row, enqueue with idempotency key
`sched:<schedule_id>:<stored tick RFC3339 UTC>` and advance
`next_fire_at = NextAfter(cron, dbNow)` atomically. Postgres `now()` is
the only clock. Missed ticks fire once, then jump forward. Unpublished
flows and unparseable crons park with `last_error` instead of wedging
the pass. The `sched:` idempotency prefix is reserved (422 on user keys).
Proof: FireDue contention test (N schedules, 2 stores, exactly N tasks)
+ two-replica e2e with a mid-race shutdown.

## Auth model (M4b)

- **Admin realm**: OIDC bearer JWT (or the session cookie set by the
  `/auth/login → IdP → /auth/callback` browser flow — the cookie IS the
  verified ID token; stateless, HA-safe). Users JIT-provision on
  (issuer, subject); `viewer` role is read-only (non-GET → 403).
  Break-glass `SHIFT_HUB_ADMIN_TOKEN` (≥16 chars, constant-time) stays
  for bootstrap/automation — audited as `admin:break-glass`, warn-logged
  when OIDC is also on, unset it in production.
- **Runner realm**: a single-use registration token, then either a **client
  certificate** (ADR-0044, preferred) or the original per-runner bearer secret
  (SHA-256 only). See "Runner credentials" below.
- `adminOrRunner` guards the endpoints both need (artifact resolve/fetch,
  trusted keys).
- Unauthenticated: `/` (static dashboard page), `/healthz`, `/readyz`,
  `/metrics` (M6a), `/api/v1/authinfo` (which login methods exist — nothing
  more).

### Runner credentials: mTLS (ADR-0044)

`SHIFT_HUB_RUNNER_CA_CERT` / `_KEY` load the control-plane CA (`internal/runnerca`)
and the hub starts issuing runner client certificates:

| | |
|---|---|
| `POST /runners/register` `{token, name, csr}` | spends the single-use token, signs a certificate whose **subject is the runner id the hub just assigned**, returns cert + CA and **no secret** |
| `POST /runners/certificate` `{csr}` (runner realm) | renewal, authenticated by the current certificate — no operator token |
| `DELETE /runners/{id}` (admin) | decommission, which under mTLS **is** revocation |

Rules the issuer holds:

- **The CSR's subject is ignored.** Only its public key is used; the hub says
  who that makes you (ADR-0044 §5, ADR-0041 §3).
- **Proof of possession is checked** (`csr.CheckSignature`) — otherwise a valid
  registration token would let somebody have a certificate issued over another
  party's public key.
- **Client-auth only, no SANs, not a CA, ECDSA P-256+/Ed25519/RSA-2048+.** A
  runner dials and never serves.
- **24h default lifetime** (`-runner-cert-ttl`). Short lifetimes replace a CRL:
  a deleted runner's certificate stays cryptographically valid and its *name*
  stops resolving, so the next request is refused and the certificate ages out.

`SHIFT_HUB_RUNNER_AUTH` = `mtls` | `bearer` | `both` (default). `mtls` refuses
the weaker credential where it would be **issued** as well as where it would be
accepted, and `Handler` fails at startup if no CA is configured. `bearer` is
retained for deployments that terminate TLS at a proxy (ADR-0044 §4).

The listener uses `VerifyClientCertIfGiven`, not `Require` — it also serves the
dashboard. Still fail-closed for what matters: an invalid certificate aborts
the handshake, and only a **verified** chain populates `r.TLS.VerifiedChains`,
which is the only thing `authRunner` reads. Schema v7 (`0014`) makes
`secret_hash` nullable and records `cert_serial` / `cert_not_after` /
`cert_issued_at` for operations — never for authentication.

## Audit log (M6b)

`Store.Audit(actor, action, entity, detail)` appends an `audit_log` row;
every human/admin **mutating** endpoint calls it (deploy/publish/execute,
schedules, secrets + KEK rotate, connections, webhooks, publisher keys,
connector
publish, runner-token create, runner register). The `actor` comes from
`actor(r)` — `user:<email>`, `admin:break-glass`, or `runner:<id>`.

- **Read path:** `GET /api/v1/audit` (admin realm) — newest-first,
  account-scoped, keyset-paginated by descending id. Filters: `action`
  (exact, or a trailing-dot family like `secret.`), `actor`, `entity`,
  `before` (id cursor), `limit` (≤500). `?format=csv` streams a CSV export.
  Surfaced as the studio **Audit** window (+ Export CSV via authenticated
  fetch).
- **Account-scoped:** migration 0009 added `audit_log.account_id` (it was
  the one un-scoped control-plane table); `Audit` writes `accountID(ctx)`,
  `ListAudit` filters on it like every other list.
- **Deliberately not audited:** the runner task-lifecycle endpoints
  (`lease`, `heartbeat`, `complete`, `fail`, `reportExecution`). These are
  high-frequency **machine** actions (constant lease polling, heartbeats at
  TTL/3) already captured in the `task_attempts` history and the M6a
  metrics — auditing them is noise, not an audit trail.

## Secrets (ADR-0010)

`PUT /api/v1/secrets/{name} {"value":…}` seals under a fresh DEK
(AES-256-GCM, AAD = name), wraps the DEK with the active KEK
(`SHIFT_HUB_KEK_FILE`, 32 raw bytes, 0600 enforced). Flow docs reference
`{"$secret":"name"}`; deploy validates existence (422 with names);
**runners** resolve via `POST /api/v1/secrets/resolve` per task — the
value never lands in `tasks.document`, task reads, or logs (e2e-proven
with a sentinel). Every access = one `secret.access` audit row. KEK
rotation: new file active + old in `SHIFT_HUB_KEK_FILES_OLD` → restart →
`POST /api/v1/keys/rotate` (re-wraps DEKs only) → retire old file.
Without a KEK configured the secrets endpoints don't exist.

## Connections (ADR-0034)

A **connection** is a named, account-scoped, reusable connector config, so
a five-node SFTP flow carries the host and credential reference once
instead of five times — and rotating that credential is one edit rather
than one per node per flow.

`PUT /api/v1/connections/{name} {"connector":…,"config":{…}}` (admin),
plus `GET` (list/one) and `DELETE`. A node references one with
`"connection":"prod-sftp"` alongside its own operation config
(`flowdoc.Step.Connection` / `Endpoint.Connection`).

The config is **metadata, not payload**: it carries `{"$secret":"name"}`
references exactly as a flow document does, resolved runner-side, so the
hub stores a reference and never a credential. PUT validates those
references the same way a deploy does.

Three checks run at **deploy** (`checkConnectionRefs`), all from documents
the hub already holds — no descriptor is parsed, keeping ADR-0018's
"hub never reads the schema" intact:

1. every named connection exists (422, names listed);
2. the node's `connector` matches the connection's (422) — merging a
   `gen` node with an `sftp` connection would hand it fields it has no
   place for;
3. no node config key collides with one the connection supplies (422,
   `flowdoc.MergeConnectionConfig`). A collision is an **error, not an
   override**: last-write-wins is how one node ends up quietly pointing
   at a different host than its siblings, and it surfaces as a network
   fault rather than a config one. The comparison is top-level only, so a
   nested override cannot slip past as a non-collision.

`DELETE` **refuses with 409 `connection_in_use`** while any *published*
flow references it, naming them (ADR-0034 open question 1). Unpublished
drafts don't count — they can't be dispatched, so blocking on one would
make the connection undeletable for as long as a stale draft sat in
history.

Runners fetch what their task references via `POST
/api/v1/connections/resolve` (runner realm, documents only, unknown name
= 404 rather than a silent omission that would merge an empty config).
Connections are **not versioned** like flows: an edit takes effect
everywhere with no publish step, which is the point for rotation and is
ADR-0034 open question 3 if it ever bites.

## Connector registry (ADR-0011)

Publish: `POST /api/v1/publisher-keys` (base64 Ed25519 public key), then
`PUT /api/v1/connectors/{name}/versions/{version}?os&arch` with
`X-Shift-Publisher-Key` + `X-Shift-Signature` headers and the raw binary
as body (cap 128 MiB). The hub verifies the consign manifest signature
**before** storing — 403 stores nothing. Runners resolve
(`…/resolve?version=latest`) and fetch (`…/artifact`), verifying
key-trust + signature + digest fail-closed (see 04-runner.md). Blobs
live in Postgres, content-addressed, deduped; "latest" = newest publish.

**Marketplace (M6e).** On top of the signed registry:
- **Version history:** `GET /api/v1/connectors/{name}/versions` lists every
  published version (all os/arch, newest first, **including yanked** ones for
  provenance). `GET /api/v1/connectors` stays latest-per-name for the summary.
- **Yank / restore:** `POST /api/v1/connectors/{name}/versions/{version}/yank`
  (`{os,arch,yanked}`) withdraws a version — excluded from resolve/download
  **fail-closed**, still visible in history — or restores it. Admin, audited
  (`connector.yank`/`connector.unyank`).
- **Discovery metadata** (description/category/icon/tags) rides **in the signed
  descriptor** (`sdk.ConnectorMeta`, ADR-0018) — tamper-evident, and the hub
  still **never parses** it; the studio decodes it client-side to render the
  browse cards. A metadata-free descriptor stays byte-identical (v1 parity).
- **Publish tool:** `shift-consign publish` signs (v2 when a descriptor is
  bound, via `-descriptor` or `-describe`) and uploads in one step — the
  end-to-end path that previously lived only in an e2e test.
- **Studio:** the **Marketplace** window (searchable cards, categories, tags,
  per-connector version history + yank/restore).

**Capability policy (M5d, ADR-0015).** A per-deployment allow/deny list
(`SHIFT_HUB_CONNECTOR_ALLOW` / `SHIFT_HUB_CONNECTOR_DENY`) restricts which
connectors flows may use: a deploy referencing a disallowed connector is
rejected (422), and disallowed connectors are hidden from listing and
return 404 on resolve — cloud hubs hide dangerous connectors "not even
visible." Empty (self-hosted default) allows everything. Name-based and
hub-wide today; capability metadata and per-tenant scope are later adds.

## Connector pinning (ADR-0047 §1)

A flow that does not say which connector build it runs takes whatever is
newest **at the moment a task dispatches** — so publishing `sftp v0.3.0` used
to change behaviour for every flow using sftp, on the next task, against live
data. Nobody chose that; it is what "resolve latest" means once a registry
holds more than one version.

The resolution machinery was already right. It ran at the wrong moment. So it
moved:

| | Before | Now |
|---|---|---|
| Draft | resolves latest at dispatch | resolves latest at dispatch |
| Published | resolves latest at dispatch | runs the build it was published against |

`PublishFlow` rewrites the version's document, filling `version` on every
unpinned connector step, **in the same transaction as the status flip** —
published-but-unpinned is precisely the state this removes, and a crash
between two statements would leave a flow in it permanently.

Four rules fall out, each of which is a test:

- **Already-pinned steps are left alone.** Republishing an unchanged document
  is a no-op, not a silent upgrade, and a rollback keeps the builds its version
  was published against.
- **A yanked version or a revoked key never becomes a new pin** — pinning fails
  closed exactly as resolution does.
- **A connector the registry does not know stays unpinned rather than blocking
  the publish.** The registry is optional: a self-hosted deployment may
  provision binaries into the runner's directory and publish no artifacts at
  all, and refusing here would make it mandatory by accident. The
  `connector-pin` review check (ADR-0042 §7) raises a warning naming every
  step still resolving to newest, which is where a typo shows up.
- **A stored document that no longer parses is left exactly as it is.**
  Publishing worked before pinning existed and still does; `/graph` already
  answers 422 for it.

On the runner, the pin travels on the step and reaches `connstore.Ensure`, so
`ResolveConnector` asks for an exact version instead of `""`. The process pool
is keyed by **name AND version**: two published flows may legitimately pin
different builds of one connector, and keying by name alone would hand the
second flow whichever build the first happened to start — the same silent
substitution, moved from the registry into the pool. The local connector
directory ignores the pin, deliberately: that is the dev path where an
operator drops in a binary and vouches for it, and `SHIFT_REQUIRE_SIGNED`
removes it entirely.

## Connector currency (ADR-0047 §4/§5/§6)

Pinning froze what a flow runs; retention decided what may be deleted. This is
the third question: how does a flow ever move **forward**?

**Every version declares what kind of change it is** (§6, migration 0019).
`?compat=compatible|behaviour-change|breaking` at publish, plus optional
`X-Shift-Release-Notes`. The default is **`unknown`, never `compatible`** —
defaulting to compatible would make every version published before the column
existed, and every publisher who forgot, claim to be safe. The class is
deliberately **not signed**: a publisher's own declaration is weak evidence
whatever we do with it, which is why §8 backs it with a compatibility suite in
CI rather than by minting a v3 manifest format for a field only humans read.

**Age advises; it never refuses** (§5). `connector-currency.behind` rides on
the deploy and publish responses and folds the **whole span**, not the last
hop — *"gen is 3 versions behind … 1 BREAKING (3.0.0), 1 behaviour change
(2.0.0), 1 undeclared (1.5.0)"* — because somebody crossing three releases
wants to know what is in all of them. An undeclared version is named as
undeclared: it is not a safe one, it is one nobody said anything about.

These notices live in `hub/internal/api`, **not** in `flowdoc`'s check
registry, and the split is load-bearing. A flowdoc check answers a question the
document answers on its own and gives the same answer everywhere, which is what
lets the runner, the CLI and the hub agree. "Is this pin three releases old?"
is a fact about the *registry at this moment*: it changes without the flow
changing, and it differs on two hubs holding different artifacts. In flowdoc it
would be a check that needs a database, or one that silently does nothing
wherever there is no registry.

**Publishing forward drags a flow forward** (§4). A version being published may
not pin a build that has fallen outside the support window (current and n-1);
the refusal names the step, the version and how far behind it is. This is where
the version limitation actually lives — bounded by the last time somebody
edited the flow rather than by a calendar, landing at the one moment a
developer already has it open.

Two exemptions, both deliberate:

- **A rollback is never gated.** Publishing a version at or below the current
  published one is an emergency action, taken when the current version is
  misbehaving, and it deliberately keeps its original pins (§1). Refusing it on
  currency grounds would deny somebody the one thing they need at the worst
  possible moment.
- **An unplaceable pin is not a stale one.** A connector provisioned outside
  the registry, or a version since collected, cannot be ranked — and "unknown"
  must not read as "old" and block a publish.

## Connector end-of-life (ADR-0047 §7)

Retention cannot touch a version a live flow pins — that is exactly what makes
GC safe — which leaves one gap: a build that is genuinely poisoned (a CVE in a
dependency, a protocol flaw) and still in use. EOL is the deliberate act that
closes it, and it is **reserved for security**. The routine upgrade path is §4
and §5.

Announced, escalating, then loud:

1. `POST /api/v1/connectors/{name}/versions/{version}/eol` with a deadline, a
   **required** reason, and where to go instead. The response **names** the
   published flows that will stop, because the next step is telling those
   people. Posting an empty body withdraws it — declaring one is a human act on
   a live system, and humans mistype version numbers.
2. Until the deadline the version resolves exactly as before, while
   `connector-eol.scheduled` rides on every deploy and publish of a flow
   pinning it. Severity stays `warn` and the **wording** escalates: flowdoc has
   two severities on purpose, so urgency belongs in the sentence somebody
   reads, not in a level nobody can rank.
3. At the deadline it stops resolving. `ResolveConnector` returns `EOLError`
   and the API answers **410 Gone**, not 404 — the runner puts that message in
   the task result, and the person reading it has a flow that worked yesterday
   and no other clue. "Not found" would send them looking for a typo. The
   artifact download refuses too, so a manifest cached before the deadline is
   not a way around it.

**They fail; they are not silently upgraded.** Swapping a connector underneath
live customer data without anyone testing it is precisely the risk this ADR
exists to remove, and doing it automatically on a timer would be the same
mistake with a clock attached. Failing after weeks of escalating notice is
honest.

Publishing a flow whose pin is already dead is refused (422) **in both
directions** — including a rollback, unlike the currency gate in §4. Rolling
back to a connector that no longer resolves gives nobody a working flow, so
blocking it costs nothing and saying so now beats a task failure in an hour.

EOL is version-level, not per platform: yank is per `(os, arch)` because a bad
*build* can be platform-specific, but a poisoned dependency is a property of
the release, and an EOL that left one platform live would be an EOL that did
not happen.

## Connector retention (ADR-0047 §2/§3)

Pinning made a published flow immutable about which build it runs, which
raises the next question: when may a build ever be deleted? "Never" means
supporting v0.0.1 forever; "after N months" is a time bomb that breaks a flow
nobody edited. So retention is by **reference**, with a floor.

`flow_connector_pins` is the index (migration 0018), written from the pinned
document inside the publish transaction. It is derived state — the document
stays authoritative — which is the right way round: a disagreement is resolved
by re-reading the flow, never by trusting a table that drifted. Four features
read it and therefore cannot disagree about what "in use" means: yank warnings,
GC, EOL notices (§7) and bulk locate (§9).

**What is retained**

- The pins of each flow's **current published version and the one before it**.
  Counting every version ever published would keep everything forever — a
  superseded version keeps its `published` status on purpose, so it stays
  runnable — and counting only the current one would let a rollback land on a
  build that had been collected. `flow_versions.published_at` orders that
  history; a rollback republishes, so it moves to the front, which is correct.
- The newest **two versions of every connector**, referenced or not, so a
  rollback has somewhere to land.

**Two verbs, deliberately not conflated**

| | What it does | Effect on a running flow |
|---|---|---|
| `yank` | withdraws a version from **new** pins | none — existing pins keep resolving |
| collect | deletes the artifact | none by construction — it only takes what nothing references |

A yank response now carries `still_pinned_by`: the published flows still on
that version. Yanking does not stop them, which is what makes yank safe — but
the person yanking almost always believes it does, and reading it there beats
reading it in a support ticket.

`POST /api/v1/connectors/collect` **reports** and deletes nothing;
`?apply=1` deletes. The destructive-by-default version would have been a
mistake worth naming: publisher private keys are never server-side (ADR-0011),
so an artifact deleted by accident cannot be regenerated from anything the hub
holds. Blobs are content-addressed and shared, so bytes go only once no version
row points at them — deleting row and bytes together would destroy a build
another version deduped against.

## Direct executions (M5d-2, ADR-0016)

Push-triggered runs (webhook / direct API) execute entirely on a runner and
never enter the queue, so the runner reports their **metadata** afterwards —
`POST /api/v1/executions` (runner realm) → the `direct_executions` table
(account, runner, flow name, trigger, state, record counts, timing; **no
document, no payload**). `GET /api/v1/executions` (admin) lists them. This
gives the hub fleet load + history for work it never queued, without ever
touching payload.

## Usage metering (M6d)

The hub is **task control, not the account/billing platform** — that is a
separate, external system that does not exist yet. So the hub only *meters and
exports* usage; the future billing platform *pulls* it. `account_id` here is a
tenant key, never an account of record, and there is **no quota/plan enforcement
on the hub** (that belongs to the external platform).

`usage_events` (migration 0010) is an **append-only ledger**: one row per
terminal execution — queued tasks (`source='task'`) and direct/push runs
(`source='webhook'|'api'`). Each row is **metadata only** — `account_id`, `at`,
`source`, `flow_name`, `outcome`, `records_in`, `records_out`, `exec_seconds` —
never payload. It is written **inside the completion transaction** so a terminal
task always has its usage record: `finish()` (success), the terminal branch of
`Fail()` (a requeued attempt is not billed — it meters once it reaches a terminal
state), and `RecordDirectExecution` (atomic with the history row). Record counts
come from the runner's result (`parseResultMetrics`, best-effort → zeros on a
nil/malformed blob); `exec_seconds` from the start/finish timestamps. The ledger
is deliberately decoupled from `tasks`: the operational task row (and its large
`result` JSONB) may be pruned, but the usage record must survive, so metrics are
promoted to typed columns here rather than re-derived from `result`.

- **Read path:** `GET /api/v1/usage?since=&until=` (admin) — account-scoped
  rollup: totals, per-flow, daily series. RFC3339 bounds; default last 30 days.
  Surfaced as the studio **Usage** window.
- **Export pull:** `GET /api/v1/usage/events?since_id=&limit=` (admin) —
  cursor-based incremental pull the external billing platform ingests; `next` is
  the cursor for the following page (0 = caught up). `?format=csv` streams.
  A global/cross-tenant pull is future work (needs a system-scoped credential;
  the hub is not the account master).
- **Deferred:** quota/plan enforcement (external platform) and engine
  **bytes-processed** (the engine measures `ArenaBytes` but never reports it;
  adding it needs byte accounting in `stream.OpStats` threaded runner→hub — a
  hot-path change, its own task).

## Webhook config (M5d-2 s3, ADR-0016)

Webhooks are authored on the hub and synced to runners. Admin CRUD:
`PUT /api/v1/webhooks/{name}` (`{flow_name, token, enabled}` — the token is
accepted in plaintext and stored only as a **SHA-256 hash**), `GET`/`DELETE`.
Runner realm: `GET /api/v1/webhooks/sync` returns each enabled hook on a
**published** flow with its token hash + the flow document; the runner
replaces its local registry from this (the hub is authoritative for attached
runners). The runner hashes an incoming hook token the same way to verify —
one path for hub-synced and locally-registered hooks. The `webhooks` table
holds metadata only; the hook payload never reaches the hub.

## Studio: flow graph view (M5d-3)

`GET /api/v1/flows/{name}/graph` (admin) returns the published flow's render
graph — `flowdoc.GraphView()`: nodes (steps, with role main|handler) + typed
outcome edges (success / complete / failure). Data-free, like everything
hub-side. The dashboard renders it as an SVG in a modal (green success/
complete edges, red failure edges to the dead-letter handler), reachable
from each flow row's "Graph" button.

**Builder canvas (M5.5 Phase B+C, ADR-0019).** The modal is a full editor.

- *Model.* On open, the dashboard loads the document (not `GraphView`) and
  normalizes it to an editable **graph-form model** (`toModel`): linear-form
  docs are lowered client-side to the same `source→ops→sink` step chain the
  runner builds, so both forms edit identically. Topology renders from a
  locally computed `clientGraph(model)` so edits show instantly with no
  round-trip.
- *Layout (Phase B).* Node positions live in the document's presentational
  `layout` field (`flowdoc.Document.Layout`, step-id → `{x,y}`, ignored by
  validation/`Plan`/engine; stale/missing keys fall back to auto-layout).
  Nodes drag (pointer deltas, 1:1 unscaled SVG).
- *Editing (Phase C).* Add nodes (source/sink + the five transforms); delete
  nodes (edges referencing them are cleared); wire edges by dragging a node's
  happy (▸, right) or fail (▾, bottom) port onto another node — happy sets
  `onSuccess` (a per-node toggle switches to `onComplete`), fail sets
  `onFailure`; click an edge to remove it. A minimal inline editor edits each
  step's fields (typed fields for transforms).
- *Config forms (Phase D, ADR-0018).* On open the editor also loads
  `GET /api/v1/connectors` and decodes each connector's signed **descriptor**
  (base64 canonical bytes) into a catalog `{connector: {action: schema}}`.
  A connector step then gets a connector dropdown (catalog names + `@webhook`
  for sources) and an action dropdown (descriptor actions filtered by
  direction); the action's JSON Schema drives a typed config form — strings,
  numbers, booleans, enums, nested objects; `x-shift-secret` fields render a
  **secret picker** from `GET /api/v1/secrets` that inserts a
  `{"$secret":"name"}` ref. No schema (unpublished/v1 connector, `@webhook`,
  or none declared) falls back to a raw-JSON config editor.
- *Deploy.* "Deploy draft"/"Save layout" serialize the model with
  `cleanStep` (only type-relevant fields, dropping stale keys) and `PUT`
  `deployFlow` as a new draft. **`pkg/flowdoc` validation stays
  authoritative** — a 422 is surfaced inline (`#gerr`), never re-implemented
  client-side.

- *Design-time review (ADR-0042 §7).* `flowdoc.Review(doc)` returns advisory
  `Notice`s — warnings first, each namespaced under the check that produced
  it — for the facts a flow's SHAPE decides silently: no `@response` ⇒ this
  route answers 202 and not the result; no `input` ⇒ it accepts anything;
  `scope: records` ⇒ only the first record was verified; `@response` plus a
  blocking operator ⇒ the caller's connection is held for the whole run.
  Checks are **registered** (`flowdoc.RegisterCheck`, init-time; the registry
  freezes at the first `Review`), so a deployment can add its own without
  touching the shared model. Served three ways: on the `deployFlow` 201 and
  the `publishFlow` 200 bodies, `POST /api/v1/flows/review` for an unsaved
  canvas, `GET /api/v1/flows/{name}/review[?version=N]` for a stored one
  (defaulting to the **latest** version, not the published one — reviewing a
  draft is how you decide whether to publish it), plus
  `GET /api/v1/review-checks` for the registered set. The studio renders them
  as a panel under the toolbar, badges the node each is about, and shows them
  in a **confirmation before publish**. Notices never block: `Review` returns
  no error, and a flow that reviews with warnings deploys and publishes
  exactly as before.

**Windowed shell (Phase E, ADR-0019).** The dashboard is an "OS-lite" desktop:
a left **dock** of tools, each toggling a draggable/resizable/closable
**window** (`initShell` + a small window manager). One singleton window per
tool — Overview, Flows, Builder, Tasks, Executions, Runners, Connectors,
Secrets — each wrapping the original render target by its unchanged id, so
`refresh()` and the render functions are untouched. The builder is now a
**window** (not a modal), so the canvas and the Tasks list sit side by side.
Window open/position/size/z-order is **browser-only** state
(`localStorage`, never sent to the hub — it is not flow data); first run
opens Overview+Flows+Tasks. Flows rows carry a **version picker + Publish**
(publish or roll back to any version 1..latest via
`POST .../versions/{v}/publish`) and Run. The test-run capture overlay
(studio→runner read, ADR-0014) remains deferred.

## Dashboard

`GET /` serves the embedded single-file page (runner pattern): overview
strip (`/api/v1/stats`), flows with publish buttons + schedule editor,
recent tasks with attempt/telemetry drill-down, runners, registry,
secret names. Login via OIDC redirect or break-glass token prompt
(sessionStorage). The page is static; every data call is authenticated.

## Management windows

Everything the hub owns is now administrable from the shell rather than only
from `curl`. Four surfaces, all plain tables plus one inline form — this is
administration, not a canvas:

| Window | Covers |
|---|---|
| **Connections** | reusable connector config (ADR-0034); credentials stay `{"$secret":…}` references |
| **Gateways** | register (one-time install token), adoption state, **config drift**, certificate expiry, re-adopt, trusted proxies (ADR-0049) |
| **Routes** | public path → flow, caller token, runner selector, source allowlist, body cap (ADR-0038 §5) |
| **Runners** | hub-asserted labels, certificate expiry, revoke, registration tokens (ADR-0041/0044) |

Three details are load-bearing rather than cosmetic:

- **One-time credentials get a surface, not an `alert()`.** A registration
  token, a gateway install token and a route's caller token are each served
  exactly once; a dismissed dialog is an unrecoverable loss of the only copy.
  They render into `#once-host` with a copy button and stay until dismissed.
- **Gateway drift is the number to read.** `config_version` is what the hub
  intends, `pushed_version` is what the gateway last acknowledged, and the two
  differ exactly while a push is outstanding or failing.
- **Certificate expiry is shown in days.** An expired runner certificate looks
  identical to a runner that simply stopped polling, so the window that lists
  runners is the right place to distinguish them.

`store.Runner` grew `labels` and `cert_not_after` for this: a manager that
writes a value it cannot read back is a guess with a button on it.

**Marketplace** gained the retention half of ADR-0047: **Used by** lists the
published flows pinning a version (and whether each is running now or is a
rollback target), and **Housekeeping** reports unreferenced builds before
deleting any.

**Builder** now shows what a step actually runs: a **connection** picker
(filtered to connections for that connector — one for another connector is
config that cannot apply) and the **pinned version**, read-only with an unpin
action. The pin is recorded by publishing, never typed: a free-text version
would be a claim about an artifact nobody checked, and the registry is the only
thing that knows which builds exist.

The serialiser fix matters more than the pickers. `cleanStep` was dropping
`connection` — so opening a flow in the builder and redeploying it silently
stripped its host and credentials, moving the step back to inline config it did
not have. `version` would have gone the same way. `TestBuilderSerialiserCarries
EveryStoredField` now guards the whole set, because this is not an editing bug
class, it is silent data loss that reads as "the flow changed behaviour".

## Testing

`pgtest.DSN(t)` gives every test a fresh database: against
`SHIFT_TEST_PG` when set, otherwise a private `initdb`/`pg_ctl` cluster.
Store tests cover queue semantics, publish resolution, schedule firing
(incl. two-store contention), registry, secrets CRUD, tenancy isolation.
`oidcauth` tests run against an in-process fake IdP (runtime-generated
RSA key; mints expired/wrong-aud/wrong-iss/foreign-key tokens). API
tests cover realm matrices, viewer read-only, secret leak-freedom.
`e2e/` proves the milestone exits with real binaries and real Postgres:
kill -9 crash recovery, two-replica exactly-once schedules, the signed
artifact supply chain (incl. DB-tamper fail-closed), and
secrets-never-at-rest.

Failure-mode / load tests (issue #10): `TestStatementTimeoutFires` proves the
#8 `statement_timeout` actually cancels a runaway query (not just config);
`TestConcurrentClaimExclusive` hammers the queue with concurrent enqueues + 4
runners claiming in parallel and asserts `FOR UPDATE SKIP LOCKED` hands each
task to exactly one runner (none lost, none double-claimed).

## Operating it

```
docker compose -f deploy/compose.dev.yml up -d       # dev Postgres
head -c 32 /dev/urandom > kek.bin && chmod 600 kek.bin
SHIFT_HUB_ADMIN_TOKEN=$(openssl rand -hex 16) \
  bin/hubd -db postgres://shift:shift-dev-only@localhost:5432/shift \
           -kek-file kek.bin \
           [-oidc-issuer https://idp… -oidc-client-id shift-hub \
            -oidc-redirect-url https://hub…/auth/callback]
```

Deploy = `PUT /api/v1/flows/{name}`; **publish** =
`POST /api/v1/flows/{name}/versions/{v}/publish` (new deploys are
drafts); execute = `POST …/execute` (version 0 = published; 409 if none);
schedule = `PUT …/schedule {"cron":"*/5 * * * *"}`; observe =
`GET /api/v1/tasks/{id}` or the dashboard on :8400.

The full self-contained stack — Postgres, Dex, TLS certgen, seeded
registry, signed-only runner — is `make up` (see `deploy/README.md`).

## Postgres operational hardening (issue #8)

The hub is the availability keystone (ADR-0002), so the pool is bounded:

- **Timeouts** — every connection sets `statement_timeout` (default `30s`,
  `SHIFT_HUB_STMT_TIMEOUT`) and `lock_timeout` (default `5s`,
  `SHIFT_HUB_LOCK_TIMEOUT`), so a pathological query or lock wait can't hang a
  connection indefinitely. An explicit value in the DSN is not overridden.
  Caveat: a very large migration (e.g. a big index build) may exceed
  `statement_timeout` — raise it for that deploy.
- **Serialization / deadlock retry** — the queue runs at the default READ
  COMMITTED isolation with `FOR UPDATE SKIP LOCKED`, so serialization failures
  (`40001`) don't arise and deadlocks (`40P01`) are very unlikely (claimers skip
  locked rows rather than block). The scheduler tick already re-runs on the next
  interval if a pass errors, so a transient deadlock self-heals. Policy: if
  `40001`/`40P01` are ever observed under load, wrap the affected transaction in
  a bounded retry (a few attempts with backoff) — do not raise the isolation
  level platform-wide.
- **Backups / restore** — operators run regular base backups + WAL archiving
  (out of scope for the app; a deployment concern). **A backup is not proven
  until a restore has been tested** — the ops runbook must include a periodic
  restore drill. (Runbook itself: issue #9.)
- **Migrations are forward-only + advisory-locked.** For an *unsafe* schema
  change use **expand/contract** across releases, never a blocking rewrite in
  one step: release N adds the new shape (nullable column / new table) and
  backfills; release N+1, once nothing reads the old shape, drops it. This keeps
  each migration fast and lets a rolling deploy run old and new code at once.
