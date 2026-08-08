# ADR-0047: Connector versions are pinned at publish and retained by reference

Date: 2026-08-06

Status: **BUILT** (§1–§9). Extends ADR-0011 (signed registry) and
ADR-0013/0029 (flow model). Consumes the review registry from ADR-0042 §7.

Built so far: resolution moved from dispatch to publish (§1) — `flowdoc` steps
carry a `version`, `PublishFlow` fills it in transactionally, the runner
resolves an exact build and pools processes per (name, version), and the
`connector-pin` check reports anything still resolving to newest.
Reference-counted retention (§2) and the yank/collect split (§3) — the
`flow_connector_pins` index, `ConnectorReferences`, report-by-default
collection, and a yank response that names the flows still pinned. Currency
(§4/§5/§6) — a per-version compatibility class, span-folding notices that
advise rather than refuse, and a publish gate that drags a flow forward without
ever blocking a rollback. End-of-life (§7) — a required reason, a named list
of the flows that will stop, escalating notices, and a 410 that says what
happened rather than a 404 that reads as a typo. Bulk upgrade (§9) — locate,
stage-and-test on the test tier, publish-all, with the batch durable across all
three so the drafts that were tested are the ones that ship and a release
landing mid-batch cannot retarget it. The compatibility gate (§8) — `sdk/compat`
diffs a connector's action surface against the shape it last released and
`sdktest.CheckSurface` fails the build when the declared class cannot support
it, so the class every other section reads is checked rather than promised.
See `docs/dev/06-hub.md` and `docs/dev/03-connector-protocol.md`.

## Context

Today a flow does not say which connector build it runs. `flowdoc.Step` has no
version field, and the runner's `connstore` resolves with an empty version
string, which the hub's `ResolveConnector` reads as *newest publish*. The
consequence is exact and unpleasant:

> Publishing `sftp v0.3.0` silently changes behaviour for **every flow using
> sftp**, on the next task, against live customer data.

Nobody chose that. It is what "resolve latest" means once a registry has more
than one version in it. The resolution machinery is already correct — it just
runs at the wrong moment. It runs when the task dispatches, so the answer can
change between two runs of a flow nobody edited.

The obvious fix — pin the version into the flow — has an equally obvious
failure mode, which Aaron named immediately:

> If a customer doesn't update for several months they could skip a version so
> pinned + multiple updates mean impossible flows to run…

A naive pin plus a support window is a time bomb: the flow was fine on Tuesday
and refuses to start in March, because a policy expired underneath it. That is
worse than the problem it fixes. But the opposite extreme is not acceptable
either:

> We need some version limitations - certainly don't want to support v 0.0.1
> forever.

So the decision has to satisfy three things at once: a published flow keeps
running, old versions do not accumulate forever, and a genuinely dangerous
version can still be killed.

## Decision

### 1. Resolution moves from dispatch to publish

When a flow version is published, every connector step resolves to a concrete
version, and that version is stored in the flow version. The runner then
resolves an exact version, never "latest".

A published flow is immutable in this respect: it runs the builds it was
published against, for as long as it exists. Upgrading a connector is an
explicit republish with a visible diff, not a side effect of somebody else's
release.

Draft flows still resolve latest — a draft has no promise to keep.

### 2. Retention is reference-counted, not eternal

A connector version is retained while any **published** flow version pins it.
Once nothing references it, it becomes collectable.

Two floors on top of that:

- The latest version and the one before it (`n-1`) are always retained, even
  when unreferenced, so a rollback has something to roll back to.
- Retention is per platform tuple (`name`, `version`, `os`, `arch`), matching
  the registry's existing key.

This is what makes "we don't support v0.0.1 forever" true without breaking
anything: v0.0.1 survives exactly as long as some published flow still names
it, and disappears the moment the last one is republished.

### 3. `yank` is a selection rule; GC is the deletion

They are different verbs and must not be conflated:

- **`yank`** — "do not select this version for new pins". Existing pins keep
  resolving. This is the current `SetConnectorYanked` behaviour, and it is
  correct; what changes is that yanking a version a published flow depends on
  must warn and name those flows.
- **GC** — actual removal of the artifact. Only ever touches unreferenced
  versions above the floor in §2. It cannot break a published flow because,
  by definition, no published flow points at what it removes.

### 4. Publish drags you forward

Publishing a flow version must pin a connector at or above `n-0.1` — the
current version or the one before it. You cannot publish a flow that pins a
build three releases old.

This is where the version limitation actually lives. Drift is bounded not by a
calendar but by the last time somebody edited the flow, and the forced upgrade
lands at the exact moment a developer is already in the builder with the flow
open and the test tier available (ADR-0048). That is the cheapest possible
moment to absorb it.

### 5. The support window is a patch policy, not an execution gate

`n-0.1` describes what we fix, not what we run. A flow pinning a version
outside the window keeps executing; it simply will not receive fixes.

Age produces a **notice**, never a refusal — an advisory `Notice` in the
ADR-0042 §7 registry, surfaced on deploy and publish where a human reads it:

> this flow pins `sftp v0.2.0`; current is `v0.5.0`, supported window is
> `v0.4.0+`

Refusing to run on grounds of age would be an arbitrary limit, which the
platform doctrine rejects for the same reason it rejects task-count caps.

### 6. Every version declares a compatibility class

Retention solves execution but not the **upgrade diff**. A customer going
`v0.2.0` → `v0.5.0` crosses three releases they never read. So a connector
publish declares, per version, one of:

- `compatible` — additive or internal; no config or output change.
- `behaviour-change` — same config, different results (a default changed, a
  field is now typed).
- `breaking` — config or output shape changed; a flow needs editing.

The upgrade notice folds the whole span rather than the last hop: *"3 versions
behind; 1 behaviour change in v0.4.0"*. Publishers declaring their own class is
weak on its own, so it is backed by §8.

### 7. End-of-life is manual, announced, and fails loudly

GC cannot remove a referenced version, which leaves one gap: a version that is
genuinely poisoned — a CVE in a dependency, a protocol flaw — and is still
pinned by live flows. That needs a deliberate act, not an automatic one.

Marking a version EOL sets a deadline. From that moment the review registry
raises a notice on every flow pinning it, escalating as the deadline nears, and
the hub dashboard shows the affected flow list. At the deadline the version
stops resolving and pinned tasks **fail** with an error naming the EOL and the
target version.

They fail; they are not silently upgraded. Swapping a connector underneath live
customer data without anyone testing it is precisely the risk this whole ADR
exists to remove, and doing it automatically at a deadline would be the same
mistake with a timer attached. Failing after weeks of escalating notice is
honest.

EOL is reserved for security. It is not the routine upgrade path.

### 8. Backward compatibility becomes a gate, not a discipline

`sdktest` already harnesses the connector wire protocol in-process. A
compatibility suite runs the current SDK against the previous connector
version's recorded action surface — config schema, action names, directions,
output shape — and fails the connector's build when something changes without
the declared class to match. "Discipline on our side" becomes CI.

### 9. Bulk upgrade is three steps, never one button

Republishing flows one at a time after every connector release is the friction
that makes people stop upgrading. So the hub offers a bulk path — but staged,
because a mass republish is a mass change against live data:

1. **Locate** — report every flow pinning connector `X` below the target, with
   the folded compatibility diff per flow.
2. **Test** — run them on the test tier (ADR-0048).
3. **Publish-all** — republish forward as one audited batch, with the per-flow
   review notices attached to the audit record.

The report and the test run are gates, not decoration. A single button that
skipped to step 3 would reintroduce exactly the silent-change problem this ADR
removes.

## Consequences

- Flow versions grow a resolved-connector set; the runner's resolve call takes
  a concrete version. Task dispatch stops being a place where behaviour can
  change.
- The registry needs a reference query (which published flow versions pin this
  connector version) to drive GC, yank warnings, EOL notices and bulk locate.
  All four read the same index.
- Storage grows with referenced versions rather than with all versions ever
  published. Signed artifacts are small; a runnable-flow guarantee is not.
- **What this does not solve:** retention guarantees *we* keep serving a build.
  It cannot keep it working. An SFTP server dropping an old cipher, or an API
  retiring `v1`, breaks an old connector regardless of our policy. Pinning
  narrows the blast radius to changes we control; it does not stop the world
  from moving.
