# ADR-0028: On-prem/hybrid connectors + runner-as-secure-agent threat model

Date: 2026-07-28
Status: **Designed (build deferred)** — sequenced as its own on-prem/hybrid wave

## Context

SHIFT is hub-and-spoke: an HA hub (control plane, cloud or local) and
stateless **runners** (the spokes) that lease work, execute streams, and touch
payload (ADR-0002, ADR-0016). The two planes are already separated — the hub
sees only metadata, payload lives with the runner (ADR-0016), secrets resolve
runner-side and never enter the queue/logs (ADR-0010).

That topology is exactly what a cloud-only iPaaS (Workato, most of Boomi's
cloud runtime) **cannot** do: reach a resource that lives on a private network
with no inbound route from the internet. The differentiator we can ship is
**deploy the runner inside the customer datacenter**. The hub stays cloud
(design studio, durable queue, registry, identity); a runner runs behind the
customer firewall, on the same LAN as Active Directory, an on-prem Exchange
server, file servers, and administrative hosts. It leases work outbound over
HTTPS long-poll (no inbound firewall hole — the control-plane pull model was
chosen partly for this), reaches internal systems on the local network, and the
**payload never leaves the building**. Data residency by topology, not policy.

Realising this needs a family of **on-prem connectors** that are, by their
nature, more dangerous than an HTTP GET: they authenticate to a directory, send
mail as the organisation, and — the sharp end — **run commands on hosts**.
ADR-0015 already established that a shared/cloud hub must not merely disable but
**hide** host-reaching connectors (filesystem, process): they are invisible on
a cloud deployment and allow-listed only where an operator has accepted the
risk. This ADR extends that "dangerous set" to a concrete on-prem connector
family and — because some of these connectors execute arbitrary commands — pins
down the **threat model** and the gates that contain it. The security model is
the heart of this decision; the connector catalogue is secondary to it.

Constraints carried in unchanged: connectors are out-of-process subprocesses,
never linked into the runner, spawned only after signature verification
(ADR-0001, ADR-0011); the streaming record contract holds (ADR-0004);
credentials arrive already-resolved as plaintext from the runner's
`{"$secret":...}` resolution and are tagged, never logged (ADR-0010); the SSH
foundation, host-key verification and network guard already exist in
`connectors/internal/sftpconn`, and the SSRF/network guard pattern in
`connectors/internal/httpconn`.

## Decision

### 1. The runner is a secure agent inside the customer network

The on-prem runner is positioned and documented as a **secure agent**: an
outbound-only spoke that the hub can task but never push to. Its trust
properties are the ADRs it already satisfies —

- **Outbound-only control plane.** The runner *pulls* leases, config, and
  secret material over HTTPS long-poll (ADR-0009/0016). No inbound port from
  the hub, no inbound port from the internet. The customer opens **no**
  firewall ingress for SHIFT to function; a webhook ingress (ADR-0016) is a
  separate, explicit opt-in.
- **Payload stays on-prem.** The data plane is runner↔source/sink only
  (ADR-0016). AD attributes, mailbox contents, command output — none of it
  transits the hub. The hub receives execution *metadata* (step ids, outcome,
  telemetry, error tags), never bytes.
- **Secrets resolve at the edge.** Directory bind passwords, service-account
  credentials, SSH keys are `{"$secret":...}` refs resolved by *this* runner
  just before execution (ADR-0010). The plaintext exists only in runner and
  connector process memory, never in the hub queue, task rows, or logs.

This is the whole pitch: a cloud control plane driving on-prem systems with the
data never leaving the customer's walls. It is a **topology** guarantee, which
is stronger than a policy promise.

### 2. The connector catalogue (this wave)

Five connectors, in two risk tiers. All follow the ADR-0024 one-node/verb-
dropdown model, declare per-action JSON Schema descriptors for schema-driven
config forms (ADR-0018), tag secret fields with `x-shift-secret`, and reuse the
network guard + host-key verification foundations from `sftpconn`/`httpconn`.

**Tier A — directory / messaging (authenticated, non-arbitrary-code):**

- **`ldap`** — Active Directory (and any LDAP v3 directory). Verbs:
  `search` (source, paged), `add`/`modify`/`delete` (config-driven sources
  emitting a status record, per ADR-0024), plus the AD-idiomatic composite
  verbs `enable`/`disable` (userAccountControl bit flip), `reset-password`
  (`unicodePwd` write), and `add-to-group`/`remove-from-group` (membership).
  **LDAPS or StartTLS is mandatory for any write or password verb** — AD sends
  password changes only over a protected channel anyway, and we refuse to bind
  credentials or push writes over cleartext LDAP (fail closed; `allow_local`
  gates plaintext only for a loopback dev directory, mirroring `sftpconn`).
  Schema discovery: an optional `describe` step reads the directory's subschema
  subentry so the studio can offer attribute names.
- **`exchange`** — on-prem Exchange via **EWS** (SOAP). Verbs: `send-mail`
  (sink), and `list`/`get`/`create`/`update` over mailbox items, calendar, and
  contacts (source / config-driven). Auth: **NTLM or Kerberos** (integrated,
  the on-prem norm) or **Basic** over TLS; Kerberos/NTLM keeps the service
  account password off the wire. Autodiscover optional; explicit EWS URL
  supported. (Exchange *Online* is a separate cloud/Graph connector, out of
  scope here — this is the on-prem SOAP story.)

**Tier B — command execution (the dangerous set; arbitrary code by design):**

- **`exec`** — run a **local** command on the runner host. Argument-**array**
  only (`argv`, never a command string); binary must be on a configured
  **allowlist**; a **working-directory jail**; a mandatory **timeout** and
  **output-size cap**. No shell by default.
- **`ssh-exec`** — run a command on a **remote** host over SSH, reusing the
  `sftpconn` SSH foundation (same dial → network guard → **mandatory host-key
  verification** → auth). Same argv/allowlist/timeout/output-cap discipline as
  `exec`. This is the cross-platform "run a script on that server" verb.
- **`winrm`** — run **PowerShell** on a remote Windows host over WinRM
  (HTTPS/negotiate). Same discipline. It **doubles as the Exchange Management
  Shell path** for on-prem Exchange administration that EWS does not cover
  (e.g. `New-Mailbox`, transport rules) via a constrained/JEA endpoint.

Tier B connectors emit their captured stdout/stderr/exit-code as records into
the stream (subject to the output cap); they are ordinary streaming connectors
whose *side effect* is running code.

### 3. Capability policy: the dangerous set is DEFAULT-DENY

ADR-0015's per-deployment `connpolicy` is the containment layer, extended:

- **`exec`, `ssh-exec`, `winrm` join `filesystem` in the "dangerous set".** On
  a shared/cloud hub they are **denied and invisible** — filtered from `list`,
  rejected at `PUT /flows` (422), 404 on `resolve` — exactly as ADR-0015
  specifies. A tenant on a shared runner cannot see, deploy, or resolve them.
- **They are enabled only per on-prem/self-hosted deployment**, by an operator
  who has accepted the risk, via the existing `SHIFT_HUB_CONNECTOR_ALLOW`
  allowlist. Default-deny is the safe default; reaching the host is an opt-in.
- Recommendation, documented: even on-prem, keep Tier B on an **allowlist**
  rather than a bare "allow all", so a compromised or careless flow author is
  bounded to the connectors the operator chose.

`ldap`/`exchange` are **not** in the dangerous set — they reach the network,
not the host, and are governed by the ordinary allow/deny lists (a cloud hub
that offers hosted AD integration may permit them; one that does not, denies
them). This is the moment ADR-0015 anticipated where the name list starts to
strain: a signed **capability-metadata** dimension on the manifest (connector
declares `process`/`filesystem`/`network`) so policy reasons about *kinds* not
names becomes worth building. This ADR flags it as the natural next step but
does not block on it — the name-based allow/deny is sufficient to ship.

### 4. Threat model — the flow author is the adversary

The concrete adversary is **someone who controls a flow definition** (a
low-privilege studio user, a compromised design account, or a malicious insert
into flow config). Assume they author arbitrary config for any allowed
connector. What can and cannot they do?

**What the gates STOP:**

- **Shell injection is structurally impossible for the default path.** Commands
  are specified as an **argv array** and executed with `exec.CommandContext`
  (no intermediate shell) — there is no shell to inject into. A field value of
  `; rm -rf /` is one literal argument, not a metacharacter. `sh -c` / arbitrary
  shell strings are available **only behind an explicit, warned opt-in flag**
  per action, off by default, and denied when the deployment forbids it.
- **Arbitrary binaries can't be run.** The invoked binary must match a
  configured **allowlist** (`exec`/`ssh-exec`/`winrm` each take an operator-set
  allowed-command list in connector config, not flow config). A flow author who
  names `/usr/bin/curl` when only `/opt/app/report.sh` is allowed is rejected
  before spawn. The allowlist is **deployment/connector config**, outside the
  flow author's reach.
- **The host filesystem is jailed.** `exec` runs in a configured working
  directory; path arguments are validated against escape (no `..` traversal out
  of the jail). It is not a container boundary — see residual risk — but it
  stops the casual `cat ../../etc/shadow`.
- **Runaway and exfil-by-volume are bounded.** Every Tier B invocation has a
  **mandatory timeout** (process killed on expiry, no unbounded hang) and an
  **output-size cap** (stdout/stderr truncated past the bound, so a command
  cannot stream gigabytes back through the flow, and cannot OOM the runner —
  consistent with the ADR-0005 memory doctrine).
- **Credentials never appear in argv or logs.** `$secret` values are passed to
  processes via **stdin or environment**, never on the command line (argv is
  world-readable via `/proc`), and never interpolated into a logged command.
  Error strings carry the *secret name*, never the value (ADR-0010). Command
  text logged for audit is the allowlisted binary + argument *shape*, redacted.
- **A cloud tenant can't reach any of this at all.** Default-deny + invisibility
  (§3) means the entire Tier B surface does not exist for a shared-hub flow.
- **The hub is never exposed to it.** Command output is payload; it flows
  runner→sink and is reported to the hub only as metadata. A malicious command's
  output cannot leak to the control plane because there is no channel for it.

**What the gates DO NOT stop (residual risk, stated honestly):**

- **An allow-listed command is still arbitrary code within its own bounds.** If
  the operator allows `report.sh`, a flow author can run `report.sh` with any
  allowed arguments. The allowlist bounds *which* programs, not what a permitted
  program does. Choosing what to allow is the operator's security decision, and
  the docs must say so plainly.
- **`exec` runs with the runner's OS privileges.** The working-dir jail is not a
  sandbox; a permitted binary can act with the runner process's identity.
  Mitigation is deployment posture: **run the on-prem runner as a low-privilege,
  dedicated service account**, ideally containerised, never root/Administrator.
  This is guidance, not a code gate — documented as a hard prerequisite for
  enabling Tier B.
- **`ssh-exec`/`winrm` run with the *target* host's credentials.** Blast radius
  is whatever that service account can do on that host; scope those accounts
  narrowly (JEA-constrained WinRM endpoints, restricted SSH users).
- **The opt-in `sh -c` path reintroduces injection** by definition. That is the
  point of making it an explicit, warned, per-deployment opt-in that the cloud
  policy forbids outright.

Net: the **default configuration** of the dangerous connectors is
injection-free, allowlisted, jailed, time- and size-bounded, credential-safe,
and invisible to anyone who shouldn't have it. The residual risks are the
irreducible ones of "we let an operator run chosen programs on their own hosts",
made explicit rather than papered over, and pushed onto deployment posture the
operator controls.

## Consequences

- **A capability cloud-only iPaaS cannot match:** cloud control plane, on-prem
  execution, payload never leaving the customer network — sold as a topology
  guarantee backed by the ADR-0016 two-plane split, not a policy promise.
- **The dangerous set grows** from `filesystem` to
  `{filesystem, exec, ssh-exec, winrm}`; `connpolicy` needs no code change (it
  is name-based), only the deployment allow/deny config and the docs that name
  the set. It **motivates the signed capability-metadata dimension** ADR-0015
  deferred — the recommended follow-on so policy keys on kinds, not names.
- **New connectors, no engine change.** `ldap`/`exchange`/`exec`/`ssh-exec`/
  `winrm` are ordinary ADR-0024 verb-node connectors over the existing
  streaming/descriptor/secret contracts; `ssh-exec`/`winrm`/`ldap`-LDAPS reuse
  the `sftpconn` SSH/TLS + host-key + network-guard foundation and the
  `httpconn` SSRF-guard pattern. No hot-path or hub change.
- **Runner deployment gains a documented hardening baseline** for Tier B:
  low-privilege service account, containerised, allowlists populated, `sh -c`
  opt-in left off, timeouts/output caps set. Enabling Tier B without it is
  unsupported.
- **Honest guarantees:** injection-free and invisible-to-cloud by construction;
  "operator-chosen arbitrary code on operator-owned hosts" is the residual risk,
  bounded by allowlist + jail + timeout + output cap + credential hygiene and
  owned by deployment posture.
- **Build deferred.** This is a design lock so the policy "dangerous set" and
  the connector shapes are fixed; the wave is sequenced after the current
  connector track, gated on the runner secure-agent packaging (low-priv
  container image, outbound-only posture) being documented and the Tier B
  hardening baseline shipping alongside the first Tier B connector.

## Proof (when built)

- `connpolicy`: `exec`/`ssh-exec`/`winrm` denied+invisible under a cloud policy
  (deploy→422, list→filtered, resolve→404), allowed only under an explicit
  allowlist — extends the existing ADR-0015 matrix.
- `exec`/`ssh-exec`/`winrm`: argv-only execution (a metacharacter argument runs
  as one literal token), allowlist rejection before spawn, working-dir jail
  escape rejected, timeout kills a hanging process, output cap truncates,
  `$secret` absent from argv and logs (stdin/env only) — the Tier B security
  matrix.
- `ssh-exec`/`winrm`/`ldap`(LDAPS): host-key/TLS verification fail-closed,
  network guard on the post-DNS IP (reusing the `sftpconn`/`httpconn` tests).
- `ldap`: write/password verbs refused over cleartext LDAP; paged `search`
  streams without buffering.
- On-prem e2e: hub in "cloud" (Tier B denied) proves invisibility; a separate
  self-hosted hub with Tier B allow-listed runs a flow end-to-end with payload
  asserted never to reach the hub.
