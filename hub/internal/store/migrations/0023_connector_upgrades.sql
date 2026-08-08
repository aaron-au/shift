-- Bulk connector upgrade batches (ADR-0047 §9).
--
-- §9 is emphatic that a mass republish is three staged steps and never one
-- button: locate, test, publish-all. That staging only means anything if the
-- three steps refer to the SAME set of flows and the SAME target version —
-- otherwise "I tested it" is a claim about a set nobody recorded, and the
-- publish is back to being a button.
--
-- So the batch is durable. It is created when the drafts are staged, it holds
-- the target version once (never re-resolved from "newest" between steps —
-- newest can move while somebody is reading the report), and publish-all
-- refuses unless every flow in it has a test task that actually passed.
--
-- The rows are also the audit record §9 asks for: which flows moved, from
-- which build to which, on which draft version, and what the per-flow review
-- said at the moment it was published.
CREATE TABLE connector_upgrade_batches (
    id           UUID PRIMARY KEY,
    account_id   UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    connector    TEXT NOT NULL,
    target       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by   TEXT NOT NULL DEFAULT '',
    -- Set when publish-all completes. A batch published twice would republish
    -- drafts that are already live, so this is the idempotency marker as well
    -- as the audit timestamp.
    published_at TIMESTAMPTZ
);

CREATE INDEX connector_upgrade_batches_recent
    ON connector_upgrade_batches (account_id, created_at DESC);

-- One row per flow in the batch. from_version is recorded at STAGE time
-- because it is the thing an operator needs to undo by hand later, and by
-- then the flow's published version has moved past it.
CREATE TABLE connector_upgrade_flows (
    batch_id      UUID NOT NULL REFERENCES connector_upgrade_batches(id) ON DELETE CASCADE,
    flow_id       UUID NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
    flow_name     TEXT NOT NULL,
    from_version  TEXT NOT NULL,
    draft_version INT  NOT NULL,
    -- The test-tier task staged for this draft (ADR-0048). NULL only if the
    -- enqueue itself failed; publish-all treats that as "untested", not as
    -- "passed", because an absent result is not a good result.
    task_id       UUID REFERENCES tasks(id) ON DELETE SET NULL,
    -- Filled at publish: what the review said about this draft, at the moment
    -- it went live. Notices change as the registry moves; the audit record
    -- must not.
    notices       JSONB,
    published_at  TIMESTAMPTZ,
    PRIMARY KEY (batch_id, flow_id)
);
