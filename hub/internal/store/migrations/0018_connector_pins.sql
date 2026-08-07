-- Connector pin references (ADR-0047 §2).
--
-- A pin lives in the flow document, which is JSONB — findable, but not
-- cheaply, and four different features need the same answer: which published
-- flow versions run this connector build? Retention/GC, yank warnings, EOL
-- notices and bulk locate all read this index rather than each scanning and
-- parsing every stored document.
--
-- It is derived state, written from the document at publish. That makes the
-- document authoritative and this a cache — the right way round, because a
-- disagreement should be resolved by re-reading the flow, never by trusting a
-- table that drifted.
CREATE TABLE flow_connector_pins (
    account_id   UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    flow_id      UUID NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
    flow_version INT  NOT NULL,
    step_id      TEXT NOT NULL,
    connector    TEXT NOT NULL,
    version      TEXT NOT NULL,
    PRIMARY KEY (flow_id, flow_version, step_id),
    FOREIGN KEY (flow_id, flow_version) REFERENCES flow_versions(flow_id, version) ON DELETE CASCADE
);

-- The reference lookup: "who pins connector X at version V".
CREATE INDEX flow_connector_pins_ref ON flow_connector_pins (account_id, connector, version);

-- published_at orders a flow's publish history, which is what makes "the
-- version it would roll back to" answerable.
--
-- Retention keeps the connector builds pinned by a flow's CURRENT published
-- version and by the one before it. Counting every version ever published
-- would mean nothing is ever collectable — a superseded version keeps its
-- 'published' status, deliberately, so it stays runnable — and counting only
-- the current one would let a rollback land on a connector that had been
-- collected. Two is the same floor the registry keeps for connectors
-- themselves (latest and n-1), for the same reason.
ALTER TABLE flow_versions ADD COLUMN published_at TIMESTAMPTZ;

-- Existing published rows get a timestamp so their history has an order.
-- created_at is the honest approximation: it is when the version existed, and
-- for anything already published it preserves the real sequence.
UPDATE flow_versions SET published_at = created_at WHERE status = 'published';

CREATE INDEX flow_versions_published ON flow_versions (flow_id, published_at DESC)
    WHERE published_at IS NOT NULL;
