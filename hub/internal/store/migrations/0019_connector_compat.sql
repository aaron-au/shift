-- Per-version compatibility class (ADR-0047 §6).
--
-- Retention solves execution but not the UPGRADE DIFF. A customer moving from
-- v0.2.0 to v0.5.0 crosses three releases they never read, and "what will
-- change?" is the question they actually have. So a version declares what kind
-- of change it is, and the notice folds the whole span rather than the last
-- hop: "3 versions behind; 1 behaviour change in v0.4.0".
--
-- Four values, and 'unknown' is the DEFAULT rather than 'compatible':
--
--   compatible       additive or internal; no config or output change
--   behaviour-change same config, different results
--   breaking         config or output shape changed; the flow needs editing
--   unknown          the publisher did not say
--
-- Defaulting to 'compatible' would be the dangerous direction — every version
-- published before this column existed would claim to be safe, and so would
-- every publisher who forgot. 'unknown' is honest and reads as a prompt.
--
-- The class is NOT part of the signed manifest. A publisher's own declaration
-- is weak evidence on its own (ADR-0047 §6 says so), which is why §8 backs it
-- with a compatibility suite in CI rather than by signing the claim. Recording
-- it unsigned and saying where it came from beats a v3 manifest format for a
-- field the hub only ever shows to a human.
ALTER TABLE connector_versions
    ADD COLUMN compat TEXT NOT NULL DEFAULT 'unknown'
    CHECK (compat IN ('compatible', 'behaviour-change', 'breaking', 'unknown'));

-- Release notes for the version, shown beside the class in an upgrade diff.
-- Free text, never parsed, bounded by the API rather than by the column.
ALTER TABLE connector_versions ADD COLUMN release_notes TEXT NOT NULL DEFAULT '';
