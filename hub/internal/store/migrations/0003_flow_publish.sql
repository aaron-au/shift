-- Flow publish workflow (M4b): versions are drafts until published.
-- Version 0 ("default") now resolves to the published version, so the
-- default execute path and the scheduler only ever run vetted
-- documents; explicit draft versions stay runnable via the admin API
-- for smoke-testing.
ALTER TABLE flow_versions ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft','published'));
ALTER TABLE flows ADD COLUMN published_version INT NOT NULL DEFAULT 0;

-- Anything deployed before this migration keeps its M4a behavior
-- (latest version was executable, so publish it).
UPDATE flow_versions SET status = 'published';
UPDATE flows SET published_version = latest_version;
