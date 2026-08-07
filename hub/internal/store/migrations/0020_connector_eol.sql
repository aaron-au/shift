-- Connector end-of-life (ADR-0047 §7).
--
-- GC cannot remove a referenced version, which leaves one gap: a build that is
-- genuinely poisoned — a CVE in a dependency, a protocol flaw — and is still
-- pinned by live flows. Retention is deliberately unable to touch it. That
-- needs a deliberate act, not an automatic one.
--
-- EOL is that act, and it is reserved for SECURITY. It is not the routine
-- upgrade path: §4 (publish drags you forward) and §5 (currency notices) are.
--
-- Three fields, and the deadline is the whole design:
--
--   eol_at      when the version stops resolving. Until then it runs exactly
--               as before, while notices escalate on every flow pinning it.
--   eol_reason  why, in the words a customer will be read over the phone.
--   eol_target  where to go instead — the one thing somebody needs next.
--
-- At the deadline pinned tasks FAIL. They are not silently upgraded: swapping
-- a connector underneath live customer data without anyone testing it is
-- precisely the risk ADR-0047 exists to remove, and doing it automatically on
-- a timer would be the same mistake with a clock attached. Failing after weeks
-- of escalating notice is honest.
--
-- Version-level, not per platform. Yank is per (os, arch) because a bad BUILD
-- can be platform-specific; a poisoned dependency is a property of the release,
-- and an EOL that left one platform live would be an EOL that did not happen.
ALTER TABLE connector_versions ADD COLUMN eol_at     TIMESTAMPTZ;
ALTER TABLE connector_versions ADD COLUMN eol_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE connector_versions ADD COLUMN eol_target TEXT NOT NULL DEFAULT '';

-- The notice path asks "which pinned versions are heading for a deadline?" on
-- every review, so it is worth an index even though the table is small.
CREATE INDEX connector_versions_eol ON connector_versions (eol_at) WHERE eol_at IS NOT NULL;
