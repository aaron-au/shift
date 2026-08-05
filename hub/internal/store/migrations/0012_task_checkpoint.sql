-- Resume cursor (ADR-0037): the source position a task's terminal sink last
-- CONFIRMED, carried so a re-dispatched task -- very possibly on a different
-- runner -- restarts from where the previous attempt got to.
--
-- Opaque bytes. The hub never interprets them; only the connector that
-- produced a cursor can. That opacity is what keeps resumption on the control
-- plane: a page token, a byte offset, a CDC LSN and a keyset high-water mark
-- are all a few bytes, and none of them is payload (ADR-0016 holds).
ALTER TABLE tasks ADD COLUMN checkpoint bytea;

-- The connector that produced the cursor, pinned so it is never handed to a
-- build that cannot understand it. A replacement runner may hold a different
-- version, and an older cursor parsed under a newer one could resolve to a
-- DIFFERENT position -- resuming at the wrong place, silently. On mismatch the
-- runner replays from the start instead: slower, and correct.
ALTER TABLE tasks ADD COLUMN checkpoint_connector text;
ALTER TABLE tasks ADD COLUMN checkpoint_version text;
