-- Gateway pairing by one-time install token (ADR-0049 §1a).
--
-- The hub cannot know a gateway's key before the gateway exists — it generates
-- one on first start — so it does not try. The operator creates the record,
-- gets a token, and supplies it at deploy time; the hub LEARNS the fingerprint
-- on the first dial and pins it from then on. The token is what makes learning
-- it safe, and it is burned once adoption completes.
ALTER TABLE gateways ADD COLUMN install_token TEXT NOT NULL DEFAULT '';
ALTER TABLE gateways ADD COLUMN token_expires_at TIMESTAMPTZ;

-- The fingerprint is empty until adoption, so the uniqueness rule has to skip
-- the unadopted. Without the predicate a second unadopted gateway would collide
-- with the first on the empty string.
DROP INDEX IF EXISTS gateways_fingerprint;
CREATE UNIQUE INDEX gateways_fingerprint ON gateways (fingerprint) WHERE fingerprint <> '';
