-- Connector config-schema discovery (ADR-0018). The descriptor is the
-- opaque, signed action-catalog blob (actions + directions + JSON-Schema
-- config forms) the studio builder renders. NULL for pre-descriptor
-- (v1-signed) artifacts. Stored and served verbatim; the hub never parses it.
ALTER TABLE connector_versions ADD COLUMN descriptor BYTEA;
