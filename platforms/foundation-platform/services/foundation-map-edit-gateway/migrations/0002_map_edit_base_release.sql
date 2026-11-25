-- Root ADR-0621 §3: the release the edit's unit served when the edit was saved. The writer
-- (foundation-api) stamps it from the catalog, never the client; the Gold build resolves it to the
-- source state the edit is pinned to. A row without one cannot be resolved, and its edit is stale.
ALTER TABLE map_edit ADD COLUMN base_release_id TEXT CHECK (base_release_id IS NULL OR length(base_release_id) = 36);
