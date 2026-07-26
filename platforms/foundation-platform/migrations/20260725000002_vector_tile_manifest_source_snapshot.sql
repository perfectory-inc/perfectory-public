-- Carry the immutable Gold/Iceberg snapshot through the tile-manifest pointer.
-- Existing pre-contract rows receive a deterministic legacy bridge value. New promotion
-- requests are validated in the application and must carry a real immutable snapshot id.
ALTER TABLE catalog.vector_tile_manifest
    ADD COLUMN source_snapshot_id text;

UPDATE catalog.vector_tile_manifest
SET source_snapshot_id = 'iceberg:legacy-vector-tile-manifest-' || md5(current_version)
WHERE source_snapshot_id IS NULL;

ALTER TABLE catalog.vector_tile_manifest
    ALTER COLUMN source_snapshot_id SET NOT NULL;

ALTER TABLE catalog.vector_tile_manifest
    ADD CONSTRAINT vector_tile_manifest_source_snapshot_id_check
    CHECK (source_snapshot_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$');
