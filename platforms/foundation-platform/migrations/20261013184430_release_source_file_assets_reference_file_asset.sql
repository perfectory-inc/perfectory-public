-- `catalog.vector_tile_release.source_file_asset_ids` is a `uuid[]`. PostgreSQL cannot put a
-- FOREIGN KEY on an array element, so a release could name a `catalog.file_asset` that never
-- existed, and an asset a release names could be deleted. Decision and rejected alternatives:
-- root ADR-0231.
--
-- The array stays the value every reader and writer already uses (its order is part of the
-- publication request fingerprint). This table holds one row per array element with real foreign
-- keys, and only the trigger below writes it, so a release whose array names a missing asset fails
-- its own INSERT with SQLSTATE 23503.

CREATE TABLE catalog.vector_tile_release_source_file_asset (
    release_id uuid NOT NULL REFERENCES catalog.vector_tile_release(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    file_asset_id uuid NOT NULL,
    PRIMARY KEY (release_id, ordinal),
    CONSTRAINT vector_tile_release_source_file_asset_ordinal_check CHECK (ordinal >= 1),
    CONSTRAINT vector_tile_release_source_file_asset_file_asset_fkey
        FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id)
);

CREATE INDEX vector_tile_release_source_file_asset_file_asset_idx
    ON catalog.vector_tile_release_source_file_asset (file_asset_id);

CREATE FUNCTION catalog.sync_vector_tile_release_source_file_asset()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    DELETE FROM catalog.vector_tile_release_source_file_asset WHERE release_id = NEW.id;
    INSERT INTO catalog.vector_tile_release_source_file_asset (release_id, ordinal, file_asset_id)
    SELECT NEW.id, element.ordinal, element.file_asset_id
    FROM unnest(NEW.source_file_asset_ids) WITH ORDINALITY AS element(file_asset_id, ordinal);
    RETURN NULL;
END
$function$;

CREATE TRIGGER vector_tile_release_source_file_asset_sync
    AFTER INSERT OR UPDATE OF source_file_asset_ids ON catalog.vector_tile_release
    FOR EACH ROW EXECUTE FUNCTION catalog.sync_vector_tile_release_source_file_asset();

-- Backfill the releases that already exist. An element naming no asset is left out instead of
-- failing the deploy; the WARNING names each such release, and ADR-0231 records the query that finds
-- them afterwards (array cardinality larger than its row count here). Measured before this
-- migration: production had none.
INSERT INTO catalog.vector_tile_release_source_file_asset (release_id, ordinal, file_asset_id)
SELECT release.id, element.ordinal, element.file_asset_id
FROM catalog.vector_tile_release AS release
CROSS JOIN LATERAL unnest(release.source_file_asset_ids)
    WITH ORDINALITY AS element(file_asset_id, ordinal)
JOIN catalog.file_asset AS asset ON asset.id = element.file_asset_id;

DO $backfill$
DECLARE
    dangling record;
BEGIN
    FOR dangling IN
        SELECT release.id, element.file_asset_id
        FROM catalog.vector_tile_release AS release
        CROSS JOIN LATERAL unnest(release.source_file_asset_ids) AS element(file_asset_id)
        WHERE NOT EXISTS (SELECT 1 FROM catalog.file_asset WHERE id = element.file_asset_id)
    LOOP
        RAISE WARNING 'vector_tile_release % names missing file_asset %; not backfilled',
            dangling.id, dangling.file_asset_id;
    END LOOP;
END
$backfill$;
