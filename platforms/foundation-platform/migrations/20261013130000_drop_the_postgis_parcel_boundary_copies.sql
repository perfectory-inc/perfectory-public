-- Root ADR-0194 §1, §5–§7 and ADR-0133 §9: serving_postgis.parcel_boundary_mirror (62 GB) and
-- serving_postgis.parcel_boundary_publication (50 GB) were the PostGIS round trip that baked the
-- parcel tiles. The lakehouse bake replaces it (parcel_tile_bake, ADR-0133 §7) and the marker
-- anchors read the Silver shards in R2 (ADR-0133 §8). Merged only when ADR-0194 §6 holds: the
-- release without their readers and writers is live, their scan and write counters stopped, the
-- last full backup holds them and a side-instance restore of it matched the ledgers, the parcel
-- unit's active release is a lakehouse bake with no fallback, and the coordinator said so.
--
-- Every dependent is named and nothing is dropped with CASCADE, so an object this file did not
-- foresee stops the migration instead of disappearing with them. Both tables live in tablespace
-- parcel_bulk on /data; dropping them returns the space to /data. The tablespace itself stays:
-- DROP TABLESPACE cannot run inside the migration's transaction.
--
-- Kept (ADR-0133 §9): the history ledgers serving_postgis.parcel_boundary_mirror_rebuild_run and
-- serving_postgis.spatial_projection_load, the catalog.parcel_publication_source_evidence rows
-- that cite them, and the release rows of source kind dynamic_postgis.

-- The Martin view over the publication (0 rows since the parcel unit went static, 2026-09).
DROP VIEW serving_postgis.parcel_boundary_current;

-- The tables; their guard triggers go with them, then the guard functions, which nothing else
-- calls.
DROP TABLE serving_postgis.parcel_boundary_publication;
DROP TABLE serving_postgis.parcel_boundary_mirror;
DROP FUNCTION serving_postgis.reject_parcel_boundary_publication_mutation();
DROP FUNCTION serving_postgis.protect_sealed_parcel_boundary_mirror_row();
DROP FUNCTION serving_postgis.reject_sealed_parcel_boundary_mirror_truncate();

-- New parcel publication source evidence checked a mirror row set; there is none to check any
-- more, and a PostGIS parcel publication is not made again. The rows already recorded stay.
CREATE OR REPLACE FUNCTION catalog.validate_parcel_publication_source_evidence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'parcel publication source evidence belonged to the PostGIS parcel round trip, retired by ADR-0194; parcel tiles are baked from the lakehouse'
        USING ERRCODE = '23514';
END;
$function$;
