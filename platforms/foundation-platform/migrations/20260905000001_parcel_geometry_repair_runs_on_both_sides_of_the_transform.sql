-- The geometry repair strategy moves to v2: repair on both sides of the reprojection.
--
-- Measured 2026-09-05: the first national parcel mirror run failed at 13.87M rows because a
-- geometry valid in EPSG:4326 becomes self-intersecting after ST_Transform to 5179, where the
-- mirror's validity CHECK lives. The rebuild now repairs after the transform as well, and the
-- strategy label the writer/sealer carry changed with it (root ADR-0082). This function pinned
-- the v1 label, so the pin moves with the truth; no sealed parcel evidence row exists yet, so
-- v2-only is a clean cutover rather than a migration of history.
CREATE OR REPLACE FUNCTION catalog.validate_parcel_publication_source_evidence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, serving_postgis, public, pg_temp
AS $function$
DECLARE
    run_quality jsonb;
    actual_row_count bigint;
    mismatched_row_count bigint;
BEGIN
    SELECT run.quality_report
      INTO run_quality
      FROM serving_postgis.parcel_boundary_mirror_rebuild_run AS run
     WHERE run.id = NEW.mirror_rebuild_run_id
       AND run.status = NEW.mirror_rebuild_run_status
       AND run.source_snapshot_id = 'iceberg:' || NEW.iceberg_snapshot_id::text
       AND run.source_table = NEW.iceberg_logical_table
       AND run.source_record_id = NEW.source_record_id
       AND run.source_file_asset_id = NEW.source_file_asset_id
       AND run.loaded_row_count = NEW.source_row_count
       AND run.rejected_row_count = NEW.mirror_rebuild_rejected_row_count
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'source evidence does not match one succeeded mirror rebuild tuple'
            USING ERRCODE = '23514';
    END IF;

    SELECT count(*),
           count(*) FILTER (
               WHERE row.source_snapshot_id IS DISTINCT FROM 'iceberg:' || NEW.iceberg_snapshot_id::text
                  OR row.source_table IS DISTINCT FROM NEW.iceberg_logical_table
                  OR row.source_record_id IS DISTINCT FROM NEW.source_record_id
                  OR row.source_file_asset_id IS DISTINCT FROM NEW.source_file_asset_id
           )
      INTO actual_row_count, mismatched_row_count
      FROM serving_postgis.parcel_boundary_mirror AS row
     WHERE row.rebuild_run_id = NEW.mirror_rebuild_run_id;
    IF actual_row_count <> NEW.source_row_count OR mismatched_row_count <> 0 THEN
        RAISE EXCEPTION 'source evidence does not match the run-keyed parcel mirror row set'
            USING ERRCODE = '23514';
    END IF;

    IF jsonb_typeof(run_quality) <> 'object'
       OR run_quality ->> 'schema_version' IS DISTINCT FROM NEW.quality_schema_version
       OR jsonb_typeof(run_quality -> 'object_count') <> 'number'
       OR (run_quality ->> 'object_count') !~ '^[1-9][0-9]*$'
       OR run_quality -> 'expected_row_count' IS DISTINCT FROM to_jsonb(NEW.source_row_count)
       OR run_quality -> 'loaded_row_count' IS DISTINCT FROM to_jsonb(NEW.source_row_count)
       OR run_quality -> 'invalid_srid_count' IS DISTINCT FROM '0'::jsonb
       OR run_quality -> 'invalid_geometry_count' IS DISTINCT FROM '0'::jsonb
       OR run_quality -> 'empty_geometry_count' IS DISTINCT FROM '0'::jsonb
       OR run_quality -> 'nonpositive_area_count' IS DISTINCT FROM '0'::jsonb
       OR run_quality ->> 'source_srid' IS DISTINCT FROM 'EPSG:4326'
       OR run_quality ->> 'target_srid' IS DISTINCT FROM 'EPSG:5179'
       OR run_quality ->> 'geometry_repair_strategy' IS DISTINCT FROM 'postgis-make-valid-both-sides-of-transform-v2'
    THEN
        RAISE EXCEPTION 'source evidence requires a complete parcel publication quality report'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;
