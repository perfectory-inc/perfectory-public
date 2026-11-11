-- Root ADR-0492: a lakehouse bake may cut its archive to a deeper zoom than the release it replaces,
-- and the release it promotes says so.
--
-- W41 (2026-11-09) measured the served parcel, complex and zoning tiles against their Gold
-- boundaries: below maxzoom every tile is under a pixel, but the map draws the z16 (zoning z14)
-- tiles to z22, and mapbox-gl re-quantises every tile to its own 8192-unit extent. A z16 tile drawn
-- at z22 is 16,384 px wide, two pixels per unit, so the drawn edge moves by up to a pixel and the
-- outline steps; at z17 and detail 13 the archive's unit is the renderer's unit and nothing is
-- rounded twice. The bake reads its layer from the active release and promotion copied the active
-- release's layers, so no bake could change the zoom a unit is cut to.
--
-- tile_max_zoom NULL: the build cuts the input release's zooms (every build before this one). A
-- value is the maxzoom of the archive the build validated, and promotion writes it on the new
-- release's layer. It is an input of the build like its recipe, so the immutability rule gains it.
-- The guard function is replaced whole (later migrations replace earlier bodies); one change: the
-- immutable inputs gain tile_max_zoom. The rest is the body of
-- 20261110150000_a_tile_release_is_named_by_its_snapshot_and_the_recipe_that_baked_it.sql unchanged.

ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN tile_max_zoom smallint,
    ADD CONSTRAINT vector_tile_build_job_tile_max_zoom_check CHECK (
        tile_max_zoom IS NULL
        OR (kind = 'lakehouse_bake' AND tile_max_zoom BETWEEN 0 AND 24)
    );

COMMENT ON COLUMN catalog.vector_tile_build_job.tile_max_zoom IS
    'The maxzoom the lakehouse bake cut its archive to when it differs from its input release''s (root ADR-0492); promotion writes it on the new release''s layer. NULL: the input''s.';

CREATE OR REPLACE FUNCTION catalog.guard_vector_tile_lakehouse_bake_build()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
    selected_release uuid;
    selected_generation bigint;
    source catalog.vector_tile_release%ROWTYPE;
    input_revision catalog.publication_revision%ROWTYPE;
    output_revision catalog.publication_revision%ROWTYPE;
    bound_record catalog.source_record%ROWTYPE;
    selected_unit text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.kind = 'lakehouse_bake' OR NEW.kind = 'lakehouse_bake') AND
           ROW(NEW.kind, NEW.publication_unit_id, NEW.input_release_id, NEW.input_data_revision,
               NEW.frozen_source_snapshot_id, NEW.input_serving_generation,
               NEW.output_data_revision, NEW.idempotency_key, NEW.source_snapshot_id,
               NEW.matching_verdict, NEW.matching_verdict_sha256, NEW.bound_source_record_id,
               NEW.bake_inputs_sha256, NEW.tile_max_zoom)
           IS DISTINCT FROM
           ROW(OLD.kind, OLD.publication_unit_id, OLD.input_release_id, OLD.input_data_revision,
               OLD.frozen_source_snapshot_id, OLD.input_serving_generation,
               OLD.output_data_revision, OLD.idempotency_key, OLD.source_snapshot_id,
               OLD.matching_verdict, OLD.matching_verdict_sha256, OLD.bound_source_record_id,
               OLD.bake_inputs_sha256, OLD.tile_max_zoom) THEN
            RAISE EXCEPTION 'lakehouse bake build inputs are immutable' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.kind <> 'lakehouse_bake' THEN
        RETURN NEW;
    END IF;
    SELECT active_release_id, serving_generation, unit_key
    INTO selected_release, selected_generation, selected_unit
    FROM catalog.vector_tile_publication_unit
    WHERE id = NEW.publication_unit_id FOR UPDATE;
    -- ADR-0133 §5: a parcels bake always binds the Silver snapshot it read. Without this a caller
    -- could start a parcels bake with no snapshot and keep the input's inherited source.
    IF selected_unit = 'parcels' AND NEW.source_snapshot_id IS NULL THEN
        RAISE EXCEPTION 'a parcels lakehouse bake must name the Silver snapshot it read and its matching verdict'
            USING ERRCODE = '23514';
    END IF;
    IF selected_release IS DISTINCT FROM NEW.input_release_id
       OR selected_generation IS DISTINCT FROM NEW.input_serving_generation THEN
        RAISE EXCEPTION 'lakehouse bake input must be the active release and generation'
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO source FROM catalog.vector_tile_release
    WHERE id = NEW.input_release_id FOR SHARE;
    IF NOT FOUND OR NOT catalog.is_validated_static_tile_release(source)
       OR source.publication_unit_id <> NEW.publication_unit_id
       OR source.data_revision <> NEW.input_data_revision THEN
        RAISE EXCEPTION 'lakehouse bake input must bind the active validated static release'
            USING ERRCODE = '23514';
    END IF;
    -- A rebake of the snapshot the input serves is a new release only under a recorded recipe the
    -- input was not baked with: the bytes differ because the code that bakes them changed.
    IF source.canonical_iceberg_snapshot_id = NEW.frozen_source_snapshot_id
       AND (NEW.bake_inputs_sha256 IS NULL
            OR NEW.bake_inputs_sha256 IS NOT DISTINCT FROM source.bake_inputs_sha256) THEN
        RAISE EXCEPTION 'lakehouse bake must freeze a snapshot other than its input''s, or bake it under a recipe its input was not baked with'
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO input_revision FROM catalog.publication_revision WHERE id = NEW.input_data_revision;
    SELECT * INTO output_revision FROM catalog.publication_revision WHERE id = NEW.output_data_revision;
    IF input_revision.id IS NULL OR output_revision.id IS NULL THEN
        RAISE EXCEPTION 'lakehouse bake names a revision the ledger does not hold'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.source_snapshot_id IS NULL THEN
        -- Summary v1: the served Gold was rebuilt over the input's collected source.
        IF output_revision.source_record_id IS DISTINCT FROM input_revision.source_record_id
           OR output_revision.bronze_object_id IS DISTINCT FROM input_revision.bronze_object_id THEN
            RAISE EXCEPTION 'lakehouse bake output revision must keep its input''s collected source'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    -- Summary v2: the served Gold was read from one Silver snapshot, which the gate passed.
    IF NEW.matching_verdict->>'schema_version' IS DISTINCT FROM 'foundation-platform.parcel_matching_verdict.v1'
       OR NEW.matching_verdict->>'snapshot_id' IS DISTINCT FROM NEW.source_snapshot_id
       OR jsonb_typeof(NEW.matching_verdict->'passed') IS DISTINCT FROM 'boolean'
       OR (NEW.matching_verdict->>'passed')::boolean IS NOT TRUE THEN
        RAISE EXCEPTION 'lakehouse bake of Silver snapshot % needs a passing matching verdict for that snapshot',
            NEW.source_snapshot_id USING ERRCODE = '23514';
    END IF;
    IF jsonb_typeof(NEW.matching_verdict->'snapshot_parcel_count') IS DISTINCT FROM 'number'
       OR jsonb_typeof(NEW.matching_verdict->'parcels'->'checked') IS DISTINCT FROM 'number'
       OR (NEW.matching_verdict->>'snapshot_parcel_count')::numeric <= 0
       OR (NEW.matching_verdict->'parcels'->>'checked')::numeric
          <> (NEW.matching_verdict->>'snapshot_parcel_count')::numeric THEN
        RAISE EXCEPTION 'the matching verdict for Silver snapshot % did not check every parcel of it',
            NEW.source_snapshot_id USING ERRCODE = '23514';
    END IF;
    SELECT * INTO bound_record FROM catalog.source_record WHERE id = NEW.bound_source_record_id;
    IF bound_record.id IS NULL
       OR bound_record.source IS DISTINCT FROM 'lakehouse:silver.parcel_boundaries'
       OR bound_record.external_id IS DISTINCT FROM NEW.source_snapshot_id THEN
        RAISE EXCEPTION 'lakehouse bake binds a source record that does not describe Silver snapshot %',
            NEW.source_snapshot_id USING ERRCODE = '23514';
    END IF;
    IF output_revision.source_record_id IS DISTINCT FROM NEW.bound_source_record_id
       OR output_revision.bronze_object_id IS NOT NULL THEN
        RAISE EXCEPTION 'lakehouse bake output revision must be anchored to the Silver snapshot it read'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;
