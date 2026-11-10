-- Root ADR-0373, ADR-0323: a tile release is named by the Gold snapshot it shows AND the recipe that
-- baked it.
--
-- On 2026-11-09 #1210 made the zoning job rebake when the tile-bake contract (the images and the
-- per-zoom tippecanoe arguments) changes, so that tiles follow code changes as they follow data. The
-- Silver snapshot had not moved, and three layers each refused the rebake as "already served": the
-- patch path, the full bake, and this guard (`lakehouse bake must freeze a snapshot other than its
-- input's`). foundation-zoning-tile-bake failed on 2026-11-09 23:40Z and 2026-11-10 10:28Z, and the
-- low-zoom generalization never reached a visitor.
--
-- A build's output is a function of its inputs and its recipe (Bazel's action key, Nix's derivation
-- hash). The bake already digests its recipe (`bake_inputs`: GDAL image, tippecanoe version, SRID,
-- arguments, ADR-0316) to name its feature indexes; this records that digest on the build and on the
-- release it promotes, and admits a rebake of the input's own snapshot only under a recorded digest
-- the input was not baked with. NULL is a release baked before the digest was recorded: its recipe
-- is unknown, so any recorded one differs from it. A readdress carries its input's digest (same
-- bytes). The release uniqueness gains the digest so the rebake's release, over the same revision and
-- snapshot, is admitted while two releases of one recipe stay refused.
--
-- The guard function is replaced whole: later migrations replace earlier bodies. One rule changes
-- (the snapshot check) and the immutable inputs gain bake_inputs_sha256; the rest is the body of
-- 20261002130000_a_lakehouse_bake_binds_the_silver_snapshot_it_read.sql unchanged.

ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN bake_inputs_sha256 character(64),
    ADD CONSTRAINT vector_tile_build_job_bake_inputs_check CHECK (
        bake_inputs_sha256 IS NULL
        OR (kind = 'lakehouse_bake' AND bake_inputs_sha256 ~ '^[0-9a-f]{64}$')
    );

ALTER TABLE catalog.vector_tile_release
    ADD COLUMN bake_inputs_sha256 character(64),
    ADD CONSTRAINT vector_tile_release_bake_inputs_check CHECK (
        bake_inputs_sha256 IS NULL
        OR (source_kind = 'static_pmtiles' AND bake_inputs_sha256 ~ '^[0-9a-f]{64}$')
    );

COMMENT ON COLUMN catalog.vector_tile_release.bake_inputs_sha256 IS
    'SHA-256 of the recipe that baked the archive (images, tippecanoe version, SRID, arguments; root ADR-0316). NULL: baked before it was recorded.';

DROP INDEX catalog.vector_tile_release_unit_revision_snapshot_kind_key;
CREATE UNIQUE INDEX vector_tile_release_unit_revision_snapshot_kind_key
    ON catalog.vector_tile_release
        (publication_unit_id, data_revision, canonical_iceberg_snapshot_id, source_kind,
         coalesce(bake_inputs_sha256, ''))
    WHERE readdressed_from_release_id IS NULL;

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
               NEW.bake_inputs_sha256)
           IS DISTINCT FROM
           ROW(OLD.kind, OLD.publication_unit_id, OLD.input_release_id, OLD.input_data_revision,
               OLD.frozen_source_snapshot_id, OLD.input_serving_generation,
               OLD.output_data_revision, OLD.idempotency_key, OLD.source_snapshot_id,
               OLD.matching_verdict, OLD.matching_verdict_sha256, OLD.bound_source_record_id,
               OLD.bake_inputs_sha256) THEN
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
