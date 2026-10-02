-- Root ADR-0133 §5: a parcel bake from a new Silver snapshot names that snapshot as its source.
--
-- `20260928120000_lakehouse_bake.sql` binds a lakehouse bake's output revision to its INPUT
-- revision's collected source. That is right for the complex and admin units, whose served Gold is
-- rebuilt over the same collected file. It is wrong for parcels: ADR-0133 rebakes parcels from
-- whichever `silver.parcel_boundaries` snapshot is newest, so the inherited source would name the
-- collection the previous map came from while the new map shows another.
--
-- A bake whose served summary names the Silver snapshot it read (summary v2) now carries three
-- facts on its build row:
--   * `source_snapshot_id`  — the `silver.parcel_boundaries` snapshot read;
--   * `matching_verdict`    — the ADR-0113 §7 verdict `parcel_matching_gate.py` wrote for it;
--   * `bound_source_record_id` — the `catalog.source_record` that describes that snapshot.
-- The output revision must be anchored to that record and to nothing else. Without a passing
-- verdict for the same snapshot, covering every parcel of it, the build row is refused and so the
-- build does not start. A bake that names no Silver snapshot (summary v1) keeps the inherited rule.
--
-- One Silver snapshot is read from 255 collected objects, and a revision holds exactly one anchor
-- (`publication_revision_one_provenance_anchor_check`), so the anchor is a source record that
-- describes the snapshot rather than any one of its objects; root ADR-0046 names that as the anchor
-- for a version the platform itself assembled. The record is unique per snapshot so two bakes of
-- one snapshot cannot describe it twice.
--
-- The guard function is replaced whole: later migrations replace earlier bodies.

CREATE UNIQUE INDEX source_record_silver_parcel_snapshot_key
    ON catalog.source_record (external_id)
    WHERE source = 'lakehouse:silver.parcel_boundaries';

ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN source_snapshot_id text,
    ADD COLUMN matching_verdict jsonb,
    ADD COLUMN matching_verdict_sha256 character(64),
    ADD COLUMN bound_source_record_id uuid REFERENCES catalog.source_record(id) ON DELETE RESTRICT,
    ADD CONSTRAINT vector_tile_build_job_silver_source_check CHECK (
        num_nonnulls(source_snapshot_id, matching_verdict, matching_verdict_sha256,
                     bound_source_record_id) IN (0, 4)
        AND (source_snapshot_id IS NULL OR kind = 'lakehouse_bake')
        AND (source_snapshot_id IS NULL OR btrim(source_snapshot_id) <> '')
        AND (matching_verdict_sha256 IS NULL OR matching_verdict_sha256 ~ '^[0-9a-f]{64}$')
    );

COMMENT ON COLUMN catalog.vector_tile_build_job.matching_verdict IS
    'The ADR-0113 §7 verdict (foundation-platform.parcel_matching_verdict.v1) for source_snapshot_id; recorded as build evidence, required to pass.';

CREATE OR REPLACE FUNCTION catalog.guard_vector_tile_lakehouse_bake_build()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
    selected_release uuid;
    selected_generation bigint;
    source catalog.vector_tile_release%ROWTYPE;
    input_revision catalog.publication_revision%ROWTYPE;
    output_revision catalog.publication_revision%ROWTYPE;
    bound_record catalog.source_record%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.kind = 'lakehouse_bake' OR NEW.kind = 'lakehouse_bake') AND
           ROW(NEW.kind, NEW.publication_unit_id, NEW.input_release_id, NEW.input_data_revision,
               NEW.frozen_source_snapshot_id, NEW.input_serving_generation,
               NEW.output_data_revision, NEW.idempotency_key, NEW.source_snapshot_id,
               NEW.matching_verdict, NEW.matching_verdict_sha256, NEW.bound_source_record_id)
           IS DISTINCT FROM
           ROW(OLD.kind, OLD.publication_unit_id, OLD.input_release_id, OLD.input_data_revision,
               OLD.frozen_source_snapshot_id, OLD.input_serving_generation,
               OLD.output_data_revision, OLD.idempotency_key, OLD.source_snapshot_id,
               OLD.matching_verdict, OLD.matching_verdict_sha256, OLD.bound_source_record_id) THEN
            RAISE EXCEPTION 'lakehouse bake build inputs are immutable' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.kind <> 'lakehouse_bake' THEN
        RETURN NEW;
    END IF;
    SELECT active_release_id, serving_generation INTO selected_release, selected_generation
    FROM catalog.vector_tile_publication_unit
    WHERE id = NEW.publication_unit_id FOR UPDATE;
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
    IF source.canonical_iceberg_snapshot_id = NEW.frozen_source_snapshot_id THEN
        RAISE EXCEPTION 'lakehouse bake must freeze a snapshot other than its input''s'
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
