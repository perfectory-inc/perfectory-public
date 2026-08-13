-- Binds one parcel projection load to one sealed source fact (ADR-0025).
--
-- This is deliberately one vertical migration. A run-keyed source set without an immutable run can
-- still be changed after validation; an immutable run without durable evidence leaves the
-- Iceberg/Catalog relationship as operator input; evidence without a load gate remains optional at
-- the only visibility switch. Shipping the four pieces together is the invariant.

-- A production source set must survive a PostgreSQL restart and a later materialisation of the same
-- PNU. The legacy table was UNLOGGED and keyed by PNU alone, so a crash could leave a succeeded run
-- without rows and a second run had to overwrite the first. Existing row values are not rewritten;
-- only their durability and key are changed.
ALTER TABLE serving_postgis.parcel_boundary_mirror SET LOGGED;
ALTER TABLE serving_postgis.parcel_boundary_mirror
    DROP CONSTRAINT parcel_boundary_mirror_pkey,
    ADD CONSTRAINT parcel_boundary_mirror_pkey PRIMARY KEY (rebuild_run_id, pnu);

-- The evidence foreign key names the whole validated run tuple, not just a run UUID that could be
-- paired with unrelated provenance. Nullable legacy/QA runs remain honest missing data: only a row
-- with non-null Catalog provenance can satisfy this key.
ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_evidence_tuple_key
    UNIQUE (
        id,
        status,
        source_snapshot_id,
        source_table,
        source_record_id,
        source_file_asset_id,
        loaded_row_count,
        rejected_row_count
    );

-- `file_asset.id` alone proves that an asset exists, but not that it belongs to the source record
-- named beside it. This referenced key makes the pair one Catalog fact for evidence rows.
ALTER TABLE catalog.file_asset
    ADD CONSTRAINT file_asset_id_source_record_key UNIQUE (id, source_record_id);

CREATE TABLE catalog.parcel_publication_source_evidence (
    id uuid PRIMARY KEY,
    mirror_rebuild_run_id uuid NOT NULL,
    -- These two fixed values are relational glue for the composite FK below. They cannot be used to
    -- turn a QA run into a succeeded one because the referenced run tuple must already contain them.
    mirror_rebuild_run_status text NOT NULL DEFAULT 'succeeded',
    mirror_rebuild_rejected_row_count bigint NOT NULL DEFAULT 0,
    iceberg_table_uuid uuid NOT NULL,
    iceberg_logical_table text NOT NULL,
    iceberg_snapshot_id bigint NOT NULL,
    canonical_iceberg_snapshot_id text
        GENERATED ALWAYS AS (iceberg_snapshot_id::text) STORED,
    mirror_source_snapshot_id text
        GENERATED ALWAYS AS ('iceberg:' || iceberg_snapshot_id::text) STORED,
    source_record_id uuid NOT NULL,
    source_file_asset_id uuid NOT NULL,
    -- Separate schema, not v2 of `silver_gold_national_promotion_execution`: v1 is truthful negative
    -- QA evidence and changing its meaning would make an old limitation look like production proof.
    execution_evidence_schema_version text NOT NULL,
    execution_evidence_object_key text NOT NULL,
    execution_evidence_sha256 character(64) NOT NULL,
    source_row_count bigint NOT NULL,
    -- The canonical bytes are defined by ADR-0025. At national scale the producer reads ordered PNU
    -- + EWKB records and updates SHA-256 incrementally in the application (constant memory); the DB
    -- does not materialise the entire stream through string_agg.
    projection_content_sha256 character(64) NOT NULL,
    quality_schema_version text NOT NULL,
    sealed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_publication_source_evidence_one_per_run_key
        UNIQUE (mirror_rebuild_run_id),
    CONSTRAINT parcel_publication_source_evidence_run_status_check
        CHECK (mirror_rebuild_run_status = 'succeeded'),
    CONSTRAINT parcel_publication_source_evidence_rejected_count_check
        CHECK (mirror_rebuild_rejected_row_count = 0),
    CONSTRAINT parcel_publication_source_evidence_table_check
        CHECK (iceberg_logical_table = 'silver.parcel_boundaries'),
    CONSTRAINT parcel_publication_source_evidence_snapshot_check
        CHECK (iceberg_snapshot_id > 0),
    CONSTRAINT parcel_publication_source_evidence_execution_schema_check
        CHECK (
            execution_evidence_schema_version =
                'foundation-platform.parcel_publication_execution_evidence.v1'
        ),
    CONSTRAINT parcel_publication_source_evidence_quality_schema_check
        CHECK (quality_schema_version = 'foundation-platform.parcel_publication_quality.v1'),
    CONSTRAINT parcel_publication_source_evidence_execution_object_key_check
        CHECK (
            execution_evidence_object_key <> ''
            AND execution_evidence_object_key !~~ '/%'
            AND execution_evidence_object_key !~~ '%//%'
            AND execution_evidence_object_key !~~ '%/./%'
            AND execution_evidence_object_key !~~ './%'
            AND execution_evidence_object_key !~~ '%/.'
            AND execution_evidence_object_key !~~ '%/../%'
            AND execution_evidence_object_key !~~ '../%'
            AND execution_evidence_object_key !~~ '%/..'
            AND position('\' IN execution_evidence_object_key) = 0
        ),
    CONSTRAINT parcel_publication_source_evidence_execution_sha256_check
        CHECK (execution_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT parcel_publication_source_evidence_source_row_count_check
        CHECK (source_row_count > 0),
    CONSTRAINT parcel_publication_source_evidence_projection_sha256_check
        CHECK (projection_content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT parcel_publication_source_evidence_run_tuple_fkey
        FOREIGN KEY (
            mirror_rebuild_run_id,
            mirror_rebuild_run_status,
            mirror_source_snapshot_id,
            iceberg_logical_table,
            source_record_id,
            source_file_asset_id,
            source_row_count,
            mirror_rebuild_rejected_row_count
        )
        REFERENCES serving_postgis.parcel_boundary_mirror_rebuild_run (
            id,
            status,
            source_snapshot_id,
            source_table,
            source_record_id,
            source_file_asset_id,
            loaded_row_count,
            rejected_row_count
        ) ON DELETE RESTRICT,
    CONSTRAINT parcel_publication_source_evidence_source_record_fkey
        FOREIGN KEY (source_record_id)
        REFERENCES catalog.source_record(id) ON DELETE RESTRICT,
    CONSTRAINT parcel_publication_source_evidence_source_file_asset_fkey
        FOREIGN KEY (source_file_asset_id, source_record_id)
        REFERENCES catalog.file_asset(id, source_record_id) ON DELETE RESTRICT
);

COMMENT ON TABLE catalog.parcel_publication_source_evidence IS
    'Append-only binding from one succeeded parcel mirror run to one verified Iceberg snapshot, Catalog provenance pair, execution object, quality dialect, row count, and projection-content digest (ADR-0025).';

CREATE OR REPLACE FUNCTION catalog.reject_parcel_publication_source_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'parcel publication source evidence is append-only'
        USING ERRCODE = '55000';
END;
$function$;

CREATE TRIGGER parcel_publication_source_evidence_append_only
BEFORE UPDATE OR DELETE ON catalog.parcel_publication_source_evidence
FOR EACH ROW EXECUTE FUNCTION catalog.reject_parcel_publication_source_evidence_mutation();

-- Cross-row validation runs at the append boundary. The composite FK owns tuple equality; this
-- trigger additionally proves that the run-keyed source set still exists, carries the same Catalog
-- provenance on every row, and has the complete quality object the evidence claims to have read.
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
       OR run_quality ->> 'geometry_repair_strategy' IS DISTINCT FROM 'postgis-make-valid-v1'
    THEN
        RAISE EXCEPTION 'source evidence requires a complete parcel publication quality report'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER parcel_publication_source_evidence_validate
BEFORE INSERT ON catalog.parcel_publication_source_evidence
FOR EACH ROW EXECUTE FUNCTION catalog.validate_parcel_publication_source_evidence();

-- A run may accumulate counts and quality while planned/running. Once it reaches a terminal state,
-- no column may change and the row may not disappear; this removes the validate-then-copy TOCTOU.
CREATE OR REPLACE FUNCTION serving_postgis.protect_parcel_boundary_mirror_rebuild_run()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, pg_temp
AS $function$
BEGIN
    IF OLD.status IN ('succeeded', 'failed', 'cancelled') THEN
        RAISE EXCEPTION 'terminal parcel mirror rebuild run is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.status IS DISTINCT FROM OLD.status
       AND NOT (
           (OLD.status = 'planned' AND NEW.status IN ('running', 'failed', 'cancelled'))
           OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'cancelled'))
       )
    THEN
        RAISE EXCEPTION 'invalid parcel mirror rebuild run state transition: % -> %',
            OLD.status, NEW.status
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard
BEFORE UPDATE OR DELETE ON serving_postgis.parcel_boundary_mirror_rebuild_run
FOR EACH ROW EXECUTE FUNCTION serving_postgis.protect_parcel_boundary_mirror_rebuild_run();

-- Legacy QA rebuilds may continue replacing unsealed rows. Once a production evidence row exists,
-- every mutation takes the parent-run lock used by the evidence validator, waits out a concurrent
-- seal, and then refuses to change that sealed source set.
CREATE OR REPLACE FUNCTION serving_postgis.protect_sealed_parcel_boundary_mirror_row()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, catalog, pg_temp
AS $function$
DECLARE
    old_run_id uuid;
    new_run_id uuid;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_run_id := OLD.rebuild_run_id;
        PERFORM 1
          FROM serving_postgis.parcel_boundary_mirror_rebuild_run
         WHERE id = old_run_id
         FOR SHARE;
        IF EXISTS (
            SELECT 1 FROM catalog.parcel_publication_source_evidence
             WHERE mirror_rebuild_run_id = old_run_id
        ) THEN
            RAISE EXCEPTION 'sealed parcel mirror source rows are append-only'
                USING ERRCODE = '55000';
        END IF;
    END IF;

    IF TG_OP <> 'DELETE' THEN
        new_run_id := NEW.rebuild_run_id;
        IF new_run_id IS DISTINCT FROM old_run_id THEN
            PERFORM 1
              FROM serving_postgis.parcel_boundary_mirror_rebuild_run
             WHERE id = new_run_id
             FOR SHARE;
        END IF;
        IF EXISTS (
            SELECT 1 FROM catalog.parcel_publication_source_evidence
             WHERE mirror_rebuild_run_id = new_run_id
        ) THEN
            RAISE EXCEPTION 'sealed parcel mirror source rows are append-only'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER parcel_boundary_mirror_sealed_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON serving_postgis.parcel_boundary_mirror
FOR EACH ROW EXECUTE FUNCTION serving_postgis.protect_sealed_parcel_boundary_mirror_row();

CREATE OR REPLACE FUNCTION serving_postgis.reject_sealed_parcel_boundary_mirror_truncate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, catalog, pg_temp
AS $function$
BEGIN
    IF EXISTS (SELECT 1 FROM catalog.parcel_publication_source_evidence) THEN
        RAISE EXCEPTION 'sealed parcel mirror source rows are append-only'
            USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE TRIGGER parcel_boundary_mirror_sealed_truncate_guard
BEFORE TRUNCATE ON serving_postgis.parcel_boundary_mirror
FOR EACH STATEMENT EXECUTE FUNCTION serving_postgis.reject_sealed_parcel_boundary_mirror_truncate();

ALTER TABLE serving_postgis.spatial_projection_load
    ADD COLUMN source_evidence_id uuid,
    ADD CONSTRAINT spatial_projection_load_source_evidence_fkey
        FOREIGN KEY (source_evidence_id)
        REFERENCES catalog.parcel_publication_source_evidence(id) ON DELETE RESTRICT;

-- A non-null binding has to agree with the load's unit/snapshot and the revision's Catalog source.
-- Nullable is retained for historical and non-parcel loads; the runtime gate below makes NULL
-- non-publishable for parcels. A terminal NULL cannot be decorated with evidence after the copy.
CREATE OR REPLACE FUNCTION serving_postgis.validate_spatial_projection_load_source_evidence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, catalog, pg_temp
AS $function$
DECLARE
    unit_key text;
    revision_source_record_id uuid;
    evidence_snapshot_id text;
    evidence_source_record_id uuid;
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.status IN ('succeeded', 'failed')
       AND NEW.source_evidence_id IS DISTINCT FROM OLD.source_evidence_id
    THEN
        RAISE EXCEPTION 'terminal spatial projection load evidence binding is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.source_evidence_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT unit.unit_key, revision.source_record_id
      INTO unit_key, revision_source_record_id
      FROM catalog.vector_tile_publication_unit AS unit
      JOIN catalog.publication_revision AS revision
        ON revision.id = NEW.data_revision
       AND revision.publication_unit_id = NEW.publication_unit_id
       AND revision.canonical_iceberg_snapshot_id = NEW.canonical_iceberg_snapshot_id
     WHERE unit.id = NEW.publication_unit_id;
    SELECT evidence.canonical_iceberg_snapshot_id, evidence.source_record_id
      INTO evidence_snapshot_id, evidence_source_record_id
      FROM catalog.parcel_publication_source_evidence AS evidence
     WHERE evidence.id = NEW.source_evidence_id
     FOR KEY SHARE;

    IF unit_key IS DISTINCT FROM 'parcels'
       OR revision_source_record_id IS NULL
       OR evidence_snapshot_id IS DISTINCT FROM NEW.canonical_iceberg_snapshot_id
       OR evidence_source_record_id IS DISTINCT FROM revision_source_record_id
    THEN
        RAISE EXCEPTION 'parcel projection load does not match its sealed source evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER spatial_projection_load_source_evidence_guard
BEFORE INSERT OR UPDATE OF source_evidence_id, publication_unit_id, data_revision,
    canonical_iceberg_snapshot_id, status
ON serving_postgis.spatial_projection_load
FOR EACH ROW EXECUTE FUNCTION serving_postgis.validate_spatial_projection_load_source_evidence();

-- The effective promotion function is replaced whole because later migrations replace earlier
-- bodies. Its existing succeeded/unit/revision/snapshot gate stays intact; the additional parcels
-- clause requires that exact load to reach a sealed evidence row whose source matches its revision.
CREATE OR REPLACE FUNCTION catalog.promote_vector_tile_runtime_manifest(
    expected_manifest_id uuid,
    next_manifest_id uuid
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
    current_manifest_id uuid;
    current_generation bigint;
    next_generation bigint;
    next_unit_count bigint;
    publication_unit_count bigint;
    updated_unit_count bigint;
BEGIN
    LOCK TABLE catalog.vector_tile_runtime_manifest_pointer IN SHARE ROW EXCLUSIVE MODE;

    SELECT manifest_id
      INTO current_manifest_id
      FROM catalog.vector_tile_runtime_manifest_pointer
     WHERE singleton = true
     FOR UPDATE;

    IF (expected_manifest_id IS NULL AND current_manifest_id IS NOT NULL)
       OR (expected_manifest_id IS NOT NULL AND current_manifest_id IS DISTINCT FROM expected_manifest_id) THEN
        RAISE EXCEPTION 'vector tile runtime manifest compare-and-swap conflict: expected %, current %',
            expected_manifest_id, current_manifest_id
            USING ERRCODE = '40001';
    END IF;

    SELECT manifest_generation
      INTO next_generation
      FROM catalog.vector_tile_runtime_manifest
     WHERE id = next_manifest_id;
    IF next_generation IS NULL THEN
        RAISE EXCEPTION 'vector tile runtime manifest % does not exist', next_manifest_id
            USING ERRCODE = '23503';
    END IF;

    SELECT count(*)
      INTO next_unit_count
      FROM catalog.vector_tile_runtime_manifest_unit
     WHERE manifest_id = next_manifest_id;
    SELECT count(*)
      INTO publication_unit_count
      FROM catalog.vector_tile_publication_unit;
    IF next_unit_count = 0 OR next_unit_count <> publication_unit_count THEN
        RAISE EXCEPTION 'runtime manifest % is not a complete publication', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND release.source_kind = 'static_pmtiles'
           AND (
               release.martin_source_id <> format('%s-%s', unit.unit_key, release.id)
               OR release.pmtiles_object_key
                  <> format('gold/vector-tiles/releases/%s.pmtiles', release.martin_source_id)
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % has a non-release-addressed static PMTiles source', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
          LEFT JOIN serving_postgis.spatial_projection_load AS load
            ON load.id = release.postgis_projection_revision
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND release.source_kind = 'dynamic_postgis'
           AND (
               load.id IS NULL
               OR load.status <> 'succeeded'
               OR load.publication_unit_id <> unit.id
               OR load.data_revision <> manifest_unit.data_revision
               OR load.canonical_iceberg_snapshot_id <> manifest_unit.canonical_iceberg_snapshot_id
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % selects a dynamic source with no succeeded PostGIS projection load', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
          LEFT JOIN serving_postgis.spatial_projection_load AS load
            ON load.id = release.postgis_projection_revision
          LEFT JOIN catalog.parcel_publication_source_evidence AS evidence
            ON evidence.id = load.source_evidence_id
          LEFT JOIN catalog.publication_revision AS revision
            ON revision.id = load.data_revision
           AND revision.publication_unit_id = load.publication_unit_id
           AND revision.canonical_iceberg_snapshot_id = load.canonical_iceberg_snapshot_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND unit.unit_key = 'parcels'
           AND release.source_kind = 'dynamic_postgis'
           AND (
               evidence.id IS NULL
               OR evidence.canonical_iceberg_snapshot_id <> load.canonical_iceberg_snapshot_id
               OR evidence.source_record_id <> revision.source_record_id
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % selects a parcels load without sealed parcel publication evidence',
            next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND unit.active_release_id IS NULL
           AND release.source_kind <> 'dynamic_postgis'
    ) THEN
        RAISE EXCEPTION 'the first runtime publication must be dynamic PostGIS'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND unit.active_release_id IS NOT NULL
           AND release.source_kind = 'static_pmtiles'
           AND manifest_unit.data_revision <> unit.active_data_revision
    ) THEN
        RAISE EXCEPTION 'static PMTiles must use the currently selected data revision'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
          JOIN catalog.vector_tile_release AS active
            ON active.id = unit.active_release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND release.source_kind = 'dynamic_postgis'
           AND manifest_unit.canonical_iceberg_snapshot_id::numeric
               < active.canonical_iceberg_snapshot_id::numeric
    ) THEN
        RAISE EXCEPTION 'runtime manifest % moves a dynamic unit to an older canonical snapshot', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND (
               (unit.active_release_id IS NULL AND manifest_unit.serving_generation <> 1)
               OR
               (unit.active_release_id IS NOT NULL
                AND manifest_unit.release_id = unit.active_release_id
                AND manifest_unit.serving_generation <> unit.serving_generation)
               OR
               (unit.active_release_id IS NOT NULL
                AND manifest_unit.release_id <> unit.active_release_id
                AND manifest_unit.serving_generation <> unit.serving_generation + 1)
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % has a serving-generation gap', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF current_manifest_id IS NOT NULL THEN
        SELECT manifest_generation
          INTO current_generation
          FROM catalog.vector_tile_runtime_manifest
         WHERE id = current_manifest_id;
        IF next_generation <= current_generation THEN
            RAISE EXCEPTION 'runtime manifest generation must increase: current %, next %',
                current_generation, next_generation
                USING ERRCODE = '40001';
        END IF;
    END IF;

    UPDATE catalog.vector_tile_publication_unit AS unit
       SET active_release_id = manifest_unit.release_id,
           active_data_revision = manifest_unit.data_revision,
           serving_generation = manifest_unit.serving_generation,
           fallback_release_id = CASE
               WHEN unit.fallback_data_revision = manifest_unit.data_revision
               THEN unit.fallback_release_id
               ELSE NULL
           END,
           fallback_data_revision = CASE
               WHEN unit.fallback_data_revision = manifest_unit.data_revision
               THEN unit.fallback_data_revision
               ELSE NULL
           END,
           version = unit.version + 1,
           updated_at = now()
      FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
     WHERE manifest_unit.manifest_id = next_manifest_id
       AND manifest_unit.publication_unit_id = unit.id;
    GET DIAGNOSTICS updated_unit_count = ROW_COUNT;
    IF updated_unit_count <> publication_unit_count THEN
        RAISE EXCEPTION 'runtime manifest % does not select every publication unit', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO catalog.vector_tile_runtime_manifest_pointer (singleton, manifest_id, updated_at)
    VALUES (true, next_manifest_id, now())
    ON CONFLICT (singleton) DO UPDATE
        SET manifest_id = EXCLUDED.manifest_id,
            updated_at = EXCLUDED.updated_at;

    RETURN next_generation;
END;
$function$;
