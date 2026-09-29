-- ADR-0112 §7·§9: 레이크하우스 굽기. 원천과 관리자 편집을 합친 Gold 스냅숏에서 정적 PMTiles 를
-- 굽고, 현재 서빙 중인 정적 release 를 입력으로 받아 새 data revision 으로 승격한다.
-- dynamic 입력이 없으므로 승격 뒤 fallback 은 남지 않는다(게이트의 CASE 가 revision 변경으로 비운다).
--
-- 새 revision 은 입력 revision 과 같은 수집 원천(bronze_object 또는 source_record)에 묶이고,
-- canonical snapshot 만 Gold 스냅숏으로 바뀐다. 한 unit 의 한 snapshot 에는 revision 이 하나뿐이라는
-- publication_revision 의 규칙은 그대로다.
ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN output_data_revision uuid,
    DROP CONSTRAINT vector_tile_build_job_kind_check,
    ADD CONSTRAINT vector_tile_build_job_kind_check
        CHECK (kind IN ('bake', 'readdress', 'lakehouse_bake')),
    DROP CONSTRAINT vector_tile_build_job_readdress_observation_check,
    ADD CONSTRAINT vector_tile_build_job_readdress_observation_check CHECK (
        (kind = 'bake' AND input_serving_generation IS NULL AND readdress_tiles_base_url IS NULL)
        OR (kind = 'readdress'
            AND input_serving_generation IS NOT NULL
            AND input_serving_generation BETWEEN 1 AND 9007199254740991
            AND readdress_tiles_base_url IS NOT NULL
            AND btrim(readdress_tiles_base_url) <> '')
        OR (kind = 'lakehouse_bake'
            AND input_serving_generation IS NOT NULL
            AND input_serving_generation BETWEEN 1 AND 9007199254740991
            AND readdress_tiles_base_url IS NULL)
    ),
    ADD CONSTRAINT vector_tile_build_job_output_revision_check
        CHECK ((kind = 'lakehouse_bake') = (output_data_revision IS NOT NULL)),
    -- The output revision is a real ledger row of this unit over exactly the snapshot the build froze.
    ADD CONSTRAINT vector_tile_build_job_output_revision_fkey
        FOREIGN KEY (output_data_revision, publication_unit_id, frozen_source_snapshot_id)
        REFERENCES catalog.publication_revision (id, publication_unit_id, canonical_iceberg_snapshot_id);

-- 입력은 시작 시점의 active 검증 정적 release 와 그 generation 이어야 하고, 굽는 snapshot 은
-- 입력의 snapshot 과 달라야 한다(같으면 새 revision 을 만들 수 없고 굽기도 필요 없다).
-- 출력 revision 은 입력 revision 과 같은 수집 원천에 묶여야 한다. 입력은 이후 불변이다.
CREATE FUNCTION catalog.guard_vector_tile_lakehouse_bake_build()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
    selected_release uuid;
    selected_generation bigint;
    source catalog.vector_tile_release%ROWTYPE;
    input_revision catalog.publication_revision%ROWTYPE;
    output_revision catalog.publication_revision%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.kind = 'lakehouse_bake' OR NEW.kind = 'lakehouse_bake') AND
           ROW(NEW.kind, NEW.publication_unit_id, NEW.input_release_id, NEW.input_data_revision,
               NEW.frozen_source_snapshot_id, NEW.input_serving_generation,
               NEW.output_data_revision, NEW.idempotency_key)
           IS DISTINCT FROM
           ROW(OLD.kind, OLD.publication_unit_id, OLD.input_release_id, OLD.input_data_revision,
               OLD.frozen_source_snapshot_id, OLD.input_serving_generation,
               OLD.output_data_revision, OLD.idempotency_key) THEN
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
    IF input_revision.id IS NULL OR output_revision.id IS NULL
       OR output_revision.source_record_id IS DISTINCT FROM input_revision.source_record_id
       OR output_revision.bronze_object_id IS DISTINCT FROM input_revision.bronze_object_id THEN
        RAISE EXCEPTION 'lakehouse bake output revision must keep its input''s collected source'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

CREATE TRIGGER vector_tile_build_job_lakehouse_bake_guard
    BEFORE INSERT OR UPDATE ON catalog.vector_tile_build_job
    FOR EACH ROW EXECUTE FUNCTION catalog.guard_vector_tile_lakehouse_bake_build();

ALTER TABLE catalog.catalog_mutation_idempotency
    DROP CONSTRAINT catalog_mutation_idempotency_command_kind_check,
    ADD CONSTRAINT catalog_mutation_idempotency_command_kind_check CHECK (
        command_kind IN ('mark_tile_layer_dynamic', 'start_vector_tile_build',
            'start_static_release_readdress', 'start_lakehouse_bake',
            'promote_tile_layer_static', 'rollback_tile_layer_source')
    ),
    DROP CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check,
    ADD CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check CHECK (
        (command_kind IN ('mark_tile_layer_dynamic', 'promote_tile_layer_static', 'rollback_tile_layer_source')
            AND outcome_manifest_id IS NOT NULL)
        OR (command_kind IN ('start_vector_tile_build', 'start_static_release_readdress', 'start_lakehouse_bake')
            AND outcome_manifest_id IS NULL)
    );

-- The effective promotion function is replaced whole because later migrations replace earlier
-- bodies. One rule changes: a static release may move its unit to a new data revision only when a
-- lakehouse bake produced it for exactly that revision. Every other rule is unchanged.
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
           -- ADR-0112: the one static release that may carry a new revision is the one a lakehouse
           -- bake produced for exactly that revision.
           AND NOT EXISTS (
               SELECT 1
                 FROM catalog.vector_tile_build_job AS build
                WHERE build.result_release_id = release.id
                  AND build.kind = 'lakehouse_bake'
                  AND build.output_data_revision = manifest_unit.data_revision
                  AND build.frozen_source_snapshot_id = manifest_unit.canonical_iceberg_snapshot_id
           )
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
