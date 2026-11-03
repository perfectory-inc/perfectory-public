-- 루트 ADR-0374: 지도 유닛은 첫 lakehouse 굽기로 태어날 수 있다.
--
-- 지금까지 유닛의 첫 공개는 PostGIS 동적 릴리스뿐이었다. 그 뒤에야 정적 릴리스와 lakehouse 굽기가
-- 그것을 갈아 끼운다. PostGIS 서빙 투영은 걷어내는 중이라(루트 ADR-0108) 새 유닛(규제 구역 `zoning`,
-- 루트 ADR-0373)이 그 길을 다시 밟으면 없어질 계층을 하나 더 만든다. 그래서 유닛 한 줄에 태어난 길을
-- 적고(`born_from`), 그 길이 `lakehouse_bake` 인 유닛만 첫 공개를 정적 PMTiles 로 받는다. 다른 규칙은
-- 하나도 바뀌지 않는다: 매니페스트는 여전히 모든 유닛을 덮고, 정적 릴리스는 릴리스 주소여야 하고,
-- 새 유닛의 serving_generation 은 1 이다. 유닛 줄과 첫 릴리스와 매니페스트는 한 트랜잭션에서 생긴다 —
-- 릴리스 없는 유닛 줄이 먼저 커밋되면 다른 모든 유닛의 승격이 "complete publication" 에서 막힌다.

ALTER TABLE catalog.vector_tile_publication_unit
    ADD COLUMN born_from text,
    ADD CONSTRAINT vector_tile_publication_unit_born_from_check
        CHECK (born_from IS NULL OR born_from = 'lakehouse_bake');

COMMENT ON COLUMN catalog.vector_tile_publication_unit.born_from IS
    'NULL: the unit''s first publication was dynamic PostGIS. lakehouse_bake: registered by its first lakehouse bake (root ADR-0374).';

ALTER TABLE catalog.catalog_mutation_idempotency
    DROP CONSTRAINT catalog_mutation_idempotency_command_kind_check,
    ADD CONSTRAINT catalog_mutation_idempotency_command_kind_check CHECK (
        command_kind IN ('mark_tile_layer_dynamic', 'start_vector_tile_build',
            'start_static_release_readdress', 'start_lakehouse_bake',
            'promote_tile_layer_static', 'rollback_tile_layer_source',
            'register_lakehouse_tile_unit')
    ),
    DROP CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check,
    ADD CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check CHECK (
        (command_kind IN ('mark_tile_layer_dynamic', 'promote_tile_layer_static', 'rollback_tile_layer_source',
            'register_lakehouse_tile_unit')
            AND outcome_manifest_id IS NOT NULL)
        OR (command_kind IN ('start_vector_tile_build', 'start_static_release_readdress', 'start_lakehouse_bake')
            AND outcome_manifest_id IS NULL)
    );

-- The effective promotion function is replaced whole because later migrations replace earlier
-- bodies. One rule changes: a unit born from a lakehouse bake may start static. Every other rule is
-- the body of 20260928120000_lakehouse_bake.sql unchanged.
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
           -- Root ADR-0374: a unit registered by its first lakehouse bake is born static.
           AND unit.born_from IS DISTINCT FROM 'lakehouse_bake'
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
