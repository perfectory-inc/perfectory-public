-- Closes a bypass the promotion gate claimed to close.
--
-- `catalog.promote_vector_tile_runtime_manifest` says it "repeats the domain state machine so a
-- direct SQL caller cannot bypass it", but its static-PMTiles identity check compared only the tail
-- of `pmtiles_object_key` against `{martin_source_id}.pmtiles`. The prefix was unconstrained, so a
-- release addressed at any path with the right filename passed the gate. The Rust validator had the
-- same leniency, which is how the documented layout (`releases/{release_id}/…`) and the shipped
-- layout (`releases/…`) disagreed for as long as they did without either failing.
--
-- `catalog_domain::static_release_pmtiles_object_key` is now the single definition of that layout and
-- compares the whole key. This replaces the function body so the database gate is not weaker than
-- the code path it exists to backstop. Nothing else in the function changes.
--
-- The literal root is repeated here rather than derived: a SQL function cannot call into the Rust
-- crate, and `docs/adr/0002` fixes `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX` to this value.
-- `vector_tile_release_object_key_prefix_matches_domain` in the integration suite is what keeps the
-- two statements from drifting.

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
    -- Lock the singleton relation itself so two bootstrap CAS calls cannot both observe an empty
    -- table and race into a unique-key error without a deterministic compare-and-swap boundary.
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

    -- Martin discovers remote PMTiles source IDs from filename stems. Keep that derived identity
    -- deterministic at the database promotion boundary: one unit/release has exactly one route, at
    -- exactly one object address. The object key is compared whole, not by filename suffix.
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

    -- The database gate repeats the domain state machine so a direct SQL caller cannot bypass it.
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
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND (
               (unit.active_release_id IS NULL AND manifest_unit.serving_generation <> 1)
               OR
               (unit.active_release_id IS NOT NULL
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
