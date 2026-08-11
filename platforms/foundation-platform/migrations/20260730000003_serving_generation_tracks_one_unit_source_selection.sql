-- Makes `serving_generation` mean what the model says it means.
--
-- The gap check required EVERY unit a manifest selects to carry
-- `unit.serving_generation + 1`, including units whose serving source did not change. A manifest
-- that switches one unit must still select all of them (`next_unit_count = publication_unit_count`),
-- so one edit advanced every unit's generation.
--
-- That contradicts why the column exists. `docs/guides/single-source-spatial-publication-implementation.md`
-- Task 6 Step 5 introduced a per-unit `expected_serving_generation` instead of a global manifest
-- version because a same-revision rollback re-activates a *preserved* release: one
-- `active_release_id` can be the selected one at two different generations, so the release id alone
-- cannot tell those states apart. The generation therefore identifies **one unit's source
-- selection**. A carried-forward unit's selection is unchanged, so advancing it asserts a change
-- that did not happen.
--
-- Two concrete costs, not a purity argument:
--
--   * `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md` has
--     clients poll and compare `serving_generation` per unit to decide which unit to refetch. When
--     every unit advances on every publication, every unit invalidates on every publication and the
--     per-unit generation carries no information the global `manifest_generation` does not.
--   * Task 6 Step 1 requires two edits to *different* units to both commit with the global
--     generation advancing twice in order. They could not: the second writer's
--     `expected_serving_generation` for its own unit had already been bumped by the first writer's
--     publication of an unrelated unit, so it lost a compare-and-swap it was not in.
--
-- The rule is narrowed, not removed. Gap detection is what stops a manifest assembled from stale
-- unit state, and it still does: a carried unit whose generation differs from the unit's current one
-- is refused exactly as before. Only the *expected value* changes, and only for a unit that
-- re-selects the release it already serves.
--
--   never published        -> 1
--   release changed        -> unit.serving_generation + 1
--   release re-selected    -> unit.serving_generation   (unchanged)
--
-- Nothing else in the function changes. The object-key literal is repeated here for the same reason
-- as in 20260730000001 — plpgsql cannot call into the Rust crate — and
-- `the_promotion_gate_and_the_domain_agree_on_the_release_object_root` reads `pg_proc.prosrc` to keep
-- the two statements from drifting.

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

    -- A unit advances its serving generation when its selected release changes, and holds it when
    -- the manifest re-selects the release the unit already serves. Both arms still pin the expected
    -- value exactly, so a manifest assembled from stale unit state is refused either way.
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
