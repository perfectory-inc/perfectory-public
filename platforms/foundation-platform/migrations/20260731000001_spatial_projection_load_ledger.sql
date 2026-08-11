-- Gives the PostGIS projection an identity, and makes the promotion gate require one.
--
-- `catalog.vector_tile_release.postgis_projection_revision` has existed since 20260724000001 and
-- nothing has ever produced a value for it. The local seed invents a UUID; the operator promote
-- binds `data_revision` into it twice (`VALUES (..., $7, $3)`). Both are fabrications, and both are
-- now inside the idempotency request fingerprint, so the fabrication was about to become an
-- invariant.
--
-- Why the field cannot simply equal `data_revision`: re-materialising one revision already happens
-- here, two ways, neither of them recorded.
--
--   * the v2 seed replaces geometry under an already-promoted revision
--     (`ON CONFLICT (data_revision, pnu) DO UPDATE`), leaving the release and the manifest frozen;
--   * `publish-administrative-boundary-postgis` is `ON CONFLICT DO NOTHING` and its completeness
--     check reads the table it just skipped, so a re-publish with changed source geometry prints
--     `-ok` having written nothing.
--
-- An earlier draft of this header offered a third: that `scripts/tiles/tiles-slice-proof.sh` runs its
-- loader twice on purpose. It does — but the doubled loop at `tiles-slice-proof.sh` runs
-- `scripts/tiles/fixture.sql`, which loads the **v1** `serving_postgis.parcel_boundary_mirror`, and
-- the v2 seed below it runs exactly once. That is not an unrecorded re-materialisation. It is the
-- opposite, and it is the argument: the mirror it reloads is the table that already has
-- `parcel_boundary_mirror_rebuild_run`, so the double load is two rows there and nobody has to guess.
--
-- Two loads of one revision are two facts. A column whose value is the revision cannot tell them
-- apart, so it cannot witness either. The industry evidence is genuinely split — PostgreSQL
-- deliberately preserves a matview's identity across `REFRESH`, and Martin's own `agg_tiles_hash` is
-- a content hash that is identical across a byte-identical rebuild — so the argument here rests on
-- this repository, not on precedent elsewhere. And this repository already answered it once: the v1
-- mirror has `serving_postgis.parcel_boundary_mirror_rebuild_run`, minted per rebuild with counts and
-- status. The v2 tables are the ones missing it.
--
-- The ledger ships with its reader, deliberately. `20260730000004` records why: "pre-shaping their
-- storage is how vector_tile_build_job became a table nothing writes." The reader is the gate at the
-- bottom of this file, and it closes a hole worth more than the naming question — until now a
-- manifest could be promoted for a revision whose projection was never loaded at all, leaving the
-- pointer claiming a unit is live and dynamic while Martin's view returns zero features.

CREATE TABLE serving_postgis.spatial_projection_load (
    id uuid PRIMARY KEY,
    publication_unit_key text NOT NULL,
    data_revision uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    status text NOT NULL,
    loaded_row_count bigint NOT NULL DEFAULT 0,
    rejected_row_count bigint NOT NULL DEFAULT 0,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- Same regex as vector_tile_publication_unit_key_check: a load names the unit it materialises,
    -- and the two spellings must not drift.
    CONSTRAINT spatial_projection_load_unit_key_check CHECK (publication_unit_key ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'),
    CONSTRAINT spatial_projection_load_snapshot_check CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$'),
    CONSTRAINT spatial_projection_load_status_check CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT spatial_projection_load_counts_check CHECK (loaded_row_count >= 0 AND rejected_row_count >= 0),
    CONSTRAINT spatial_projection_load_finished_check CHECK (finished_at IS NULL OR finished_at >= started_at),
    -- A succeeded load that materialised nothing is a contradiction, and it is exactly the state the
    -- gate below exists to refuse. Shape copied from parcel_boundary_mirror_rebuild_run_check1.
    CONSTRAINT spatial_projection_load_succeeded_check CHECK (
        status <> 'succeeded' OR (finished_at IS NOT NULL AND loaded_row_count > 0)
    ),
    CONSTRAINT spatial_projection_load_failed_check CHECK (status <> 'failed' OR error_message IS NOT NULL)
);

CREATE INDEX spatial_projection_load_unit_revision_idx
    ON serving_postgis.spatial_projection_load (publication_unit_key, data_revision, started_at DESC);

-- Standing in for the loads nobody recorded. Deterministic id so re-running the migration on a
-- restored dump produces the same row, following the legacy-row precedent in 20260727000001.
-- `status` is derived from what is actually in the table rather than assumed: a revision with rows
-- succeeded, and one without them did not — which is the honest reading of a load nobody logged.
--
-- Grouped by `(data_revision, canonical_iceberg_snapshot_id)`, not by revision alone, and the id
-- derives from both. The snapshot is `text`, so an aggregate over a revision whose rows disagree
-- would pick lexicographically — `'9'` beats `'841361364657368624'` — and record a snapshot no load
-- ever used. Grouping instead means disagreement produces two loads and says so.
--
-- `started_at` is supplied rather than defaulted. The column defaults to `now()`, which is the
-- migration's own clock, and `finished_at` comes from rows written long before it — so the default
-- would put every backfilled row in violation of `spatial_projection_load_finished_check`.
INSERT INTO serving_postgis.spatial_projection_load
    (id, publication_unit_key, data_revision, canonical_iceberg_snapshot_id, status,
     loaded_row_count, started_at, finished_at, error_message)
SELECT
    md5('legacy:spatial-projection-load:parcels:' || publication.data_revision::text || ':'
        || publication.canonical_iceberg_snapshot_id)::uuid,
    'parcels',
    publication.data_revision,
    publication.canonical_iceberg_snapshot_id,
    'succeeded',
    count(*),
    min(publication.loaded_at),
    max(publication.loaded_at),
    NULL
FROM serving_postgis.parcel_boundary_publication AS publication
GROUP BY publication.data_revision, publication.canonical_iceberg_snapshot_id;

INSERT INTO serving_postgis.spatial_projection_load
    (id, publication_unit_key, data_revision, canonical_iceberg_snapshot_id, status,
     loaded_row_count, started_at, finished_at, error_message)
SELECT
    md5('legacy:spatial-projection-load:admin:' || publication.data_revision::text || ':'
        || publication.canonical_iceberg_snapshot_id)::uuid,
    'admin',
    publication.data_revision,
    publication.canonical_iceberg_snapshot_id,
    'succeeded',
    count(*),
    min(publication.loaded_at),
    max(publication.loaded_at),
    NULL
FROM serving_postgis.administrative_unit_boundary_publication AS publication
GROUP BY publication.data_revision, publication.canonical_iceberg_snapshot_id;

-- The publication rows are re-keyed by load, not by revision. That is what turns a second load of
-- one revision from an in-place overwrite (parcels) or a silent no-op (admin) into a second row set
-- that can be served, compared, or discarded on its own.
--
-- "Discarded" names no mechanism, and this is the place to say so. Nothing prunes either publication
-- table today — `serving_postgis.administrative_unit_boundary_publication` cannot even be deleted
-- from without the publisher capability, because `administrative_boundary_publication_append_only`
-- covers DELETE. Before this change a re-load of one revision overwrote in place and the tables held
-- one row set per revision; after it they hold one row set per *load*, and these rows are geometry,
-- not ledger keys. `20260730000004` declined a retention sweep on the grounds that no `catalog.*`
-- table has one and the rows are small; the second half of that argument does not carry over here.
-- No sweeper ships with this migration — the growth is per operator-initiated load, not per request,
-- so it is bounded by how often someone publishes. The trigger to revisit is the first unit that
-- reloads on a schedule rather than on an operator's command.
ALTER TABLE serving_postgis.parcel_boundary_publication
    ADD COLUMN projection_load_id uuid;
UPDATE serving_postgis.parcel_boundary_publication
   SET projection_load_id = md5('legacy:spatial-projection-load:parcels:' || data_revision::text || ':'
                                || canonical_iceberg_snapshot_id)::uuid;
ALTER TABLE serving_postgis.parcel_boundary_publication
    ALTER COLUMN projection_load_id SET NOT NULL,
    ADD CONSTRAINT parcel_boundary_publication_load_fkey
        FOREIGN KEY (projection_load_id) REFERENCES serving_postgis.spatial_projection_load(id),
    DROP CONSTRAINT parcel_boundary_publication_pkey,
    ADD CONSTRAINT parcel_boundary_publication_pkey PRIMARY KEY (projection_load_id, pnu);
-- Dropped, not re-created under the new key. `parcel_boundary_publication_data_revision_idx` was an
-- exact duplicate of the primary key's own unique index — same columns, same order — and recreating
-- it as `(projection_load_id, pnu)` would duplicate the new one just as exactly. This is the commit
-- that drops and would recreate it, so it is the commit that owns the choice. No lookup is lost.
DROP INDEX IF EXISTS serving_postgis.parcel_boundary_publication_data_revision_idx;

ALTER TABLE serving_postgis.administrative_unit_boundary_publication
    ADD COLUMN projection_load_id uuid;
-- `administrative_boundary_publication_append_only` fires BEFORE UPDATE and refuses every writer that
-- is not holding the publisher capability. Backfilling the new column is that writer, so take the
-- capability transaction-locally exactly as `administrative_boundary_postgis_publish.rs` does. Without
-- this line the migration passes on an empty database and fails on every database that has rows —
-- the UPDATE below is the only statement in this file that touches an already-published row.
SELECT set_config('foundation.temporal_publisher', 'on', true);
UPDATE serving_postgis.administrative_unit_boundary_publication
   SET projection_load_id = md5('legacy:spatial-projection-load:admin:' || data_revision::text || ':'
                                || canonical_iceberg_snapshot_id)::uuid;
ALTER TABLE serving_postgis.administrative_unit_boundary_publication
    ALTER COLUMN projection_load_id SET NOT NULL,
    ADD CONSTRAINT administrative_unit_boundary_publication_load_fkey
        FOREIGN KEY (projection_load_id) REFERENCES serving_postgis.spatial_projection_load(id),
    -- The constraint is named for the table's inline PRIMARY KEY clause, which Postgres derived from
    -- the CHECK-constraint prefix the table uses (`administrative_boundary_publication_*`) rather
    -- than from the relation name. Kept as found.
    DROP CONSTRAINT administrative_boundary_publication_pkey,
    ADD CONSTRAINT administrative_boundary_publication_pkey PRIMARY KEY (projection_load_id, administrative_unit_id),
    -- Re-keyed for the same reason as the primary key, and it has to move with it. Left at
    -- `(data_revision, ...)` this UNIQUE would still refuse a second load of one revision, which is
    -- the exact state this migration exists to make representable — the new primary key would admit
    -- the second row set and this constraint would reject it one statement later. Narrowing it to the
    -- load says what the projection can actually promise: within one materialisation a canonical code
    -- names one unit. Agreement *between* two loads of a revision is not a serving-projection
    -- invariant; `catalog.administrative_unit_identifier` owns code identity.
    DROP CONSTRAINT administrative_boundary_publication_code_key,
    ADD CONSTRAINT administrative_boundary_publication_code_key
        UNIQUE (projection_load_id, scope_kind, canonical_code);

-- Existing releases point at invented UUIDs. Repoint the dynamic ones at the legacy load for their
-- revision and snapshot, so the foreign key below can be declared.
UPDATE catalog.vector_tile_release AS release
   SET postgis_projection_revision = load.id
  FROM serving_postgis.spatial_projection_load AS load
  JOIN catalog.vector_tile_publication_unit AS unit ON unit.unit_key = load.publication_unit_key
 WHERE release.source_kind = 'dynamic_postgis'
   AND release.publication_unit_id = unit.id
   AND release.data_revision = load.data_revision
   AND release.canonical_iceberg_snapshot_id = load.canonical_iceberg_snapshot_id;

-- A dynamic release whose revision has no publication rows is repointed at nothing by the statement
-- above, and `vector_tile_release_source_fields_check` forbids nulling the column for a dynamic
-- release. Without this statement the foreign key below would abort the migration on exactly the
-- state the header says is universal today — every dynamic release carries a fabricated UUID — and
-- the operator would go looking at the promotion gate for an error the `ALTER TABLE` raised.
--
-- So give the fabricated id a row and say what it is: a load that did not succeed, because there is
-- no evidence it ever ran. The gate then refuses to promote it, which is the intended answer, and it
-- is the gate that says so. `DISTINCT ON` because two releases sharing one fabricated value must not
-- make this statement fail on its own primary key.
INSERT INTO serving_postgis.spatial_projection_load
    (id, publication_unit_key, data_revision, canonical_iceberg_snapshot_id, status, error_message)
SELECT DISTINCT ON (release.postgis_projection_revision)
    release.postgis_projection_revision,
    unit.unit_key,
    release.data_revision,
    release.canonical_iceberg_snapshot_id,
    'failed',
    'no publication rows were recorded for this revision; the projection load predates the ledger'
FROM catalog.vector_tile_release AS release
JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = release.publication_unit_id
WHERE release.source_kind = 'dynamic_postgis'
  AND release.postgis_projection_revision IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM serving_postgis.spatial_projection_load AS load
       WHERE load.id = release.postgis_projection_revision
  )
ORDER BY release.postgis_projection_revision, release.created_at;

ALTER TABLE catalog.vector_tile_release
    ADD CONSTRAINT vector_tile_release_projection_load_fkey
    FOREIGN KEY (postgis_projection_revision) REFERENCES serving_postgis.spatial_projection_load(id);

-- Martin reads these. They join through the release's projection load instead of through
-- `data_revision`, so the rows a tile is cut from are the exact rows the selected release names —
-- not "whatever is currently stored under that revision".
CREATE OR REPLACE VIEW serving_postgis.parcel_boundary_current AS
SELECT
    publication.pnu::text AS pnu,
    publication.official_complex_code,
    publication.geom::public.geometry(MultiPolygon, 5179) AS geom
FROM serving_postgis.parcel_boundary_publication AS publication
JOIN catalog.vector_tile_release AS release
  ON release.postgis_projection_revision = publication.projection_load_id
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.release_id = release.id
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
WHERE unit.unit_key = 'parcels'
  AND release.source_kind = 'dynamic_postgis';

CREATE OR REPLACE VIEW serving_postgis.administrative_unit_boundary_current AS
SELECT
    publication.administrative_unit_id::text AS administrative_unit_id,
    publication.scope_kind,
    publication.canonical_code,
    publication.display_name,
    publication.geom::public.geometry(MultiPolygon, 4326) AS geom
FROM serving_postgis.administrative_unit_boundary_publication AS publication
JOIN catalog.vector_tile_release AS release
  ON release.postgis_projection_revision = publication.projection_load_id
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.release_id = release.id
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
WHERE unit.unit_key = 'admin'
  AND release.source_kind = 'dynamic_postgis';

-- The reader. Until now a manifest could select a dynamic unit whose projection had never been
-- loaded: the gate never referenced `serving_postgis`, and neither did the operator command's input
-- verification. The pointer would claim the unit was live while Martin's view returned zero features,
-- and nothing anywhere would have said otherwise.
--
-- This is the payload of the change. The load identity is the mechanism; refusing to promote an
-- unbacked projection is the point.
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

    -- A dynamic unit serves rows out of PostGIS. Refuse to point at one whose projection load did
    -- not succeed, or whose release names a load for a different unit, revision or snapshot than the
    -- manifest selects — every one of those would leave the pointer claiming a source that serves
    -- nothing, or serves rows built from something other than what the manifest says it published.
    --
    -- The snapshot is compared for a second reason: it is the only reader the ledger's
    -- `canonical_iceberg_snapshot_id` has. A column three writers fill and nobody checks is the
    -- failure `20260730000004` names, and this file cites that lesson as its own justification.
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
               OR load.publication_unit_key <> unit.unit_key
               OR load.data_revision <> manifest_unit.data_revision
               OR load.canonical_iceberg_snapshot_id <> manifest_unit.canonical_iceberg_snapshot_id
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % selects a dynamic source with no succeeded PostGIS projection load', next_manifest_id
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
