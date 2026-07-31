-- A data revision belongs to the unit it revises, and the ledger that holds it is not the
-- administrative boundary's.
--
-- `catalog.vector_tile_release.data_revision` has carried a composite foreign key into
-- `catalog.administrative_boundary_revision` since `20260727000001`. So every publication unit's
-- revision — parcels, complex, anything future — had to be registered in the ADMINISTRATIVE BOUNDARY
-- ledger. One domain's ledger was doing double duty as the universal one, and the lie is already in
-- committed code: the v2 seed registers a *parcels* revision there, and `spatial_tile_publication.rs`
-- does the same for `parcels` and `complex`.
--
-- That is what blocked a parcels projection loader. It could not mint a parcels revision without
-- recording it as an administrative boundary revision — a different source, a different status
-- machine, a different validation. Any loader built on that inherits the defect.
--
-- The table's own columns say it was never administrative. All seven are generic: `id`,
-- `canonical_iceberg_snapshot_id`, `source_snapshot_id`, `source_record_id`, `status`, `created_at`,
-- `validated_at`. And the composite unique key the release FK needs was not declared with the table —
-- it was bolted on 226 lines later, immediately before that FK. It exists to serve the generic
-- consumer. This migration finishes a split that migration had already started by accident.
--
-- What this ledger deliberately does NOT have:
--
--   * **No `status`.** The four values on the administrative ledger have exactly one writer between
--     them (`candidate` -> `validated`); `published` and `superseded` have no writer anywhere in the
--     repository. A publication revision's validity is already stated twice over — by whether a
--     `serving_postgis.spatial_projection_load` succeeded for it, and by whether the runtime pointer
--     selects a release that names it. `20260727000002` says outright that the runtime manifest is
--     the only visibility switch. A third statement of that fact is the `vector_tile_build_job`
--     failure `20260730000004` names.
--   * **No `source_snapshot_id`.** `20260727000001` had to fabricate one to satisfy its own CHECK —
--     `'iceberg:vector-tile-release:' || uuid`, a string synthesised to name the revision it is a
--     column of. Provenance here is the real Iceberg snapshot number plus the `source_record_id` the
--     release already carried. The fabrication has nowhere to be written.
--   * **No ordinal key.** A `(unit, ordinal)` primary key was considered and rejected: it would
--     enforce *allocation* order while the invariant anyone cares about is *content* order, and the
--     value that carries content order is already numeric and already here —
--     `canonical_iceberg_snapshot_id`. See ADR-0017.

-- The capability is taken as statement zero, not where it is first needed. Two statements below
-- require it — the backfill INSERT under this ledger's own capability trigger, and the DELETE of
-- migrated rows out of the append-only administrative ledger — and `set_config(..., true)` is
-- transaction-local, which is the whole migration because sqlx wraps each file in one transaction.
-- DDL does not fire row-level triggers; only those two DML statements need this.
SELECT set_config('foundation.temporal_publisher', 'on', true);

-- Report before reinterpreting. This migration assigns every existing revision to exactly one
-- publication unit, and a row that cannot be assigned must name itself rather than surface later as
-- a NOT NULL abort naming a column. Shape copied from `20260727000001`'s guard block.
DO $$
DECLARE
    offending text;
BEGIN
    SELECT string_agg(format('%s (units: %s)', data_revision, units), '; ')
      INTO offending
      FROM (
          SELECT release.data_revision,
                 string_agg(DISTINCT unit.unit_key, ',') AS units,
                 count(DISTINCT release.publication_unit_id) AS unit_count
            FROM catalog.vector_tile_release AS release
            JOIN catalog.vector_tile_publication_unit AS unit
              ON unit.id = release.publication_unit_id
           GROUP BY release.data_revision
      ) AS grouped
     WHERE grouped.unit_count > 1;
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'one data revision is shared by several publication units and cannot be scoped: %',
            offending
            USING ERRCODE = '23514';
    END IF;

    -- `vector_tile_release_unit_revision_snapshot_kind_key` includes the snapshot, so one revision id
    -- can appear under two canonical snapshots. The new ledger keys a revision by `id` alone, so one
    -- of those releases would lose its foreign key. Two snapshots are two revisions; say which.
    SELECT string_agg(format('%s (snapshots: %s)', data_revision, snapshots), '; ')
      INTO offending
      FROM (
          SELECT release.data_revision,
                 string_agg(DISTINCT release.canonical_iceberg_snapshot_id, ',') AS snapshots,
                 count(DISTINCT release.canonical_iceberg_snapshot_id) AS snapshot_count
            FROM catalog.vector_tile_release AS release
           GROUP BY release.data_revision
      ) AS grouped
     WHERE grouped.snapshot_count > 1;
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'one data revision spans several canonical snapshots and cannot be one revision row: %',
            offending
            USING ERRCODE = '23514';
    END IF;

    SELECT string_agg(DISTINCT load.publication_unit_key, ', ')
      INTO offending
      FROM serving_postgis.spatial_projection_load AS load
     WHERE NOT EXISTS (
         SELECT 1 FROM catalog.vector_tile_publication_unit AS unit
          WHERE unit.unit_key = load.publication_unit_key
     );
    IF offending IS NOT NULL THEN
        -- Reachable in production: `publish-administrative-boundary-postgis` opens a load, and the
        -- `admin` publication unit is created by the *promote* binary, which runs separately and may
        -- not have run yet.
        RAISE EXCEPTION 'projection loads name publication units that do not exist: %; run the promote command for them first',
            offending
            USING ERRCODE = '23503';
    END IF;
END
$$;

CREATE TABLE catalog.publication_revision (
    -- A uuid, not an ordinal. The handle stays globally unique so it can appear alone in a log line,
    -- a metric label, an operator env var and the public manifest — every place a revision is named
    -- without a schema row around it to supply a second column.
    id uuid PRIMARY KEY,
    publication_unit_id uuid NOT NULL REFERENCES catalog.vector_tile_publication_unit(id) ON DELETE RESTRICT,
    canonical_iceberg_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL REFERENCES catalog.source_record(id) ON DELETE RESTRICT,
    -- The domain fact revision this was derived from, where one exists. Set for administrative units,
    -- NULL for parcels: a parcels revision asserts nothing about administrative boundaries. This is
    -- what stops the split from forking one publisher's single revision into two unrelated ids in two
    -- ledgers with nothing tying them together.
    derived_from_administrative_revision uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT publication_revision_snapshot_check
        CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$'),
    -- One revision per unit per canonical snapshot. The snapshot *is* the data version, so a second
    -- revision of one unit over one snapshot would be two names for one fact. Re-materialising the
    -- same revision is the load ledger's job (`20260731000001`), not a second revision's.
    CONSTRAINT publication_revision_unit_snapshot_key
        UNIQUE (publication_unit_id, canonical_iceberg_snapshot_id),
    -- The key `vector_tile_release` and `spatial_projection_load` reference. Carrying the unit into
    -- the referenced key is the whole point: a release cannot name a revision belonging to another
    -- unit, because the pair is refused rather than merely discouraged.
    CONSTRAINT publication_revision_scoped_key
        UNIQUE (id, publication_unit_id, canonical_iceberg_snapshot_id)
);

ALTER TABLE catalog.publication_revision
    ADD CONSTRAINT publication_revision_administrative_lineage_fkey
    FOREIGN KEY (derived_from_administrative_revision, canonical_iceberg_snapshot_id)
    REFERENCES catalog.administrative_boundary_revision(id, canonical_iceberg_snapshot_id)
    -- MATCH SIMPLE, and it is load-bearing: with MATCH FULL a parcels revision — which has no
    -- administrative lineage and so leaves the first column NULL — would be refused outright.
    MATCH SIMPLE
    ON DELETE RESTRICT;

CREATE INDEX publication_revision_unit_idx
    ON catalog.publication_revision (publication_unit_id, created_at DESC);

-- INSERT, not just UPDATE and DELETE. `administrative_boundary_revision_append_only` covers only the
-- latter two, and `infra/compose/grant-foundation-runtime.sql` grants INSERT on every table in
-- `catalog` while its REVOKE list omits the revision ledger — so the API role could mint a revision
-- claiming any snapshot, with no evidence behind it. A grant is a role boundary and survives only
-- until someone adds a role; this is a capability boundary and survives the mistake.
CREATE TRIGGER publication_revision_publisher_only
BEFORE INSERT OR UPDATE OR DELETE ON catalog.publication_revision
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

-- Backfill pass one: every revision a release names.
INSERT INTO catalog.publication_revision
    (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
     derived_from_administrative_revision, created_at)
SELECT DISTINCT ON (release.data_revision)
    release.data_revision,
    release.publication_unit_id,
    release.canonical_iceberg_snapshot_id,
    release.source_record_id,
    -- Every legacy revision came out of the administrative ledger by construction, so the lineage is
    -- the row it already was. The rows this migration deletes below are exactly the ones with no
    -- administrative fact referencing them, and those are nulled first.
    release.data_revision,
    min(release.created_at) OVER (PARTITION BY release.data_revision)
FROM catalog.vector_tile_release AS release
ORDER BY release.data_revision, release.created_at;

-- Backfill pass two: revisions that only a projection load knows about. A load is opened before its
-- release exists — `publish-administrative-boundary-postgis` commits one, and the release is written
-- later by a separate binary — so a database caught between the two has loads whose revision no
-- release names. Pass one would leave those unscoped and the NOT NULL below would abort.
INSERT INTO catalog.publication_revision
    (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
     derived_from_administrative_revision, created_at)
SELECT DISTINCT ON (load.data_revision)
    load.data_revision,
    unit.id,
    load.canonical_iceberg_snapshot_id,
    revision.source_record_id,
    load.data_revision,
    min(load.started_at) OVER (PARTITION BY load.data_revision)
FROM serving_postgis.spatial_projection_load AS load
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.unit_key = load.publication_unit_key
JOIN catalog.administrative_boundary_revision AS revision
  ON revision.id = load.data_revision
WHERE NOT EXISTS (
    SELECT 1 FROM catalog.publication_revision AS existing
     WHERE existing.id = load.data_revision
)
ORDER BY load.data_revision, load.started_at;

-- Repoint the release. The referenced key carries the unit, so the FK now refuses a release naming
-- another unit's revision instead of merely being unable to notice it.
ALTER TABLE catalog.vector_tile_release
    DROP CONSTRAINT vector_tile_release_data_revision_fkey,
    ADD CONSTRAINT vector_tile_release_data_revision_fkey
        FOREIGN KEY (data_revision, publication_unit_id, canonical_iceberg_snapshot_id)
        REFERENCES catalog.publication_revision (id, publication_unit_id, canonical_iceberg_snapshot_id)
        ON DELETE RESTRICT;

-- The load names its unit by foreign key, not by a text column that had to be kept in step with
-- `vector_tile_publication_unit.unit_key` by a comment. `20260731000001` wrote that comment — "the
-- two spellings must not drift" — which is a foreign key's job described in prose.
ALTER TABLE serving_postgis.spatial_projection_load
    ADD COLUMN publication_unit_id uuid;
UPDATE serving_postgis.spatial_projection_load AS load
   SET publication_unit_id = unit.id
  FROM catalog.vector_tile_publication_unit AS unit
 WHERE unit.unit_key = load.publication_unit_key;
ALTER TABLE serving_postgis.spatial_projection_load
    ALTER COLUMN publication_unit_id SET NOT NULL,
    ADD CONSTRAINT spatial_projection_load_unit_fkey
        FOREIGN KEY (publication_unit_id)
        REFERENCES catalog.vector_tile_publication_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT spatial_projection_load_revision_fkey
        FOREIGN KEY (data_revision, publication_unit_id, canonical_iceberg_snapshot_id)
        REFERENCES catalog.publication_revision (id, publication_unit_id, canonical_iceberg_snapshot_id)
        ON DELETE RESTRICT,
    DROP CONSTRAINT spatial_projection_load_unit_key_check,
    DROP COLUMN publication_unit_key;
DROP INDEX IF EXISTS serving_postgis.spatial_projection_load_unit_revision_idx;
CREATE INDEX spatial_projection_load_unit_revision_idx
    ON serving_postgis.spatial_projection_load (publication_unit_id, data_revision, started_at DESC);

-- The administrative ledger keeps only what is genuinely an administrative boundary fact. A row is a
-- publication revision and not a fact revision exactly when no administrative fact table references
-- it — deleted by referent rather than by matching the fabricated `iceberg:vector-tile-release:`
-- string, which would miss the seed's differently-fabricated `iceberg:tile-runtime-v2`.
UPDATE catalog.publication_revision AS publication
   SET derived_from_administrative_revision = NULL
 WHERE NOT EXISTS (
     SELECT 1 FROM catalog.administrative_unit_identifier AS fact WHERE fact.data_revision = publication.id
     UNION ALL
     SELECT 1 FROM catalog.administrative_unit_transition AS fact WHERE fact.data_revision = publication.id
     UNION ALL
     SELECT 1 FROM catalog.administrative_unit_parent AS fact WHERE fact.data_revision = publication.id
     UNION ALL
     SELECT 1 FROM catalog.parcel_identifier AS fact WHERE fact.data_revision = publication.id
     UNION ALL
     SELECT 1 FROM catalog.parcel_administrative_unit AS fact WHERE fact.data_revision = publication.id
     UNION ALL
     SELECT 1 FROM serving_postgis.administrative_unit_boundary_publication AS fact
      WHERE fact.data_revision = publication.id
 );

DELETE FROM catalog.administrative_boundary_revision AS revision
 WHERE EXISTS (
     SELECT 1 FROM catalog.publication_revision AS publication
      WHERE publication.id = revision.id
        AND publication.derived_from_administrative_revision IS NULL
 );

-- `published` had no writer. `20260727000002` states that the runtime manifest is the only visibility
-- switch, so a revision claiming to be published was restating a fact the pointer owns. Folded into
-- `validated`, which is what the single existing writer actually sets.
UPDATE catalog.administrative_boundary_revision
   SET status = 'validated'
 WHERE status = 'published';
ALTER TABLE catalog.administrative_boundary_revision
    DROP CONSTRAINT administrative_boundary_revision_status_check,
    ADD CONSTRAINT administrative_boundary_revision_status_check
        CHECK (status IN ('candidate', 'validated', 'superseded'));

COMMENT ON TABLE catalog.publication_revision IS
    'Unit-scoped data revision of a vector tile publication unit. The administrative boundary revision ledger holds administrative facts only.';

-- The gate, replaced for the fourth time. Two changes.
--
-- 1. The dynamic readiness check compares publication unit *ids*, not the load's text spelling of a
--    unit key. That column no longer exists; the load names its unit by foreign key.
-- 2. A dynamic re-selection may not move a unit backwards. `catalog_domain::serving_publication`
--    documents this — "returning to dynamic is allowed for either the same data revision (a safe
--    fallback) or a newer revision" — and neither the domain function nor this gate has ever
--    enforced it, because a uuid has no order and the check was written against uuids. It is
--    enforced here on `canonical_iceberg_snapshot_id`, which is a real Iceberg snapshot number and
--    is the value that actually carries content order. `::numeric` rather than `::bigint`: the ids
--    outrun 64 bits, and both sides are CHECK-constrained to `^[1-9][0-9]*$`.
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

    -- A dynamic selection may hold the unit's canonical snapshot or move it forward, never back.
    -- Holding is the same-data rollback the domain calls a safe fallback; moving back would put
    -- superseded content behind a higher serving generation, which the client reads as newer.
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
