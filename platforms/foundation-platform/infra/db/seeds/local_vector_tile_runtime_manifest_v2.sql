-- Deterministic local v2 runtime-manifest seed for the disposable tile proof.
--
-- The legacy local_vector_tile_manifest.sql is intentionally untouched. This seed adds one
-- complete dynamic `parcels` publication unit and its immutable ledger rows; production release
-- promotion uses the Catalog transaction rather than this fixture.

BEGIN;

-- `catalog.publication_revision` guards INSERT as well as UPDATE and DELETE, because
-- `grant-foundation-runtime.sql` hands every table in `catalog` an INSERT grant and a role boundary
-- alone would let the API mint a revision claiming any canonical snapshot. Seeding one is publishing
-- one, so the seed takes the capability — transaction-locally, inside this BEGIN.
SELECT set_config('foundation.temporal_publisher', 'on', true);

INSERT INTO catalog.vector_tile_publication_unit
    (id, unit_key, serving_generation, version)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743601', 'parcels', 1, 1)
ON CONFLICT (id) DO NOTHING;

-- A parcels revision, in the parcels unit's own ledger. This used to be an INSERT into
-- `catalog.administrative_boundary_revision` with a fabricated `iceberg:tile-runtime-v2` source
-- snapshot — a parcels revision recorded as an administrative boundary fact, because that was the
-- only ledger a release could reference. `derived_from_administrative_revision` stays NULL: parcels
-- geometry asserts nothing about administrative boundaries.
INSERT INTO catalog.publication_revision
    (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743603', '019d2b87-3fd1-7e3a-8d88-0b72c8743601',
     '841361364657368623', '019d2b87-3fd1-7e3a-8d88-0b72c8742001')
ON CONFLICT (id) DO NOTHING;

-- The load this seed's release serves rows out of. It used to be an invented UUID in the release row
-- and nothing else; `serving_postgis.spatial_projection_load` now has to hold it, because the release
-- carries a foreign key to it and `catalog.promote_vector_tile_runtime_manifest` refuses to point at a
-- dynamic unit whose load did not succeed. Opened `running` here and closed below, in that order, for
-- the same reason the publisher does: the row count is only known once the rows are in.
-- ADR-0025/0026: a `parcels` load may only be promoted if it names sealed evidence for the mirror
-- rebuild it came from, so the seed has to seal that evidence rather than skip it. The sealer
-- capability is transaction-local and this file runs inside one transaction, matching how the
-- publisher takes it. Every column here is pinned by the evidence composite FK to the rebuild run
-- `scripts/tiles/fixture.sql` closed: change one and the insert is refused, not silently accepted.
--
-- The two digests are synthetic. This proof has no execution-evidence object and no Iceberg reader,
-- so neither value is ADR-0025 canonical bytes and neither may be read as proof of content; the
-- column CHECKs require only 64 lowercase hex characters.
SELECT set_config('foundation.parcel_publication_evidence_sealer', 'on', true);

INSERT INTO catalog.parcel_publication_source_evidence
    (id, mirror_rebuild_run_id, mirror_rebuild_run_status, mirror_rebuild_rejected_row_count,
     iceberg_table_uuid, iceberg_logical_table, iceberg_snapshot_id,
     source_record_id, source_file_asset_id,
     execution_evidence_schema_version, execution_evidence_object_key, execution_evidence_sha256,
     source_row_count, projection_content_sha256, quality_schema_version)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743901',
     '019d2b87-3fd1-7e3a-8d88-0b72c8742301', 'succeeded', 0,
     '019d2b87-3fd1-7e3a-8d88-0b72c8743902', 'silver.parcel_boundaries', 841361364657368623,
     '019d2b87-3fd1-7e3a-8d88-0b72c8742001', '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
     'foundation-platform.parcel_publication_execution_evidence.v1',
     'evidence/parcel-publication/019d2b87-3fd1-7e3a-8d88-0b72c8743901.json', repeat('e', 64),
     3, repeat('f', 64), 'foundation-platform.parcel_publication_quality.v1')
ON CONFLICT (id) DO NOTHING;

SELECT set_config('foundation.parcel_publication_evidence_sealer', 'off', true);

-- The evidence binding is set here, at INSERT, because it becomes immutable once the load reaches a
-- terminal status below.
INSERT INTO serving_postgis.spatial_projection_load
    (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status,
     source_evidence_id)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743604', '019d2b87-3fd1-7e3a-8d88-0b72c8743601',
     '019d2b87-3fd1-7e3a-8d88-0b72c8743603', '841361364657368623', 'running',
     '019d2b87-3fd1-7e3a-8d88-0b72c8743901')
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.vector_tile_release
    (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
     source_record_id, source_file_asset_ids, source_kind, martin_source_id,
     tiles_url_template, postgis_projection_revision)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743602',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743601',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743603',
        '841361364657368623',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        ARRAY['019d2b87-3fd1-7e3a-8d88-0b72c8742004'::uuid],
        'dynamic_postgis',
        'parcels',
        'http://127.0.0.1:3110/parcels/{z}/{x}/{y}',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743604'
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.vector_tile_release_layer
    (release_id, layer_id, source_layer, feature_id_property,
     tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
     feature_filter_properties)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743602', 'parcels', 'parcels', 'pnu',
        14, 16, 14, 22, '{"pnu":"pnu"}'::jsonb
    )
ON CONFLICT (release_id, layer_id) DO NOTHING;

UPDATE catalog.vector_tile_publication_unit
SET active_release_id = '019d2b87-3fd1-7e3a-8d88-0b72c8743602',
    active_data_revision = '019d2b87-3fd1-7e3a-8d88-0b72c8743603',
    updated_at = now()
WHERE id = '019d2b87-3fd1-7e3a-8d88-0b72c8743601';

INSERT INTO catalog.vector_tile_runtime_manifest
    (id, manifest_generation, published_at)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743605', 1, TIMESTAMPTZ '2026-07-24 00:00:00+00')
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.vector_tile_runtime_manifest_unit
    (manifest_id, publication_unit_id, release_id, serving_generation,
     data_revision, canonical_iceberg_snapshot_id)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743605',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743601',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743602',
        1,
        '019d2b87-3fd1-7e3a-8d88-0b72c8743603',
        '841361364657368623'
    )
ON CONFLICT (manifest_id, publication_unit_id) DO NOTHING;

INSERT INTO catalog.vector_tile_runtime_manifest_pointer (singleton, manifest_id)
VALUES (true, '019d2b87-3fd1-7e3a-8d88-0b72c8743605')
ON CONFLICT (singleton) DO UPDATE SET manifest_id = EXCLUDED.manifest_id, updated_at = now();

-- No `official_complex_code`, and no join to `catalog.industrial_complex` (ADR-0024). That inner
-- join was not incidental: it was the only way to satisfy the column's NOT NULL, and it silently
-- excluded every parcel outside a complex — which is most of the country. The seed still scopes to
-- one complex through the mirror's own nullable column, because this fixture is a bounded slice,
-- but nothing structural requires a parcel to have one any more.
INSERT INTO serving_postgis.parcel_boundary_publication
    (pnu, data_revision, canonical_iceberg_snapshot_id, source_record_id,
     source_object_key, complex_id, parcel_id,
     geometry_checksum_sha256, geom, properties, projection_load_id)
SELECT
    mirror.pnu,
    '019d2b87-3fd1-7e3a-8d88-0b72c8743603',
    '841361364657368623',
    mirror.source_record_id,
    mirror.source_object_key,
    mirror.complex_id,
    mirror.parcel_id,
    mirror.geometry_checksum_sha256,
    mirror.geom,
    mirror.properties,
    '019d2b87-3fd1-7e3a-8d88-0b72c8743604'
FROM serving_postgis.parcel_boundary_mirror AS mirror
WHERE mirror.complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'
-- `DO NOTHING`, not the `DO UPDATE` this used to carry. A load names one materialisation, so
-- re-running the seed re-asserts the same load rather than replacing its geometry underneath a
-- release that already points at it. Changed source geometry needs a new load id, which is the whole
-- distinction the ledger introduces.
ON CONFLICT (projection_load_id, pnu) DO NOTHING;

-- Closes the load with the count actually in the table. The `succeeded` CHECK requires a positive
-- count, so a seed that materialised nothing fails here instead of leaving a promoted pointer at a
-- unit Martin would serve zero features for.
UPDATE serving_postgis.spatial_projection_load
   SET status = 'succeeded',
       loaded_row_count = (
           SELECT count(*) FROM serving_postgis.parcel_boundary_publication
            WHERE projection_load_id = '019d2b87-3fd1-7e3a-8d88-0b72c8743604'
       ),
       finished_at = now()
 WHERE id = '019d2b87-3fd1-7e3a-8d88-0b72c8743604'
   AND status = 'running';

-- The load id above is a literal, so a re-run cannot mint a new load the way the publisher does, and
-- `DO NOTHING` means changed mirror geometry would be skipped in silence — the same shape of hole
-- this ledger exists to close. A fixture cannot derive a fresh identity, but it can refuse to claim
-- one it no longer matches: if the mirror stops agreeing with what this load recorded, say so here
-- rather than serving the stored rows under a release that says they are current.
-- Compared by content, not by cardinality. A count agreed whenever an edit replaced one geometry
-- with another, which is exactly the case where the stored rows are stale and the count cannot say
-- so. Both sides already carry `geometry_checksum_sha256`, so the digest below distinguishes "the
-- same rows" from "the same number of rows".
DO $$
DECLARE
    stored_rows bigint;
    mirror_rows bigint;
    stored_digest text;
    mirror_digest text;
BEGIN
    SELECT count(*), md5(coalesce(string_agg(pnu || geometry_checksum_sha256, ',' ORDER BY pnu), ''))
      INTO stored_rows, stored_digest
      FROM serving_postgis.parcel_boundary_publication
     WHERE projection_load_id = '019d2b87-3fd1-7e3a-8d88-0b72c8743604';
    SELECT count(*), md5(coalesce(string_agg(mirror.pnu || mirror.geometry_checksum_sha256, ',' ORDER BY mirror.pnu), ''))
      INTO mirror_rows, mirror_digest
      FROM serving_postgis.parcel_boundary_mirror AS mirror
     WHERE mirror.complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101';
    IF stored_digest <> mirror_digest THEN
        RAISE EXCEPTION
            'projection load 019d2b87-3fd1-7e3a-8d88-0b72c8743604 holds % row(s) that no longer match the mirror''s % row(s); mint a new load id in this seed, and a new release to name it',
            stored_rows, mirror_rows;
    END IF;
END
$$;

COMMIT;
