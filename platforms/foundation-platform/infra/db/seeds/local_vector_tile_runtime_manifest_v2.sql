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
INSERT INTO serving_postgis.spatial_projection_load
    (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743604', '019d2b87-3fd1-7e3a-8d88-0b72c8743601',
     '019d2b87-3fd1-7e3a-8d88-0b72c8743603', '841361364657368623', 'running')
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

INSERT INTO serving_postgis.parcel_boundary_publication
    (pnu, data_revision, canonical_iceberg_snapshot_id, source_record_id,
     source_object_key, complex_id, parcel_id, official_complex_code,
     geometry_checksum_sha256, geom, properties, projection_load_id)
SELECT
    mirror.pnu,
    '019d2b87-3fd1-7e3a-8d88-0b72c8743603',
    '841361364657368623',
    mirror.source_record_id,
    mirror.source_object_key,
    mirror.complex_id,
    mirror.parcel_id,
    complex.official_complex_code,
    mirror.geometry_checksum_sha256,
    mirror.geom,
    mirror.properties,
    '019d2b87-3fd1-7e3a-8d88-0b72c8743604'
FROM serving_postgis.parcel_boundary_mirror AS mirror
JOIN catalog.industrial_complex AS complex ON complex.id = mirror.complex_id
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
DO $$
DECLARE
    recorded bigint;
    available bigint;
BEGIN
    SELECT loaded_row_count INTO recorded
      FROM serving_postgis.spatial_projection_load
     WHERE id = '019d2b87-3fd1-7e3a-8d88-0b72c8743604';
    SELECT count(*) INTO available
      FROM serving_postgis.parcel_boundary_mirror AS mirror
      JOIN catalog.industrial_complex AS complex ON complex.id = mirror.complex_id
     WHERE mirror.complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101';
    IF recorded <> available THEN
        RAISE EXCEPTION
            'projection load 019d2b87-3fd1-7e3a-8d88-0b72c8743604 recorded % rows but the mirror now offers %; mint a new load id in this seed',
            recorded, available;
    END IF;
END
$$;

COMMIT;
