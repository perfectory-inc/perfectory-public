-- Deterministic local v2 runtime-manifest seed for the disposable tile proof.
--
-- The legacy local_vector_tile_manifest.sql is intentionally untouched. This seed adds one
-- complete dynamic `parcels` publication unit and its immutable ledger rows; production release
-- promotion uses the Catalog transaction rather than this fixture.

BEGIN;

INSERT INTO catalog.vector_tile_publication_unit
    (id, unit_key, serving_generation, version)
VALUES
    ('019d2b87-3fd1-7e3a-8d88-0b72c8743601', 'parcels', 1, 1)
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
     geometry_checksum_sha256, geom, properties)
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
    mirror.properties
FROM serving_postgis.parcel_boundary_mirror AS mirror
JOIN catalog.industrial_complex AS complex ON complex.id = mirror.complex_id
WHERE mirror.complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'
ON CONFLICT (data_revision, pnu) DO UPDATE
SET canonical_iceberg_snapshot_id = EXCLUDED.canonical_iceberg_snapshot_id,
    geom = EXCLUDED.geom,
    properties = EXCLUDED.properties,
    updated_at = now();

COMMIT;
