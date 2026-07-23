-- Local smoke seed for ADR 0004.
--
-- Purpose:
--   Make `GET /catalog/v1/vector-tiles/manifest` return a real active manifest
--   in local development without giving Gongzzang any manifest write ownership.
--
-- Tile bytes are addressed by object_key_prefix. Local clients must provide a
-- public base URL for the object store/static file server that owns those bytes.
--
-- Run after migrations:
--   psql "$DATABASE_URL" -f infra/db/seeds/local_vector_tile_manifest.sql

BEGIN;

INSERT INTO catalog.source_record
    (id, source, source_url, external_id, checksum_sha256, raw_object_key)
VALUES
    (
        '018f0000-0000-7000-8000-000000010001',
        'gongzzang-local-dev-vector-tiles',
        'http://127.0.0.1:8787/dev-tiles/parcels',
        'dev-local',
        repeat('0', 64),
        'dev-tiles/parcels/metadata.json'
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.file_asset
    (id, object_key, mime_type, size_bytes, checksum_sha256, title, source_record_id, visibility, version)
VALUES
    (
        '018f0000-0000-7000-8000-000000020001',
        'gold/manifest.json',
        'application/json',
        1075,
        repeat('0', 64),
        'Local active vector tile manifest',
        '018f0000-0000-7000-8000-000000010001',
        'public',
        1
    ),
    (
        '018f0000-0000-7000-8000-000000020002',
        'gold/dev-local/parcels.json',
        'application/json',
        1075,
        repeat('0', 64),
        'Local parcels TileJSON placeholder',
        '018f0000-0000-7000-8000-000000010001',
        'public',
        1
    ),
    (
        '018f0000-0000-7000-8000-000000020003',
        'dev-tiles/parcels/metadata.json',
        'application/json',
        1075,
        repeat('0', 64),
        'Local parcels source metadata',
        '018f0000-0000-7000-8000-000000010001',
        'internal',
        1
    )
ON CONFLICT (object_key) DO UPDATE
SET
    mime_type = EXCLUDED.mime_type,
    size_bytes = EXCLUDED.size_bytes,
    checksum_sha256 = EXCLUDED.checksum_sha256,
    title = EXCLUDED.title,
    source_record_id = EXCLUDED.source_record_id,
    visibility = EXCLUDED.visibility,
    updated_at = now(),
    version = catalog.file_asset.version + 1;

UPDATE catalog.vector_tile_manifest
SET is_active = false
WHERE is_active = true
  AND id <> '018f0000-0000-7000-8000-000000030001';

INSERT INTO catalog.vector_tile_manifest
    (
        id,
        current_version,
        previous_version,
        tiles_url_template,
        manifest_file_asset_id,
        source_record_id,
        is_active,
        version
    )
VALUES
    (
        '018f0000-0000-7000-8000-000000030001',
        '019d2b87-3fd1-7e3a-8d88-0b72c8741001',
        '019d2b87-3fd1-7e3a-8d88-0b72c8741000',
        '{object_key_prefix}/{z}/{x}/{y}.pbf',
        '018f0000-0000-7000-8000-000000020001',
        '018f0000-0000-7000-8000-000000010001',
        true,
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    current_version = EXCLUDED.current_version,
    previous_version = EXCLUDED.previous_version,
    tiles_url_template = EXCLUDED.tiles_url_template,
    manifest_file_asset_id = EXCLUDED.manifest_file_asset_id,
    source_record_id = EXCLUDED.source_record_id,
    is_active = true,
    published_at = now(),
    updated_at = now(),
    version = catalog.vector_tile_manifest.version + 1;

INSERT INTO catalog.vector_tile_artifact
    (
        id,
        manifest_id,
        layer,
        source_layer,
        tile_min_zoom,
        tile_max_zoom,
        render_min_zoom,
        render_max_zoom,
        tilejson_file_asset_id,
        object_key_prefix,
        flat_tile_count,
        flat_tile_total_bytes,
        source_record_id,
        version
    )
VALUES
    (
        '018f0000-0000-7000-8000-000000040001',
        '018f0000-0000-7000-8000-000000030001',
        'parcels',
        'parcels',
        14,
        17,
        14,
        22,
        '018f0000-0000-7000-8000-000000020002',
        'dev-tiles/parcels/',
        235,
        265407,
        '018f0000-0000-7000-8000-000000010001',
        1
    )
ON CONFLICT (manifest_id, layer) DO UPDATE
SET
    source_layer = EXCLUDED.source_layer,
    tile_min_zoom = EXCLUDED.tile_min_zoom,
    tile_max_zoom = EXCLUDED.tile_max_zoom,
    render_min_zoom = EXCLUDED.render_min_zoom,
    render_max_zoom = EXCLUDED.render_max_zoom,
    tilejson_file_asset_id = EXCLUDED.tilejson_file_asset_id,
    object_key_prefix = EXCLUDED.object_key_prefix,
    flat_tile_count = EXCLUDED.flat_tile_count,
    flat_tile_total_bytes = EXCLUDED.flat_tile_total_bytes,
    source_record_id = EXCLUDED.source_record_id,
    updated_at = now(),
    version = catalog.vector_tile_artifact.version + 1;

INSERT INTO catalog.vector_tile_artifact_source_file_asset
    (artifact_id, file_asset_id)
VALUES
    (
        '018f0000-0000-7000-8000-000000040001',
        '018f0000-0000-7000-8000-000000020003'
    )
ON CONFLICT (artifact_id, file_asset_id) DO NOTHING;

COMMIT;
