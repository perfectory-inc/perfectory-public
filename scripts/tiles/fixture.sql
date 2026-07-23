-- Disposable synthetic geometry fixture for the Martin object-storage-first tile proof.
-- public-repository-safety: synthetic-fixture
--
-- This is deliberately one small industrial complex. The three parcel facts live
-- once in a temporary table and feed the Catalog, PostGIS mirror, anchor registry,
-- and proof-only Martin views so the fixture cannot drift internally.

BEGIN;

INSERT INTO catalog.source_record
    (id, source, source_url, external_id, captured_at, checksum_sha256, raw_object_key)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        'tiles-slice-proof-fixture',
        NULL,
        'synthetic-industrial-complex-v1',
        TIMESTAMPTZ '2026-07-21 00:00:00+00',
        repeat('a', 64),
        'tiles-slice-proof/fixture/source.json'
    )
ON CONFLICT (id) DO UPDATE
SET
    source = EXCLUDED.source,
    source_url = EXCLUDED.source_url,
    external_id = EXCLUDED.external_id,
    captured_at = EXCLUDED.captured_at,
    checksum_sha256 = EXCLUDED.checksum_sha256,
    raw_object_key = EXCLUDED.raw_object_key;

INSERT INTO catalog.file_asset
    (
        id,
        object_key,
        mime_type,
        size_bytes,
        checksum_sha256,
        title,
        source_record_id,
        visibility,
        version
    )
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742002',
        'tiles-slice-proof/local/manifest.json',
        'application/json',
        1,
        repeat('b', 64),
        'Martin tile-slice local manifest',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        'public',
        1
    ),
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742003',
        'tiles-slice-proof/local/foundation-static.tilejson.json',
        'application/json',
        1,
        repeat('c', 64),
        'Martin tile-slice TileJSON metadata',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        'public',
        1
    ),
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
        'tiles-slice-proof/fixture/parcel-boundaries.geojson',
        'application/geo+json',
        1,
        repeat('d', 64),
        'Namdong parcel geometry proof fixture',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        'internal',
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    object_key = EXCLUDED.object_key,
    mime_type = EXCLUDED.mime_type,
    size_bytes = EXCLUDED.size_bytes,
    checksum_sha256 = EXCLUDED.checksum_sha256,
    title = EXCLUDED.title,
    source_record_id = EXCLUDED.source_record_id,
    visibility = EXCLUDED.visibility,
    updated_at = now(),
    version = catalog.file_asset.version + 1;

INSERT INTO catalog.industrial_complex
    (id, name, kind, primary_bjdong_code, area_m2, official_complex_code, version)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742101',
        'Synthetic Industrial Complex',
        'general',
        '9999999999',
        9574000,
        'IC-SYNTHETIC-001',
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    name = EXCLUDED.name,
    kind = EXCLUDED.kind,
    primary_bjdong_code = EXCLUDED.primary_bjdong_code,
    area_m2 = EXCLUDED.area_m2,
    official_complex_code = EXCLUDED.official_complex_code,
    archived_at = NULL,
    archived_by_staff_id = NULL,
    archive_reason = NULL,
    updated_at = now(),
    version = catalog.industrial_complex.version + 1;

CREATE TEMP TABLE tiles_slice_fixture_parcel (
    parcel_id uuid PRIMARY KEY,
    anchor_id uuid UNIQUE NOT NULL,
    pnu character(19) UNIQUE NOT NULL,
    kind text NOT NULL,
    area_m2 bigint NOT NULL,
    boundary_wkt text NOT NULL,
    anchor_lng double precision NOT NULL,
    anchor_lat double precision NOT NULL,
    geometry_checksum_sha256 character(64) NOT NULL
) ON COMMIT DROP;

INSERT INTO tiles_slice_fixture_parcel
    (
        parcel_id,
        anchor_id,
        pnu,
        kind,
        area_m2,
        boundary_wkt,
        anchor_lng,
        anchor_lat,
        geometry_checksum_sha256
    )
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742201',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742401',
        '9999900000000000001',
        'factory',
        3000,
        'POLYGON((127.1231 36.1231,127.1234 36.1231,127.1234 36.1234,127.1231 36.1234,127.1231 36.1231))',
        127.12325,
        36.12325,
        repeat('1', 64)
    ),
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742202',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742402',
        '9999900000000000002',
        'factory',
        3200,
        'POLYGON((127.1235 36.1231,127.1238 36.1231,127.1238 36.1234,127.1235 36.1234,127.1235 36.1231))',
        127.12365,
        36.12325,
        repeat('2', 64)
    ),
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742203',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742403',
        '9999900000000000003',
        'support',
        2800,
        'POLYGON((127.1231 36.1235,127.1234 36.1235,127.1234 36.1238,127.1231 36.1238,127.1231 36.1235))',
        127.12325,
        36.12365,
        repeat('3', 64)
    );

INSERT INTO catalog.parcel
    (id, complex_id, pnu, kind, area_m2, version)
SELECT
    fixture.parcel_id,
    '019d2b87-3fd1-7e3a-8d88-0b72c8742101',
    fixture.pnu,
    fixture.kind,
    fixture.area_m2,
    1
FROM tiles_slice_fixture_parcel AS fixture
ON CONFLICT (id) DO UPDATE
SET
    complex_id = EXCLUDED.complex_id,
    pnu = EXCLUDED.pnu,
    kind = EXCLUDED.kind,
    area_m2 = EXCLUDED.area_m2,
    updated_at = now(),
    version = catalog.parcel.version + 1;

INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
    (
        id,
        source_snapshot_id,
        source_table,
        source_record_id,
        source_file_asset_id,
        srid,
        status,
        loaded_row_count,
        rejected_row_count,
        quality_report,
        started_at,
        finished_at,
        version
    )
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742301',
        'iceberg:tiles-slice-proof-v1',
        'silver.parcel_boundaries',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
        5179,
        'succeeded',
        3,
        0,
        jsonb_build_object('fixture', true, 'invalid_geometry_count', 0),
        TIMESTAMPTZ '2026-07-21 00:00:00+00',
        TIMESTAMPTZ '2026-07-21 00:00:01+00',
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    source_snapshot_id = EXCLUDED.source_snapshot_id,
    source_table = EXCLUDED.source_table,
    source_record_id = EXCLUDED.source_record_id,
    source_file_asset_id = EXCLUDED.source_file_asset_id,
    srid = EXCLUDED.srid,
    status = EXCLUDED.status,
    loaded_row_count = EXCLUDED.loaded_row_count,
    rejected_row_count = EXCLUDED.rejected_row_count,
    quality_report = EXCLUDED.quality_report,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    error_message = NULL,
    updated_at = now(),
    version = serving_postgis.parcel_boundary_mirror_rebuild_run.version + 1;

INSERT INTO serving_postgis.parcel_boundary_mirror
    (
        pnu,
        rebuild_run_id,
        source_snapshot_id,
        source_table,
        source_record_id,
        source_file_asset_id,
        source_object_key,
        source_row_id,
        complex_id,
        parcel_id,
        geometry_checksum_sha256,
        properties,
        geom,
        version
    )
SELECT
    fixture.pnu,
    '019d2b87-3fd1-7e3a-8d88-0b72c8742301',
    'iceberg:tiles-slice-proof-v1',
    'silver.parcel_boundaries',
    '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
    '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
    'tiles-slice-proof/fixture/parcel-boundaries.geojson',
    fixture.pnu::text,
    '019d2b87-3fd1-7e3a-8d88-0b72c8742101',
    fixture.parcel_id,
    fixture.geometry_checksum_sha256,
        jsonb_build_object('fixture', true, 'official_complex_code', 'IC-SYNTHETIC-001'),
    public.ST_Multi(
        public.ST_Transform(
            public.ST_SetSRID(public.ST_GeomFromText(fixture.boundary_wkt), 4326),
            5179
        )
    )::public.geometry(MultiPolygon, 5179),
    1
FROM tiles_slice_fixture_parcel AS fixture
ON CONFLICT (pnu) DO UPDATE
SET
    rebuild_run_id = EXCLUDED.rebuild_run_id,
    source_snapshot_id = EXCLUDED.source_snapshot_id,
    source_table = EXCLUDED.source_table,
    source_record_id = EXCLUDED.source_record_id,
    source_file_asset_id = EXCLUDED.source_file_asset_id,
    source_object_key = EXCLUDED.source_object_key,
    source_row_id = EXCLUDED.source_row_id,
    complex_id = EXCLUDED.complex_id,
    parcel_id = EXCLUDED.parcel_id,
    geometry_checksum_sha256 = EXCLUDED.geometry_checksum_sha256,
    properties = EXCLUDED.properties,
    geom = EXCLUDED.geom,
    loaded_at = now(),
    updated_at = now(),
    version = serving_postgis.parcel_boundary_mirror.version + 1;

INSERT INTO catalog.parcel_marker_anchor_generation_run
    (
        id,
        source_snapshot_id,
        source_table,
        source_record_id,
        source_file_asset_id,
        algorithm,
        algorithm_version,
        srid,
        status,
        loaded_row_count,
        rejected_row_count,
        quality_report,
        started_at,
        finished_at,
        version
    )
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742302',
        'iceberg:tiles-slice-proof-v1',
        'silver.parcel_boundaries',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
        'polylabel',
        'polylabel:1',
        4326,
        'succeeded',
        3,
        0,
        jsonb_build_object('fixture', true, 'outside_polygon_count', 0),
        TIMESTAMPTZ '2026-07-21 00:00:01+00',
        TIMESTAMPTZ '2026-07-21 00:00:02+00',
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    source_snapshot_id = EXCLUDED.source_snapshot_id,
    source_table = EXCLUDED.source_table,
    source_record_id = EXCLUDED.source_record_id,
    source_file_asset_id = EXCLUDED.source_file_asset_id,
    algorithm = EXCLUDED.algorithm,
    algorithm_version = EXCLUDED.algorithm_version,
    srid = EXCLUDED.srid,
    status = EXCLUDED.status,
    loaded_row_count = EXCLUDED.loaded_row_count,
    rejected_row_count = EXCLUDED.rejected_row_count,
    quality_report = EXCLUDED.quality_report,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    error_message = NULL,
    updated_at = now(),
    version = catalog.parcel_marker_anchor_generation_run.version + 1;

INSERT INTO catalog.parcel_marker_anchor
    (
        id,
        pnu,
        parcel_id,
        generation_run_id,
        source_geometry_version,
        source_table,
        source_record_id,
        source_file_asset_id,
        source_object_key,
        source_row_id,
        anchor_point,
        algorithm,
        algorithm_version,
        source_geometry_checksum_sha256,
        computed_at_utc,
        activated_at_utc,
        superseded_at_utc,
        is_active,
        version
    )
SELECT
    fixture.anchor_id,
    fixture.pnu,
    fixture.parcel_id,
    '019d2b87-3fd1-7e3a-8d88-0b72c8742302',
    'iceberg:tiles-slice-proof-v1',
    'silver.parcel_boundaries',
    '019d2b87-3fd1-7e3a-8d88-0b72c8742001',
    '019d2b87-3fd1-7e3a-8d88-0b72c8742004',
    'tiles-slice-proof/fixture/parcel-boundaries.geojson',
    fixture.pnu::text,
    public.ST_SetSRID(
        public.ST_MakePoint(fixture.anchor_lng, fixture.anchor_lat),
        4326
    )::public.geometry(Point, 4326),
    'polylabel',
    'polylabel:1',
    fixture.geometry_checksum_sha256,
    TIMESTAMPTZ '2026-07-21 00:00:02+00',
    TIMESTAMPTZ '2026-07-21 00:00:02+00',
    NULL,
    true,
    1
FROM tiles_slice_fixture_parcel AS fixture
ON CONFLICT (id) DO UPDATE
SET
    pnu = EXCLUDED.pnu,
    parcel_id = EXCLUDED.parcel_id,
    generation_run_id = EXCLUDED.generation_run_id,
    source_geometry_version = EXCLUDED.source_geometry_version,
    source_table = EXCLUDED.source_table,
    source_record_id = EXCLUDED.source_record_id,
    source_file_asset_id = EXCLUDED.source_file_asset_id,
    source_object_key = EXCLUDED.source_object_key,
    source_row_id = EXCLUDED.source_row_id,
    anchor_point = EXCLUDED.anchor_point,
    algorithm = EXCLUDED.algorithm,
    algorithm_version = EXCLUDED.algorithm_version,
    source_geometry_checksum_sha256 = EXCLUDED.source_geometry_checksum_sha256,
    computed_at_utc = EXCLUDED.computed_at_utc,
    activated_at_utc = EXCLUDED.activated_at_utc,
    superseded_at_utc = NULL,
    is_active = true,
    updated_at = now(),
    version = catalog.parcel_marker_anchor.version + 1;

-- These views are a bounded serving projection for the proof. Their columns are
-- exactly the Martin property allowlists plus one typed geometry column.
CREATE OR REPLACE VIEW serving_postgis.tiles_slice_parcels AS
SELECT
    boundary.pnu::text AS pnu,
    boundary.pnu::text AS "PNU",
    complex.official_complex_code,
    boundary.geom::public.geometry(MultiPolygon, 5179) AS geom
FROM serving_postgis.parcel_boundary_mirror AS boundary
JOIN catalog.industrial_complex AS complex ON complex.id = boundary.complex_id
WHERE complex.id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101';

CREATE OR REPLACE VIEW serving_postgis.tiles_slice_parcel_anchor_aggregate AS
SELECT
    min(anchor.pnu::text) AS pnu,
    complex.official_complex_code,
    count(*)::integer AS count,
    public.ST_Centroid(public.ST_Collect(anchor.anchor_point))::public.geometry(Point, 4326) AS geom
FROM catalog.parcel_marker_anchor AS anchor
JOIN catalog.parcel AS parcel ON parcel.id = anchor.parcel_id
JOIN catalog.industrial_complex AS complex ON complex.id = parcel.complex_id
WHERE anchor.is_active
  AND complex.id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'
GROUP BY complex.id, complex.official_complex_code;

CREATE OR REPLACE VIEW serving_postgis.tiles_slice_parcel_anchor AS
SELECT
    anchor.pnu::text AS pnu,
    complex.official_complex_code,
    anchor.anchor_point::public.geometry(Point, 4326) AS geom
FROM catalog.parcel_marker_anchor AS anchor
JOIN catalog.parcel AS parcel ON parcel.id = anchor.parcel_id
JOIN catalog.industrial_complex AS complex ON complex.id = parcel.complex_id
WHERE anchor.is_active
  AND complex.id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101';

COMMIT;
