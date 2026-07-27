-- public-repository-safety: synthetic-fixture
-- Disposable lineage seed for the administrative-boundary publisher smoke run.

BEGIN;

INSERT INTO catalog.source_record
    (id, source, external_id, checksum_sha256, raw_object_key)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743702',
        'official-administrative-boundary-fixture',
        'synthetic-administrative-boundary-v1',
        repeat('e', 64),
        'tiles-slice-proof/administrative-boundary/fixture.geojson'
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.file_asset
    (id, object_key, mime_type, size_bytes, checksum_sha256, title,
     source_record_id, visibility, version)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743703',
        'tiles-slice-proof/administrative-boundary/fixture.geojson',
        'application/geo+json',
        1,
        repeat('e', 64),
        'Synthetic administrative boundary fixture',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743702',
        'internal',
        1
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.administrative_boundary_revision
    (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8743701',
        '841361364657368624',
        'iceberg:administrative-boundary-fixture-v1',
        '019d2b87-3fd1-7e3a-8d88-0b72c8743702',
        'candidate'
    )
ON CONFLICT (id) DO NOTHING;

COMMIT;
