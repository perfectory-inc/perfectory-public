-- Disposable synthetic lineage for the industrial-complex half of the boundary slice proof.
-- public-repository-safety: synthetic-fixture
--
-- The geometry itself is `industrial-complex-boundary-fixture.jsonl`, in the shape
-- `infra/lakehouse/spark/jobs/industrial_complex_boundaries_silver_to_postgis_handoff.py` exports.
-- What cannot live in that file is the Catalog lineage the publish and the promotion check against:
-- `publish-industrial-complex-boundary-postgis` refuses unless a `catalog.source_record` addresses
-- the same immutable object key every exported row cites, and
-- `promote-industrial-complex-boundary-runtime` refuses unless the release's
-- `catalog.file_asset` exists.
--
-- Separate rows from `fixture.sql`'s, not a re-use of them. That file's source record addresses a
-- parcel GeoJSON, and pointing the boundary publish at it would make the export's own
-- `source_record_id` disagree with the object the Catalog says was read — which is the exact
-- mismatch `validate_row` exists to refuse, so re-using it would mean weakening the fixture until
-- the check passed.

BEGIN;

INSERT INTO catalog.source_record
    (id, source, source_url, external_id, captured_at, checksum_sha256, raw_object_key)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742005',
        'boundary-slice-proof-fixture',
        NULL,
        'synthetic-industrial-complex-boundary-v1',
        TIMESTAMPTZ '2026-07-21 00:00:00+00',
        repeat('5', 64),
        'tiles-slice-proof/synthetic-industrial-complex-boundary/fixture.zip'
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
        '019d2b87-3fd1-7e3a-8d88-0b72c8742006',
        'tiles-slice-proof/synthetic-industrial-complex-boundary/fixture.zip',
        'application/zip',
        1,
        repeat('6', 64),
        'Synthetic industrial-complex designation boundary proof fixture',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742005',
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

COMMIT;
