-- Disposable synthetic lineage for the industrial-complex half of the boundary slice proof.
-- public-repository-safety: synthetic-fixture
--
-- The geometry itself is `industrial-complex-boundary-fixture.jsonl`, in the shape
-- `infra/lakehouse/spark/jobs/industrial_complex_boundaries_silver_to_postgis_handoff.py` exports.
-- What cannot live in that file is the Catalog lineage the publish and the promotion check against:
-- `publish-industrial-complex-boundary-postgis` refuses unless a `catalog.bronze_object` was
-- collected at the same immutable object key every exported row cites, with the checksum the
-- publish names (root ADR-0046), and `promote-industrial-complex-boundary-runtime` refuses unless
-- the release's own `catalog.source_record` and `catalog.file_asset` exist.
--
-- Those are two different rows on purpose. The Bronze object is the file the polygons came out of;
-- the source record describes the *release* the promotion mints. This fixture seeds both because
-- the slice proof runs both commands, and giving them one row would hide that they ask different
-- questions.
--
-- The Bronze object arrives with the source catalog and ingestion run that produced it, because
-- `catalog.bronze_object` cannot exist without them. A row seeded without them would be shaped like
-- a collection without being reachable from one.
--
-- Separate rows from `fixture.sql`'s, not a re-use of them. That file's source record addresses a
-- parcel GeoJSON, and pointing the boundary publish at it would make the export's own
-- `source_record_id` disagree with the object the Catalog says was read — which is the exact
-- mismatch `validate_row` exists to refuse, so re-using it would mean weakening the fixture until
-- the check passed.

BEGIN;

INSERT INTO catalog.source_catalog
    (id, slug, name, provider, dataset_name, auth_kind, payload_format)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742007',
        'tiles-slice-proof-sandan-boundary',
        'Boundary slice proof synthetic industrial-complex source',
        'boundary-slice-proof-fixture',
        'synthetic-industrial-complex-boundary',
        'none',
        'zip'
    )
ON CONFLICT (id) DO UPDATE
SET
    slug = EXCLUDED.slug,
    name = EXCLUDED.name,
    provider = EXCLUDED.provider,
    dataset_name = EXCLUDED.dataset_name,
    auth_kind = EXCLUDED.auth_kind,
    payload_format = EXCLUDED.payload_format,
    updated_at = now(),
    version = catalog.source_catalog.version + 1;

INSERT INTO catalog.ingestion_run
    (id, source_catalog_id, trigger, status, objects_written)
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742008',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742007',
        'test',
        'succeeded',
        1
    )
ON CONFLICT (id) DO UPDATE
SET
    source_catalog_id = EXCLUDED.source_catalog_id,
    trigger = EXCLUDED.trigger,
    status = EXCLUDED.status,
    objects_written = EXCLUDED.objects_written,
    updated_at = now(),
    version = catalog.ingestion_run.version + 1;

-- `checksum_sha256` is 64 sevens, and the publish is given the same literal. The two agreeing is
-- what the proof asserts; if the seed and the argument were derived from each other there would be
-- nothing to agree.
INSERT INTO catalog.bronze_object
    (
        id,
        source_catalog_id,
        ingestion_run_id,
        dedupe_key,
        object_key,
        checksum_sha256,
        content_type,
        size_bytes,
        source_identity_key,
        snapshot_date,
        snapshot_granularity,
        snapshot_basis,
        collected_at
    )
VALUES
    (
        '019d2b87-3fd1-7e3a-8d88-0b72c8742009',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742007',
        '019d2b87-3fd1-7e3a-8d88-0b72c8742008',
        'tiles-slice-proof/synthetic-industrial-complex-boundary/fixture.zip',
        'tiles-slice-proof/synthetic-industrial-complex-boundary/fixture.zip',
        repeat('7', 64),
        'application/zip',
        1,
        'tiles-slice-proof/synthetic-industrial-complex-boundary',
        DATE '2026-07-21',
        'day',
        'collected_at_fallback',
        TIMESTAMPTZ '2026-07-21 00:00:00+00'
    )
ON CONFLICT (id) DO UPDATE
SET
    source_catalog_id = EXCLUDED.source_catalog_id,
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    dedupe_key = EXCLUDED.dedupe_key,
    object_key = EXCLUDED.object_key,
    checksum_sha256 = EXCLUDED.checksum_sha256,
    content_type = EXCLUDED.content_type,
    size_bytes = EXCLUDED.size_bytes,
    source_identity_key = EXCLUDED.source_identity_key,
    snapshot_date = EXCLUDED.snapshot_date,
    snapshot_granularity = EXCLUDED.snapshot_granularity,
    snapshot_basis = EXCLUDED.snapshot_basis,
    collected_at = EXCLUDED.collected_at;

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
