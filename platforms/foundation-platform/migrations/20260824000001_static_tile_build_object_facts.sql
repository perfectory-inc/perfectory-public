-- ADR-0053: a static promotion consumes object facts sealed on its build row.
--
-- Before this migration the build ledger recorded only an evidence digest, while promotion accepted
-- checksum and size again from an operator-facing command. These nullable columns stay absent for a
-- running/failed build and become an all-or-nothing tuple for validated/promoted builds.

ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN result_release_id uuid,
    ADD COLUMN result_pmtiles_file_asset_id uuid,
    ADD COLUMN result_pmtiles_object_key text,
    ADD COLUMN result_tiles_url_template text,
    ADD COLUMN result_pmtiles_sha256 character(64),
    ADD COLUMN result_pmtiles_bytes bigint,
    ADD COLUMN result_recorded_by_staff_id uuid,
    ADD COLUMN failure_reason text;

ALTER TABLE catalog.vector_tile_build_job
    DROP CONSTRAINT vector_tile_build_job_result_evidence_check,
    ADD CONSTRAINT vector_tile_build_job_result_facts_check CHECK (
        (
            status IN ('validated', 'promoted', 'superseded')
            AND result_snapshot_id = frozen_source_snapshot_id
            AND result_evidence_sha256 ~ '^[0-9a-f]{64}$'
            AND result_release_id IS NOT NULL
            AND result_pmtiles_file_asset_id IS NOT NULL
            AND result_pmtiles_object_key ~ '^gold/vector-tiles/releases/[A-Za-z][A-Za-z0-9_-]{0,127}-[0-9a-f-]{36}\.pmtiles$'
            AND btrim(result_tiles_url_template) <> ''
            AND result_pmtiles_sha256 ~ '^[0-9a-f]{64}$'
            AND result_pmtiles_bytes > 0
            AND result_recorded_by_staff_id IS NOT NULL
            AND failure_reason IS NULL
        )
        OR
        (
            status = 'failed'
            AND result_snapshot_id IS NULL
            AND result_evidence_sha256 IS NULL
            AND result_release_id IS NULL
            AND result_pmtiles_file_asset_id IS NULL
            AND result_pmtiles_object_key IS NULL
            AND result_tiles_url_template IS NULL
            AND result_pmtiles_sha256 IS NULL
            AND result_pmtiles_bytes IS NULL
            AND result_recorded_by_staff_id IS NOT NULL
            AND btrim(failure_reason) <> ''
        )
        OR
        (
            status IN ('planned', 'running')
            AND result_snapshot_id IS NULL
            AND result_evidence_sha256 IS NULL
            AND result_release_id IS NULL
            AND result_pmtiles_file_asset_id IS NULL
            AND result_pmtiles_object_key IS NULL
            AND result_tiles_url_template IS NULL
            AND result_pmtiles_sha256 IS NULL
            AND result_pmtiles_bytes IS NULL
            AND result_recorded_by_staff_id IS NULL
            AND failure_reason IS NULL
        )
    );

CREATE UNIQUE INDEX vector_tile_build_job_result_release_idx
    ON catalog.vector_tile_build_job (result_release_id)
    WHERE result_release_id IS NOT NULL;

CREATE UNIQUE INDEX vector_tile_build_job_result_pmtiles_file_asset_idx
    ON catalog.vector_tile_build_job (result_pmtiles_file_asset_id)
    WHERE result_pmtiles_file_asset_id IS NOT NULL;

CREATE UNIQUE INDEX vector_tile_build_job_result_pmtiles_object_key_idx
    ON catalog.vector_tile_build_job (result_pmtiles_object_key)
    WHERE result_pmtiles_object_key IS NOT NULL;
