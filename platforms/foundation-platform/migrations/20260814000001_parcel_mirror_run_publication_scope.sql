-- Makes execution scope/limits part of the immutable terminal mirror-run fact (root ADR-0031).

ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD COLUMN publication_scope jsonb,
    ADD COLUMN publication_limits jsonb;

-- Existing runs were created by bounded QA writers. Their exact processed counts are conservative
-- per-run limits; no historical row is promoted to complete national scope during backfill.
-- The existing terminal-run guard correctly rejects ordinary UPDATEs. This migration temporarily
-- suspends only that guard inside its transaction, backfills every historical row, and restores the
-- guard before making the new columns mandatory.
ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
    DISABLE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard;

UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
SET publication_scope = '{"kind":"bounded","complete":false}'::jsonb,
    publication_limits = jsonb_build_object(
        'object_limit', CASE
            WHEN quality_report ->> 'object_count' ~ '^[1-9][0-9]*$'
            THEN (quality_report ->> 'object_count')::bigint
            ELSE 1
        END,
        'row_limit', CASE
            WHEN quality_report ->> 'expected_row_count' ~ '^[1-9][0-9]*$'
            THEN (quality_report ->> 'expected_row_count')::bigint
            ELSE GREATEST(loaded_row_count, 1)
        END,
        'shard_limit', CASE
            WHEN quality_report ->> 'object_count' ~ '^[1-9][0-9]*$'
            THEN (quality_report ->> 'object_count')::bigint
            ELSE 1
        END
    );

ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
    ENABLE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard;

ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
    ALTER COLUMN publication_scope SET NOT NULL,
    ALTER COLUMN publication_limits SET NOT NULL,
    -- The run must name one Catalog lineage pair, not two independently valid UUIDs. MATCH FULL
    -- also rejects half-populated provenance while preserving historical NULL/NULL rows.
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_source_asset_pair_fkey
        FOREIGN KEY (source_file_asset_id, source_record_id)
        REFERENCES catalog.file_asset(id, source_record_id)
        MATCH FULL ON DELETE RESTRICT,
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_publication_scope_check CHECK (
        publication_scope IN (
            '{"kind":"bounded","complete":false}'::jsonb,
            '{"kind":"national","complete":true}'::jsonb
        )
    ),
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_publication_limits_check CHECK (
        jsonb_typeof(publication_limits) = 'object'
        AND publication_limits ?& ARRAY['object_limit', 'row_limit', 'shard_limit']
        AND publication_limits - ARRAY['object_limit', 'row_limit', 'shard_limit'] = '{}'::jsonb
        AND (
            (publication_limits -> 'object_limit' = 'null'::jsonb)
            OR (
                jsonb_typeof(publication_limits -> 'object_limit') = 'number'
                AND (publication_limits ->> 'object_limit')::bigint > 0
            )
        )
        AND (
            (publication_limits -> 'row_limit' = 'null'::jsonb)
            OR (
                jsonb_typeof(publication_limits -> 'row_limit') = 'number'
                AND (publication_limits ->> 'row_limit')::bigint > 0
            )
        )
        AND (
            (publication_limits -> 'shard_limit' = 'null'::jsonb)
            OR (
                jsonb_typeof(publication_limits -> 'shard_limit') = 'number'
                AND (publication_limits ->> 'shard_limit')::bigint > 0
            )
        )
        AND (
            (
                publication_scope = '{"kind":"national","complete":true}'::jsonb
                AND publication_limits =
                    '{"object_limit":null,"row_limit":null,"shard_limit":null}'::jsonb
            )
            OR (
                publication_scope = '{"kind":"bounded","complete":false}'::jsonb
                AND publication_limits <>
                    '{"object_limit":null,"row_limit":null,"shard_limit":null}'::jsonb
            )
        )
    );
