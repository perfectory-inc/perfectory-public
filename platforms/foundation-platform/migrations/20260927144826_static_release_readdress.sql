-- ADR-0111 §11: 주소 변경도 새 발행이다. 기존 release와 bake 유일성은 보존하고,
-- 검증된 정적 원본을 가리키는 readdress만 같은 revision/snapshot을 다시 발행한다.
-- 원본 하나의 재주소 발행 횟수를 제한하지 않는다. 같은 원본/목적지 쌍만 중복 금지다.
ALTER TABLE catalog.vector_tile_release
    ADD COLUMN readdressed_from_release_id uuid,
    ADD CONSTRAINT vector_tile_release_readdress_static_check CHECK (
        readdressed_from_release_id IS NULL
        OR (source_kind = 'static_pmtiles' AND readdressed_from_release_id <> id)
    ),
    ADD CONSTRAINT vector_tile_release_readdress_binding_fkey
        FOREIGN KEY (readdressed_from_release_id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id)
        REFERENCES catalog.vector_tile_release (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id),
    ADD CONSTRAINT vector_tile_release_readdress_destination_key
        UNIQUE (readdressed_from_release_id, tiles_url_template),
    DROP CONSTRAINT vector_tile_release_unit_revision_snapshot_kind_key;

CREATE UNIQUE INDEX vector_tile_release_unit_revision_snapshot_kind_key
    ON catalog.vector_tile_release
        (publication_unit_id, data_revision, canonical_iceberg_snapshot_id, source_kind)
    WHERE readdressed_from_release_id IS NULL;

ALTER TABLE catalog.vector_tile_build_job
    ADD COLUMN kind text NOT NULL DEFAULT 'bake',
    ADD COLUMN input_serving_generation bigint,
    ADD COLUMN readdress_tiles_base_url text,
    ADD CONSTRAINT vector_tile_build_job_kind_check CHECK (kind IN ('bake', 'readdress')),
    ADD CONSTRAINT vector_tile_build_job_readdress_observation_check CHECK (
        (kind = 'bake' AND input_serving_generation IS NULL AND readdress_tiles_base_url IS NULL)
        OR (kind = 'readdress'
            AND input_serving_generation IS NOT NULL
            AND input_serving_generation BETWEEN 1 AND 9007199254740991
            AND readdress_tiles_base_url IS NOT NULL
            AND btrim(readdress_tiles_base_url) <> '')
    );

-- 이전 CHECK의 NULL/UNKNOWN 허용에 기대지 않고 readdress 원본의 검증 사실을 한 곳에서 검사한다.
CREATE FUNCTION catalog.is_validated_static_tile_release(source catalog.vector_tile_release)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $function$
    SELECT coalesce(
        (source).source_kind = 'static_pmtiles'
        AND (source).validated_at IS NOT NULL
        AND (source).pmtiles_sha256 ~ '^[0-9a-f]{64}$'
        AND (source).pmtiles_bytes > 0
        AND (source).validation_evidence_sha256 ~ '^[0-9a-f]{64}$', false)
$function$;

-- CHECK에는 다른 행을 조회하는 subquery를 넣을 수 없다. 현재 active 조건은 INSERT 때
-- unit 행 잠금 아래 검사하고, 이후 불변 입력을 유지한다. promoted로 바뀔 때는 원본이
-- 더 이상 active가 아니므로 상태 UPDATE마다 active를 다시 요구해서는 안 된다.
CREATE FUNCTION catalog.guard_vector_tile_readdress_build()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
    selected_release uuid;
    selected_generation bigint;
    source catalog.vector_tile_release%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.kind = 'readdress' OR NEW.kind = 'readdress') AND
           ROW(NEW.kind, NEW.publication_unit_id, NEW.input_release_id,
               NEW.input_data_revision, NEW.frozen_source_snapshot_id,
               NEW.input_serving_generation, NEW.readdress_tiles_base_url, NEW.idempotency_key)
           IS DISTINCT FROM
           ROW(OLD.kind, OLD.publication_unit_id, OLD.input_release_id,
               OLD.input_data_revision, OLD.frozen_source_snapshot_id,
               OLD.input_serving_generation, OLD.readdress_tiles_base_url, OLD.idempotency_key) THEN
            RAISE EXCEPTION 'readdress build inputs are immutable' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NEW.kind <> 'readdress' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT active_release_id, serving_generation INTO selected_release, selected_generation
        FROM catalog.vector_tile_publication_unit
        WHERE id = NEW.publication_unit_id FOR UPDATE;
        IF selected_release IS DISTINCT FROM NEW.input_release_id
           OR selected_generation IS DISTINCT FROM NEW.input_serving_generation THEN
            RAISE EXCEPTION 'readdress input must be the active release and generation'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    SELECT * INTO source FROM catalog.vector_tile_release
    WHERE id = NEW.input_release_id FOR SHARE;
    IF NOT FOUND OR NOT catalog.is_validated_static_tile_release(source)
       OR source.publication_unit_id <> NEW.publication_unit_id
       OR source.data_revision <> NEW.input_data_revision
       OR source.canonical_iceberg_snapshot_id <> NEW.frozen_source_snapshot_id THEN
        RAISE EXCEPTION 'readdress input must bind a validated static release'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.status IN ('validated', 'promoted', 'superseded') AND (
        NEW.result_pmtiles_sha256 IS DISTINCT FROM source.pmtiles_sha256
        OR NEW.result_pmtiles_bytes IS DISTINCT FROM source.pmtiles_bytes
        OR NEW.result_tiles_url_template IS DISTINCT FROM
            rtrim(NEW.readdress_tiles_base_url, '/') || '/' ||
            (SELECT unit_key FROM catalog.vector_tile_publication_unit WHERE id = NEW.publication_unit_id)
            || '-' || NEW.result_release_id::text || '/{z}/{x}/{y}'
    ) THEN
        RAISE EXCEPTION 'readdress result must preserve source bytes and claimed destination'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

CREATE TRIGGER vector_tile_build_job_readdress_guard
    BEFORE INSERT OR UPDATE ON catalog.vector_tile_build_job
    FOR EACH ROW EXECUTE FUNCTION catalog.guard_vector_tile_readdress_build();

-- FK는 동일 selection을 보장하고 이 게이트는 원본의 정적 검증 및 전바이트 동일성을
-- 보장한다. 동적 원본을 가리켜 partial unique index를 우회하는 직접 SQL도 거부한다.
CREATE FUNCTION catalog.guard_vector_tile_readdress_release()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
    source catalog.vector_tile_release%ROWTYPE;
BEGIN
    IF NEW.readdressed_from_release_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO source FROM catalog.vector_tile_release
    WHERE id = NEW.readdressed_from_release_id FOR SHARE;
    IF NOT FOUND OR NOT catalog.is_validated_static_tile_release(source)
       OR NEW.pmtiles_sha256 IS DISTINCT FROM source.pmtiles_sha256
       OR NEW.pmtiles_bytes IS DISTINCT FROM source.pmtiles_bytes
       OR NEW.source_record_id IS DISTINCT FROM source.source_record_id
       OR NEW.source_file_asset_ids IS DISTINCT FROM source.source_file_asset_ids THEN
        RAISE EXCEPTION 'readdress release must preserve validated static source bytes and lineage'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

CREATE TRIGGER vector_tile_release_readdress_guard
    BEFORE INSERT OR UPDATE ON catalog.vector_tile_release
    FOR EACH ROW EXECUTE FUNCTION catalog.guard_vector_tile_readdress_release();

ALTER TABLE catalog.catalog_mutation_idempotency
    DROP CONSTRAINT catalog_mutation_idempotency_command_kind_check,
    ADD CONSTRAINT catalog_mutation_idempotency_command_kind_check CHECK (
        command_kind IN ('mark_tile_layer_dynamic', 'start_vector_tile_build',
            'start_static_release_readdress', 'promote_tile_layer_static', 'rollback_tile_layer_source')
    ),
    DROP CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check,
    ADD CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check CHECK (
        (command_kind IN ('mark_tile_layer_dynamic', 'promote_tile_layer_static', 'rollback_tile_layer_source')
            AND outcome_manifest_id IS NOT NULL)
        OR (command_kind IN ('start_vector_tile_build', 'start_static_release_readdress')
            AND outcome_manifest_id IS NULL)
    );
