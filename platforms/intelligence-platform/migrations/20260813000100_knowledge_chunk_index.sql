-- 지식 검색 수직 슬라이스의 색인 표.
-- 벡터 컬럼과 확장은 의도적으로 없다 — Intelligence ADR-0002가 승인되지 않은
-- embedding provider·vector index를 코드에 고정하는 것을 금지한다.

CREATE TABLE IF NOT EXISTS ip_knowledge_release (
    tenant_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, product_id, release_id)
);

-- 범위마다 활성 release는 최대 하나. 교체 규칙을 응용 코드가 아니라 DB가 강제한다.
CREATE UNIQUE INDEX IF NOT EXISTS idx_ip_knowledge_release_single_active
    ON ip_knowledge_release (tenant_id, product_id)
    WHERE is_active;

CREATE TABLE IF NOT EXISTS ip_knowledge_chunk (
    tenant_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    chunk_ordinal INTEGER NOT NULL,
    heading_path TEXT NOT NULL,
    body TEXT NOT NULL,
    -- ADR-0002의 네 식별자: source_id(=canonical_entity_id), source_snapshot_id,
    -- release_id, content_checksum.
    source_snapshot_id TEXT NOT NULL,
    content_checksum_sha256 TEXT NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 한국어 형태소 분석기가 없으므로 'simple'을 쓴다. 조사가 토큰에 붙어 재현율이
    -- 낮다는 것은 알려진 한계이며 이 슬라이스의 합격 기준이 아니다.
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', heading_path || ' ' || body)
    ) STORED,
    PRIMARY KEY (tenant_id, product_id, release_id, source_id, chunk_ordinal),
    FOREIGN KEY (tenant_id, product_id, release_id)
        REFERENCES ip_knowledge_release (tenant_id, product_id, release_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ip_knowledge_chunk_search
    ON ip_knowledge_chunk USING GIN (search_vector);
