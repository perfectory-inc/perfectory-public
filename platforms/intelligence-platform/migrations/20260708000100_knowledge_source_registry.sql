-- C2-B knowledge source registry.
-- Durable effect for contract-gated foundation knowledge consumers.

CREATE TABLE IF NOT EXISTS ip_source_registry (
    tenant_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    content_uri TEXT,
    content_checksum_sha256 TEXT,
    last_event_id TEXT NOT NULL,
    last_seen_at_millis BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, product_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_ip_source_registry_updated_at
    ON ip_source_registry (updated_at);

CREATE INDEX IF NOT EXISTS idx_ip_source_registry_source_kind
    ON ip_source_registry (tenant_id, product_id, source_kind);
