-- Enterprise Runtime C0/C1 foundation: normalization outbox + audit tables.
-- Safe to re-run (IF NOT EXISTS throughout).

CREATE TABLE IF NOT EXISTS ip_normalization_outbox (
    id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    aggregatetype TEXT NOT NULL DEFAULT 'normalization-proposal',
    aggregateid TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'normalization-proposal.submission-requested',
    payload JSONB NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    ce_id UUID NOT NULL,
    ce_source TEXT NOT NULL DEFAULT '/intelligence-platform/normalization',
    ce_specversion TEXT NOT NULL DEFAULT '1.0',
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    submission_result JSONB,
    last_error TEXT,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ip_normalization_outbox_claimable
    ON ip_normalization_outbox (status, claimed_until, updated_at);

CREATE TABLE IF NOT EXISTS ip_normalization_audit_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    trace_context JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ip_normalization_audit_created
    ON ip_normalization_audit_events (created_at);
