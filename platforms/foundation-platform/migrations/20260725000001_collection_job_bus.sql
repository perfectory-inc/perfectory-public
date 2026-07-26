-- Durable Collection Event Fabric job dispatch state (ADR 0013 / ADR-0047).
--
-- The table is the Postgres JobBus queue. The state machine is deliberately separate from
-- catalog.outbox_event: outbox_event is the fan-out notification log, while this table owns
-- collection work leases and retry/dead-letter state.

CREATE TABLE catalog.collection_job (
    job_id text NOT NULL,
    scope_unit_id text NOT NULL,
    shard_id text NOT NULL,
    provider text NOT NULL,
    endpoint text NOT NULL,
    endpoint_slug text NOT NULL,
    idempotency_key text NOT NULL,
    request_fingerprint_sha256 character(64) NOT NULL,
    request_fingerprint_schema_version text NOT NULL,
    collection_snapshot_id text NOT NULL,
    spec jsonb NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    leased_at timestamp with time zone,
    completed_at timestamp with time zone,
    dead_lettered_at timestamp with time zone,
    last_failure jsonb,
    success jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT collection_job_pkey PRIMARY KEY (job_id),
    CONSTRAINT collection_job_job_id_check CHECK ((btrim(job_id) <> ''::text)),
    CONSTRAINT collection_job_scope_unit_id_check CHECK ((btrim(scope_unit_id) <> ''::text)),
    CONSTRAINT collection_job_shard_id_check CHECK ((btrim(shard_id) <> ''::text)),
    CONSTRAINT collection_job_provider_check CHECK ((btrim(provider) <> ''::text)),
    CONSTRAINT collection_job_endpoint_check CHECK ((btrim(endpoint) <> ''::text)),
    CONSTRAINT collection_job_endpoint_slug_check CHECK ((btrim(endpoint_slug) <> ''::text)),
    CONSTRAINT collection_job_idempotency_key_check CHECK ((btrim(idempotency_key) <> ''::text)),
    CONSTRAINT collection_job_request_fingerprint_sha256_check CHECK ((request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT collection_job_request_fingerprint_schema_version_check CHECK ((btrim(request_fingerprint_schema_version) <> ''::text)),
    CONSTRAINT collection_job_snapshot_check CHECK ((btrim(collection_snapshot_id) <> ''::text)),
    CONSTRAINT collection_job_spec_check CHECK ((jsonb_typeof(spec) = 'object'::text)),
    CONSTRAINT collection_job_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'in_flight'::text, 'completed'::text, 'dead_lettered'::text]))),
    CONSTRAINT collection_job_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT collection_job_failure_check CHECK (((last_failure IS NULL) OR (jsonb_typeof(last_failure) = 'object'::text))),
    CONSTRAINT collection_job_success_check CHECK (((success IS NULL) OR (jsonb_typeof(success) = 'object'::text)))
);

CREATE INDEX collection_job_poll_idx
    ON catalog.collection_job USING btree (available_at, created_at, job_id)
    WHERE (state = 'pending'::text);

CREATE INDEX collection_job_expired_lease_idx
    ON catalog.collection_job USING btree (leased_at, created_at, job_id)
    WHERE (state = 'in_flight'::text);

CREATE INDEX collection_job_state_updated_idx
    ON catalog.collection_job USING btree (state, updated_at DESC, job_id);
