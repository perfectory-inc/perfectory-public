CREATE SCHEMA identity;

REVOKE ALL ON SCHEMA identity FROM PUBLIC;

CREATE TABLE identity.staff (
    id                  UUID CONSTRAINT identity_staff_pkey PRIMARY KEY,
    zitadel_subject     TEXT NOT NULL,
    email               TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    primary_role_code   TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    version             BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT identity_staff_zitadel_subject_key UNIQUE (zitadel_subject),
    CONSTRAINT identity_staff_primary_role_code_check
        CHECK (primary_role_code ~ '^[A-Z0-9_]+$'),
    CONSTRAINT identity_staff_version_check CHECK (version >= 1)
);

CREATE TABLE identity.staff_role (
    staff_id            UUID NOT NULL,
    role_code           TEXT NOT NULL,
    granted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by          UUID NOT NULL,
    CONSTRAINT identity_staff_role_pkey PRIMARY KEY (staff_id, role_code),
    CONSTRAINT identity_staff_role_staff_id_fkey
        FOREIGN KEY (staff_id) REFERENCES identity.staff(id) ON DELETE CASCADE,
    CONSTRAINT identity_staff_role_granted_by_fkey
        FOREIGN KEY (granted_by) REFERENCES identity.staff(id),
    CONSTRAINT identity_staff_role_role_code_check CHECK (role_code ~ '^[A-Z0-9_]+$')
);

CREATE TABLE identity.staff_session (
    session_id          UUID CONSTRAINT identity_staff_session_pkey PRIMARY KEY,
    staff_id            UUID NOT NULL,
    jti                 TEXT NOT NULL,
    issued_at           TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    CONSTRAINT identity_staff_session_staff_id_fkey
        FOREIGN KEY (staff_id) REFERENCES identity.staff(id) ON DELETE CASCADE,
    CONSTRAINT identity_staff_session_jti_key UNIQUE (jti),
    CONSTRAINT identity_staff_session_time_order_check CHECK (expires_at > issued_at)
);

CREATE INDEX identity_staff_session_expiry_idx
    ON identity.staff_session (expires_at);

CREATE TABLE identity.revoked_jti (
    jti                 TEXT CONSTRAINT identity_revoked_jti_pkey PRIMARY KEY,
    revoked_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason              TEXT NOT NULL
);

CREATE TABLE identity.service_principal (
    id                  UUID CONSTRAINT identity_service_principal_pkey PRIMARY KEY,
    zitadel_subject     TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT identity_service_principal_zitadel_subject_key UNIQUE (zitadel_subject)
);

CREATE TABLE identity.service_capability_grant (
    service_principal_id UUID NOT NULL,
    capability           TEXT NOT NULL,
    granted_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT identity_service_capability_grant_pkey
        PRIMARY KEY (service_principal_id, capability),
    CONSTRAINT identity_service_capability_grant_principal_fkey
        FOREIGN KEY (service_principal_id)
        REFERENCES identity.service_principal(id) ON DELETE CASCADE,
    CONSTRAINT identity_service_capability_grant_capability_check
        CHECK (capability ~ '^[^:]*:[^:]*$')
);

CREATE TABLE identity.outbox_event (
    event_id            UUID CONSTRAINT identity_outbox_event_pkey PRIMARY KEY,
    type                TEXT NOT NULL,
    payload             JSONB NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at        TIMESTAMPTZ,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner         TEXT,
    claim_token         UUID,
    lease_expires_at    TIMESTAMPTZ,
    last_error_code     TEXT,
    CONSTRAINT identity_outbox_event_type_check
        CHECK (type ~ '^identity\.[a-z0-9_]+(\.[a-z0-9_]+)*\.v1$'),
    CONSTRAINT identity_outbox_event_payload_check CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT identity_outbox_event_attempt_count_check CHECK (attempt_count BETWEEN 0 AND 1000),
    CONSTRAINT identity_outbox_event_lease_check CHECK (
        (lease_owner IS NULL AND claim_token IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_owner IS NOT NULL AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT identity_outbox_event_last_error_code_check
        CHECK (last_error_code IS NULL OR char_length(last_error_code) <= 64)
);

CREATE INDEX identity_outbox_event_due_idx
    ON identity.outbox_event (next_attempt_at, occurred_at, event_id)
    WHERE published_at IS NULL AND attempt_count < 1000;

REVOKE ALL ON ALL TABLES IN SCHEMA identity FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA identity FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA identity REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA identity REVOKE ALL ON SEQUENCES FROM PUBLIC;
