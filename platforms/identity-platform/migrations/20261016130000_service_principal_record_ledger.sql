-- A service principal's subject and display name keep every value they had (root ADR-0264).
--
-- The provisioner upserts `identity.service_principal` and overwrites `zitadel_subject` and
-- `display_name` when the workload policy or bindings change, so the subject a principal was bound
-- to yesterday is gone today. Grants already have an append-only ledger (root ADR-0212); this is
-- the same ledger for the principal itself. The provisioner appends a record only when a value
-- changes, in the same transaction as the upsert. `identity.service_principal` stays the current
-- row: grant records reference it and readers keep reading it.

CREATE TABLE identity.service_principal_record (
    record_seq           BIGINT GENERATED ALWAYS AS IDENTITY
        CONSTRAINT identity_service_principal_record_pkey PRIMARY KEY,
    service_principal_id UUID NOT NULL,
    zitadel_subject      TEXT NOT NULL,
    display_name         TEXT NOT NULL,
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source               TEXT NOT NULL,
    policy_sha256        TEXT,
    CONSTRAINT identity_service_principal_record_principal_fkey
        FOREIGN KEY (service_principal_id) REFERENCES identity.service_principal(id),
    CONSTRAINT identity_service_principal_record_source_check
        CHECK (
            (source = 'workload_policy' AND policy_sha256 ~ '^[0-9a-f]{64}$')
            OR (source = 'baseline_backfill' AND policy_sha256 IS NULL)
        )
);

CREATE INDEX identity_service_principal_record_latest_idx
    ON identity.service_principal_record (service_principal_id, record_seq DESC);

CREATE FUNCTION identity.reject_service_principal_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, identity, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'service principal records are append-only; append the new value instead'
        USING ERRCODE = '42501';
END;
$function$;

CREATE TRIGGER service_principal_record_append_only
BEFORE UPDATE OR DELETE ON identity.service_principal_record
FOR EACH ROW EXECUTE FUNCTION identity.reject_service_principal_record_mutation();

CREATE TRIGGER service_principal_record_no_truncate
BEFORE TRUNCATE ON identity.service_principal_record
FOR EACH STATEMENT EXECUTE FUNCTION identity.reject_service_principal_record_mutation();

-- What each principal holds now is the first record; earlier values were already overwritten.
INSERT INTO identity.service_principal_record
    (service_principal_id, zitadel_subject, display_name, recorded_at, source)
SELECT id, zitadel_subject, display_name, updated_at, 'baseline_backfill'
FROM identity.service_principal
ORDER BY updated_at, id;

REVOKE ALL ON identity.service_principal_record FROM PUBLIC;
