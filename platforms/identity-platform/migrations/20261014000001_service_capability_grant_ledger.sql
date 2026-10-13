-- Service capability grants become an append-only ledger (root ADR-0212).
--
-- Before: the provisioner reconciled `identity.service_capability_grant` with
-- DELETE + INSERT and wrote no event, so a revoked capability left no trace.
-- After: every grant and revoke is one immutable row here, the provisioner
-- writes an `identity.service_principal.capability_*.v1` outbox event in the
-- same transaction, and the old table name becomes a view of the latest change
-- per (principal, capability) — readers keep the same three columns.

CREATE TABLE identity.service_capability_grant_record (
    record_seq           BIGINT GENERATED ALWAYS AS IDENTITY
        CONSTRAINT identity_service_capability_grant_record_pkey PRIMARY KEY,
    service_principal_id UUID NOT NULL,
    capability           TEXT NOT NULL,
    change               TEXT NOT NULL,
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source               TEXT NOT NULL,
    policy_sha256        TEXT,
    CONSTRAINT identity_service_capability_grant_record_principal_fkey
        FOREIGN KEY (service_principal_id) REFERENCES identity.service_principal(id),
    CONSTRAINT identity_service_capability_grant_record_capability_check
        CHECK (capability ~ '^[^:]*:[^:]*$'),
    CONSTRAINT identity_service_capability_grant_record_change_check
        CHECK (change IN ('granted', 'revoked')),
    CONSTRAINT identity_service_capability_grant_record_source_check
        CHECK (
            (source = 'workload_policy' AND policy_sha256 ~ '^[0-9a-f]{64}$')
            OR (source = 'baseline_backfill' AND policy_sha256 IS NULL)
        )
);

CREATE INDEX identity_service_capability_grant_record_latest_idx
    ON identity.service_capability_grant_record
       (service_principal_id, capability, record_seq DESC);

CREATE FUNCTION identity.reject_service_capability_grant_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, identity, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'service capability grant records are append-only; append a revoke instead'
        USING ERRCODE = '42501';
END;
$function$;

CREATE TRIGGER service_capability_grant_record_append_only
BEFORE UPDATE OR DELETE ON identity.service_capability_grant_record
FOR EACH ROW EXECUTE FUNCTION identity.reject_service_capability_grant_record_mutation();

CREATE TRIGGER service_capability_grant_record_no_truncate
BEFORE TRUNCATE ON identity.service_capability_grant_record
FOR EACH STATEMENT EXECUTE FUNCTION identity.reject_service_capability_grant_record_mutation();

INSERT INTO identity.service_capability_grant_record
    (service_principal_id, capability, change, recorded_at, source)
SELECT service_principal_id, capability, 'granted', granted_at, 'baseline_backfill'
FROM identity.service_capability_grant
ORDER BY granted_at, service_principal_id, capability;

DROP TABLE identity.service_capability_grant;

CREATE VIEW identity.service_capability_grant AS
SELECT latest.service_principal_id, latest.capability, latest.recorded_at AS granted_at
FROM (
    SELECT DISTINCT ON (service_principal_id, capability)
           service_principal_id, capability, change, recorded_at
    FROM identity.service_capability_grant_record
    ORDER BY service_principal_id, capability, record_seq DESC
) AS latest
WHERE latest.change = 'granted';

REVOKE ALL ON identity.service_capability_grant_record FROM PUBLIC;
REVOKE ALL ON identity.service_capability_grant FROM PUBLIC;
