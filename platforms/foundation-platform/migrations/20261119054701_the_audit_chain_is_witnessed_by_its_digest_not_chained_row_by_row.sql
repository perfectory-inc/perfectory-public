-- The audit chain is witnessed by its hourly digest, not chained row by row (root ADR-0584, which
-- replaces ADR-0569 §3's writer lock and §4's per-row hash chain).
--
-- ADR-0569's own threat model puts the evidence at the hourly digest the host signs into the public
-- Rekor log: whoever can disable the append-only triggers can also recompute a per-row chain, so the
-- chain added nothing the digest does not already give. It cost one writer at a time (an advisory
-- lock), a READ COMMITTED-only rule, and a 503 for a request that waited too long on that lock.
--
-- What stays: one door (`audit.append`, which checks the caller's source), the append-only
-- triggers, the operator ledger recording itself in its own transaction, and `(source,
-- source_event_id)` written once.
--
-- What changes:
-- - `seq` is an identity: it orders rows and pages the console, and may have holes (a rolled-back
--   or duplicate append spends a number). The digest does not count numbers; it hashes rows.
-- - Each row carries `txid`, the id of the transaction that wrote it. A digest covers the rows whose
--   `txid` is in `[from_txid, to_txid)`, where `to_txid` is `pg_snapshot_xmin` when the digest is
--   made: every transaction below it has ended, so no row can still arrive in a range once it is
--   witnessed, and no lock is needed for that.
-- - A range's hash is `audit.rows_sha256`: SHA-256 of the rows' canonical text, one per line,
--   in `seq` order. A row rewritten, removed, added or reordered inside a witnessed range changes it.
--
-- On 2026-11-19 `audit.event` held 0 rows and `audit.digest` 0 in production; the digest table is
-- recreated in its new shape, and refuses to be if it ever held one.

-- The per-row chain's functions go first; `audit.append` and the ledger trigger are replaced below.
DROP FUNCTION audit.append(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid);
DROP FUNCTION audit.chain(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid);
DROP FUNCTION audit.row_hash(audit.event);
DROP FUNCTION audit.canon(audit.event);

ALTER TABLE audit.event DROP COLUMN prev_hash, DROP COLUMN row_hash;
ALTER TABLE audit.event ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY;
SELECT setval(pg_get_serial_sequence('audit.event', 'seq'),
              coalesce((SELECT max(seq) FROM audit.event), 0) + 1, false);
ALTER TABLE audit.event ADD COLUMN txid xid8 NOT NULL DEFAULT pg_current_xact_id();
CREATE INDEX event_txid_idx ON audit.event (txid);

DO $$
BEGIN
    IF EXISTS (SELECT FROM audit.digest) THEN
        RAISE EXCEPTION 'audit.digest holds a digest of the per-row chain; it is never dropped (root ADR-0584)';
    END IF;
END;
$$;
DROP TABLE audit.digest;

-- One witnessed range of the record (root ADR-0584 §2). Digests follow one another without a gap:
-- each starts at the one before's `to_txid` and names its hash.
CREATE TABLE audit.digest (
    digest_no              bigint      PRIMARY KEY CHECK (digest_no >= 1),
    from_txid              xid8        NOT NULL,
    to_txid                xid8        NOT NULL,
    row_count              bigint      NOT NULL CHECK (row_count >= 0),
    rows_sha256            bytea       NOT NULL CHECK (length(rows_sha256) = 32),
    previous_digest_sha256 bytea       CHECK (previous_digest_sha256 IS NULL OR length(previous_digest_sha256) = 32),
    digest_sha256          bytea       NOT NULL UNIQUE CHECK (length(digest_sha256) = 32),
    rekor_log_index        bigint,
    rekor_inclusion_proof  jsonb,
    made_at                timestamptz NOT NULL DEFAULT now(),
    UNIQUE (from_txid),
    UNIQUE (to_txid),
    CHECK (from_txid <= to_txid),
    CONSTRAINT digest_first_has_no_previous CHECK ((digest_no = 1) = (previous_digest_sha256 IS NULL))
);
CREATE TRIGGER digest_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.digest
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();

-- The bytes a row contributes to its range's hash: every column, as one jsonb object's text.
-- Postgres prints equal jsonb values as equal text (keys ordered by length, then bytes; one spacing),
-- so the same row is the same bytes on every host and release. Computed here only; the fixed vector
-- in catalog-infrastructure/tests/audit_event_record.rs pins it.
CREATE FUNCTION audit.canon(e audit.event) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT jsonb_build_object(
        'seq', e.seq,
        'txid', e.txid::text,
        'event_id', e.event_id,
        'occurred_at', audit.canon_time(e.occurred_at),
        'recorded_at', audit.canon_time(e.recorded_at),
        'source', e.source,
        'source_event_id', e.source_event_id,
        'action', e.action,
        'operation', e.operation,
        'actor_kind', e.actor_kind,
        'actor_staff_id', e.actor_staff_id,
        'actor_subject', e.actor_subject,
        'actor_external', e.actor_external,
        'actor_verified', e.actor_verified,
        'on_behalf_of', e.on_behalf_of,
        'object_type', e.object_type,
        'object_id', e.object_id,
        'before', e.before,
        'after', e.after,
        'reason', e.reason,
        'applied_via_kind', e.applied_via_kind,
        'applied_via_ref', e.applied_via_ref,
        'result', e.result,
        'error', e.error,
        'change_id', e.change_id
    )::text
$$;

-- The hash of a set of rows: SHA-256 of their canonical text, one row per line, in `seq` order.
-- No rows hash to SHA-256 of nothing.
CREATE FUNCTION audit.rows_sha256(rows audit.event[]) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT sha256(convert_to(coalesce(string_agg(audit.canon(r) || E'\n', '' ORDER BY r.seq), ''), 'UTF8'))
    FROM unnest(rows) AS r
$$;

-- The rows a digest covers: those whose transaction id is in `[p_from, p_to)`.
CREATE FUNCTION audit.range_sha256(p_from xid8, p_to xid8) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT audit.rows_sha256(coalesce(array_agg(e), '{}'))
    FROM audit.event AS e WHERE e.txid >= p_from AND e.txid < p_to
$$;

-- Writes one event. Not callable by any application role: `audit.append` (which checks the caller's
-- source) and the operator ledger's trigger (whose INSERT grant is the check) call it. Returns the
-- new `seq`, or NULL when (source, source_event_id) is already recorded: an importer that reads a
-- source twice writes it once. No lock: concurrent writers each insert their own row.
CREATE FUNCTION audit.record(
    p_event_id uuid, p_occurred_at timestamptz, p_source text, p_source_event_id text,
    p_action text, p_operation text, p_actor_kind text, p_actor_staff_id uuid, p_actor_subject text,
    p_actor_external text, p_actor_verified boolean, p_on_behalf_of text, p_object_type text,
    p_object_id text, p_before jsonb, p_after jsonb, p_reason text, p_applied_via_kind text,
    p_applied_via_ref text, p_result text, p_error text, p_change_id uuid
) RETURNS bigint
LANGUAGE plpgsql SET search_path = audit, pg_temp AS $$
DECLARE
    new_seq bigint;
BEGIN
    INSERT INTO audit.event (
        event_id, occurred_at, recorded_at, source, source_event_id, action, operation, actor_kind,
        actor_staff_id, actor_subject, actor_external, actor_verified, on_behalf_of, object_type,
        object_id, before, after, reason, applied_via_kind, applied_via_ref, result, error, change_id)
    VALUES (
        p_event_id, p_occurred_at, clock_timestamp(), p_source, p_source_event_id, p_action, p_operation,
        p_actor_kind, p_actor_staff_id, p_actor_subject, p_actor_external, p_actor_verified,
        p_on_behalf_of, p_object_type, p_object_id, p_before, p_after, p_reason, p_applied_via_kind,
        p_applied_via_ref, p_result, p_error, p_change_id)
    ON CONFLICT (source, source_event_id) DO NOTHING
    RETURNING seq INTO new_seq;
    RETURN new_seq;
END;
$$;

-- The door every application role writes through. The source is the caller's only if its role is
-- listed as that source's writer, and the actor counts as verified only if that source verifies it.
CREATE FUNCTION audit.append(
    p_event_id uuid, p_occurred_at timestamptz, p_source text, p_source_event_id text,
    p_action text, p_operation text, p_actor_kind text, p_actor_staff_id uuid, p_actor_subject text,
    p_actor_external text, p_actor_verified boolean, p_on_behalf_of text, p_object_type text,
    p_object_id text, p_before jsonb, p_after jsonb, p_reason text, p_applied_via_kind text,
    p_applied_via_ref text, p_result text, p_error text, p_change_id uuid
) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, pg_temp AS $$
DECLARE
    verifies boolean;
BEGIN
    SELECT verifies_actor INTO verifies FROM audit.source
     WHERE source = p_source AND writer_role = session_user;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'role % may not write audit source % (root ADR-0569 §3)', session_user, p_source
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN audit.record(
        p_event_id, p_occurred_at, p_source, p_source_event_id, p_action, p_operation, p_actor_kind,
        p_actor_staff_id, p_actor_subject, p_actor_external, p_actor_verified AND verifies, p_on_behalf_of,
        p_object_type, p_object_id, p_before, p_after, p_reason, p_applied_via_kind, p_applied_via_ref,
        p_result, p_error, p_change_id);
END;
$$;

REVOKE ALL ON FUNCTION audit.record(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION audit.append(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid) FROM PUBLIC;

-- The ledger's request and its answer become audit events in the ledger's own transaction, as
-- before; only the internal step's name changed.
CREATE OR REPLACE FUNCTION catalog.operator_action_is_audited() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM audit.record(
            gen_random_uuid(), NEW.requested_at, 'dawneer', 'operator_action:' || NEW.id || ':requested',
            'operator_action.' || NEW.action, 'execute', 'staff', NEW.actor::uuid, NULL, NULL, true, NULL,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN 'lane' ELSE 'job' END,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN coalesce(NEW.lanes, NEW.job_id) ELSE NEW.job_id END,
            NULL, catalog.operator_action_arguments(NEW), NEW.reason, 'api', NEW.id::text, 'requested', NULL, NEW.id);
    ELSIF OLD.consumed_at IS NULL AND NEW.consumed_at IS NOT NULL THEN
        PERFORM audit.record(
            gen_random_uuid(), NEW.consumed_at, 'dawneer', 'operator_action:' || NEW.id || ':answered',
            'operator_action.' || NEW.action, 'execute', 'system', NULL, NULL, NULL, false, NULL,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN 'lane' ELSE 'job' END,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN coalesce(NEW.lanes, NEW.job_id) ELSE NEW.job_id END,
            NULL, jsonb_build_object('outcome', NEW.outcome), NULL, 'api', NEW.id::text, 'succeeded', NULL, NEW.id);
    END IF;
    RETURN NULL;
END;
$$;
