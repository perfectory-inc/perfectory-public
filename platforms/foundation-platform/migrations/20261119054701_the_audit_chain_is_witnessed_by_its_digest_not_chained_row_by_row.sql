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
-- - Each row carries `txid`, the id of the transaction that wrote it. A digest is the snapshot it was
--   made in and covers the rows whose transaction that snapshot sees and the digest before's did not.
--   A snapshot's set of seen transactions never changes, so no row can arrive in a range once it is
--   witnessed, a long transaction holds back only its own rows, and no lock is needed for any of it.
-- - A range's hash is `audit.rows_sha256`: SHA-256 of the rows' canonical text, one per line,
--   in `seq` order. A row rewritten, removed, added or reordered inside a witnessed range changes it.
--
-- On 2026-11-19 `audit.event` held 0 rows and `audit.digest` 0 in production; the digest table is
-- recreated in its new shape (with its witness apart, and a trigger that makes each digest follow
-- the one before and match its rows), and refuses to be if it ever held one.

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

-- One witnessed range of the record (root ADR-0584 §2). A digest is the snapshot it was made in: it
-- covers the rows whose transaction is visible in `to_snapshot` and was not in the digest before's
-- (`from_snapshot`, the same value; none for the first). Once a snapshot is taken, which transactions
-- it sees can never change, so a row cannot arrive in a range already made, and one long transaction
-- anywhere on the cluster holds back only its own rows, not everything after it.
CREATE TABLE audit.digest (
    digest_no              bigint      PRIMARY KEY CHECK (digest_no >= 1),
    from_snapshot          pg_snapshot,
    to_snapshot            pg_snapshot NOT NULL,
    row_count              bigint      NOT NULL CHECK (row_count >= 0),
    rows_sha256            bytea       NOT NULL CHECK (length(rows_sha256) = 32),
    previous_digest_sha256 bytea       CHECK (previous_digest_sha256 IS NULL OR length(previous_digest_sha256) = 32),
    digest_sha256          bytea       NOT NULL UNIQUE CHECK (length(digest_sha256) = 32),
    made_at                timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT digest_first_has_no_previous CHECK ((digest_no = 1) = (previous_digest_sha256 IS NULL)),
    CONSTRAINT digest_first_starts_from_nothing CHECK ((digest_no = 1) = (from_snapshot IS NULL))
);
CREATE TRIGGER digest_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.digest
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();

-- Where the public log holds each digest. Apart from the digest so that a digest is stored before it
-- is uploaded: a retry after a failed upload uploads the same digest again (Rekor answers an
-- identical entry with the one it has) instead of making a second digest of the same range.
CREATE TABLE audit.digest_witness (
    digest_no             bigint      PRIMARY KEY REFERENCES audit.digest (digest_no),
    rekor_log_index       bigint      NOT NULL CHECK (rekor_log_index >= 0),
    rekor_inclusion_proof jsonb       NOT NULL,
    witnessed_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER digest_witness_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.digest_witness
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

-- The rows between two snapshots: written by a transaction visible in `p_to` and not in `p_from`
-- (none: from the beginning). `pg_visible_in_snapshot` answers for the top-level id, which is what
-- `txid` holds (`pg_current_xact_id()` inside a savepoint is still the outer transaction's id). The
-- id bounds only let the index narrow the scan. The digest and the audit feed both read through this.
-- The answer does not depend on when it is asked, as long as every transaction `p_to` sees has ended
-- (a snapshot that was current once): those rows are committed and visible to any later reader, and
-- rows of later transactions are not visible in `p_to`.
CREATE FUNCTION audit.rows_between(p_from pg_snapshot, p_to pg_snapshot) RETURNS SETOF audit.event
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT e.* FROM audit.event AS e
    WHERE e.txid < pg_snapshot_xmax(p_to)
      AND pg_visible_in_snapshot(e.txid, p_to)
      AND (p_from IS NULL
           OR (e.txid >= pg_snapshot_xmin(p_from) AND NOT pg_visible_in_snapshot(e.txid, p_from)))
$$;

CREATE FUNCTION audit.range_sha256(p_from pg_snapshot, p_to pg_snapshot) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT audit.rows_sha256(coalesce(array_agg(r), '{}')) FROM audit.rows_between(p_from, p_to) AS r
$$;

-- What a digest's own hash covers, and the bytes anyone re-hashes to check it: one jsonb object's text
-- of these seven fields (snapshots in their text form, hashes in lower-case hex, the first's missing
-- values as null). `made_at` and the witness are outside it.
CREATE FUNCTION audit.digest_sha256(
    p_digest_no bigint, p_from pg_snapshot, p_to pg_snapshot, p_row_count bigint, p_rows_sha256 bytea,
    p_previous bytea
) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT sha256(convert_to(jsonb_build_object(
        'schema', 'audit.digest.v1',
        'digest_no', p_digest_no,
        'from_snapshot', p_from::text,
        'to_snapshot', p_to::text,
        'row_count', p_row_count,
        'rows_sha256', encode(p_rows_sha256, 'hex'),
        'previous_digest_sha256', encode(p_previous, 'hex')
    )::text, 'UTF8'))
$$;

-- A digest follows the one before without a gap, and says only what is true of the rows: the next
-- number, the previous digest's snapshot as its start and its hash as `previous_digest_sha256`, a
-- snapshot that does not go back (a restored dump restarts transaction ids, and every later row would
-- look already covered: refused, so the digest job fails loudly), and its range's real count and hash.
CREATE FUNCTION audit.digest_follows() RETURNS trigger
LANGUAGE plpgsql SET search_path = audit, pg_temp AS $$
DECLARE
    prev audit.digest;
    actual_count bigint;
BEGIN
    LOCK TABLE audit.digest IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO prev FROM audit.digest ORDER BY digest_no DESC LIMIT 1;
    IF FOUND THEN
        IF NEW.digest_no <> prev.digest_no + 1
           OR NEW.from_snapshot IS NULL OR NEW.from_snapshot::text <> prev.to_snapshot::text
           OR NEW.previous_digest_sha256 IS DISTINCT FROM prev.digest_sha256 THEN
            RAISE EXCEPTION 'digest % does not follow digest % (root ADR-0584 §2)', NEW.digest_no, prev.digest_no;
        END IF;
        IF pg_snapshot_xmax(NEW.to_snapshot) < pg_snapshot_xmax(prev.to_snapshot)
           OR pg_snapshot_xmin(NEW.to_snapshot) < pg_snapshot_xmin(prev.to_snapshot) THEN
            RAISE EXCEPTION 'digest % snapshot % is older than digest % snapshot % (restored cluster?)',
                NEW.digest_no, NEW.to_snapshot, prev.digest_no, prev.to_snapshot;
        END IF;
    ELSIF NEW.digest_no <> 1 THEN
        RAISE EXCEPTION 'the first digest is number 1, not %', NEW.digest_no;
    END IF;
    -- A snapshot that was current once: every transaction it sees has ended. A made-up snapshot that
    -- sees a transaction still running would name rows that are not there yet.
    IF pg_snapshot_xmax(NEW.to_snapshot) > pg_snapshot_xmax(pg_current_snapshot())
       OR EXISTS (SELECT FROM pg_snapshot_xip(pg_current_snapshot()) AS running(x)
                  WHERE x < pg_snapshot_xmax(NEW.to_snapshot) AND pg_visible_in_snapshot(x, NEW.to_snapshot)) THEN
        RAISE EXCEPTION 'digest % snapshot % sees a transaction that has not ended', NEW.digest_no, NEW.to_snapshot;
    END IF;
    SELECT count(*) INTO actual_count FROM audit.rows_between(NEW.from_snapshot, NEW.to_snapshot);
    IF NEW.row_count <> actual_count
       OR NEW.rows_sha256 <> audit.range_sha256(NEW.from_snapshot, NEW.to_snapshot) THEN
        RAISE EXCEPTION 'digest % does not match its rows', NEW.digest_no;
    END IF;
    IF NEW.digest_sha256 <> audit.digest_sha256(NEW.digest_no, NEW.from_snapshot, NEW.to_snapshot,
                                                NEW.row_count, NEW.rows_sha256, NEW.previous_digest_sha256) THEN
        RAISE EXCEPTION 'digest % has the wrong digest_sha256', NEW.digest_no;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER digest_follows BEFORE INSERT ON audit.digest
    FOR EACH ROW EXECUTE FUNCTION audit.digest_follows();

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
