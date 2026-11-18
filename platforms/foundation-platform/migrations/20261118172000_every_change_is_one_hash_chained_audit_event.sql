-- Every change to the platform is one hash-chained audit event keyed by the staff account (root ADR-0569).
--
-- The ways the platform changes (a console switch, a map edit, a merged PR, a deploy, an operator's
-- host command) each kept their own record, in their own shape, or none: on 2026-11-19
-- `catalog.operator_action` held 0 rows, the host commands 183 journal lines with a unix login, the
-- deploys 500 state files that name nobody. "Who changed this, when and why" had no one place.
--
-- `audit.event` is that place. A row is never rewritten or removed, and it is written by one door:
--
-- - `audit.append` (SECURITY DEFINER) checks that the session's role may write the event's source
--   (`audit.source`, the vocabulary contract `config/audit-events.contract.json`), so a role cannot
--   claim another source's events, and that the transaction is READ COMMITTED: under REPEATABLE READ
--   the snapshot is taken before the lock and two rows would chain onto the same `prev_hash`.
-- - Every row carries `prev_hash`, the `row_hash` of the row before it, and
--   `row_hash = sha256(prev_hash || canon(row))`. The table owner can disable the triggers and
--   rewrite a row, and recompute the chain after it; the hourly digest the host witnesses in the
--   public Rekor log (§4, a later stage) is what makes that visible.
-- - `catalog.operator_action` records itself: its AFTER triggers append the request, and the
--   answer when `consumed_at` is filled, in the same transaction. No ledger row without its audit
--   row; an audit row that cannot be written refuses the ledger row too.
--
-- No sequence: a rolled-back transaction would spend a number and leave a hole in `seq`, which the
-- digest could not tell from a deleted row. `seq` is the last one plus one, under a transaction lock.

CREATE SCHEMA IF NOT EXISTS audit;

-- Who may write which source, and whether that source verified the actor it names. One row per
-- (source, writer role). The contract `config/audit-events.contract.json` lists the same rows, and
-- orchestration/tests/test_audit_events_contract.py refuses any difference.
CREATE TABLE audit.source (
    source text NOT NULL CHECK (source ~ '^[a-z_]+$'),
    writer_role text NOT NULL CHECK (writer_role ~ '^[a-z_]+$'),
    verifies_actor boolean NOT NULL,
    PRIMARY KEY (source, writer_role)
);

INSERT INTO audit.source (source, writer_role, verifies_actor) VALUES
    ('dawneer', 'foundation_api', true),
    ('audit', 'foundation_api', true),
    ('audit', 'foundation_audit_importer', false),
    ('github', 'foundation_audit_importer', true),
    ('merge_queue', 'foundation_audit_importer', true),
    ('deploy', 'foundation_audit_importer', false),
    ('host_command', 'foundation_audit_importer', false),
    ('repo_settings', 'foundation_audit_importer', true),
    ('identity', 'foundation_audit_importer', true);

CREATE TABLE audit.event (
    seq               bigint      PRIMARY KEY CHECK (seq >= 1),
    event_id          uuid        NOT NULL UNIQUE,
    occurred_at       timestamptz NOT NULL,
    recorded_at       timestamptz NOT NULL,
    source            text        NOT NULL CHECK (source IN ('dawneer', 'audit', 'github', 'merge_queue', 'deploy',
                                                           'host_command', 'repo_settings', 'identity')),
    source_event_id   text        NOT NULL CHECK (length(source_event_id) BETWEEN 1 AND 200),
    action            text        NOT NULL CHECK (action ~ '^[a-z_]+\.[a-z_]+$'),
    operation         text        NOT NULL CHECK (operation IN ('create', 'modify', 'remove', 'execute', 'access')),
    actor_kind        text        NOT NULL CHECK (actor_kind IN ('staff', 'service', 'system', 'unlinked')),
    actor_staff_id    uuid,
    actor_subject     text        CHECK (actor_subject IS NULL OR length(actor_subject) BETWEEN 1 AND 200),
    actor_external    text        CHECK (actor_external ~ '^(github:[0-9]+|unix:[0-9]+@[a-z0-9-]+|github-app:[0-9]+)$'),
    actor_verified    boolean     NOT NULL,
    on_behalf_of      text        CHECK (on_behalf_of IS NULL OR on_behalf_of ~ '^github-app:[0-9]+$'),
    object_type       text        NOT NULL CHECK (object_type ~ '^[a-z_]+$'),
    object_id         text        NOT NULL CHECK (length(object_id) BETWEEN 1 AND 300),
    before            jsonb       CHECK (before IS NULL OR jsonb_typeof(before) = 'object'),
    after             jsonb       CHECK (after IS NULL OR jsonb_typeof(after) = 'object'),
    reason            text        CHECK (reason IS NULL OR (length(btrim(reason)) BETWEEN 1 AND 300 AND reason !~ '[[:cntrl:]]')),
    applied_via_kind  text        CHECK (applied_via_kind IN ('pull_request', 'approval', 'host_command', 'deploy', 'api')),
    applied_via_ref   text        CHECK (applied_via_ref IS NULL OR length(applied_via_ref) BETWEEN 1 AND 200),
    result            text        NOT NULL CHECK (result IN ('requested', 'succeeded', 'failed', 'refused', 'expired')),
    error             text        CHECK (error IS NULL OR length(error) BETWEEN 1 AND 1000),
    change_id         uuid,
    prev_hash         bytea       NOT NULL CHECK (length(prev_hash) = 32),
    row_hash          bytea       NOT NULL UNIQUE CHECK (length(row_hash) = 32),
    UNIQUE (source, source_event_id),
    -- A staff actor names the staff account; nothing else does.
    CONSTRAINT event_staff_actor_names_the_account CHECK ((actor_kind = 'staff') = (actor_staff_id IS NOT NULL)),
    CONSTRAINT event_via_is_whole CHECK ((applied_via_kind IS NULL) = (applied_via_ref IS NULL))
);

CREATE INDEX event_actor_idx ON audit.event (actor_staff_id, seq DESC) WHERE actor_staff_id IS NOT NULL;
CREATE INDEX event_object_idx ON audit.event (object_type, object_id, seq DESC);
CREATE INDEX event_change_idx ON audit.event (change_id) WHERE change_id IS NOT NULL;
CREATE INDEX event_time_idx ON audit.event (occurred_at);

-- The hourly digest of a range of the chain and its public witness (§4). Written by a later stage.
CREATE TABLE audit.digest (
    to_seq                 bigint      PRIMARY KEY,
    from_seq               bigint      NOT NULL CHECK (from_seq >= 1),
    count                  bigint      NOT NULL CHECK (count = to_seq - from_seq + 1),
    head_hash              bytea       NOT NULL CHECK (length(head_hash) = 32),
    previous_digest_sha256 bytea       CHECK (previous_digest_sha256 IS NULL OR length(previous_digest_sha256) = 32),
    digest_sha256          bytea       NOT NULL UNIQUE CHECK (length(digest_sha256) = 32),
    rekor_log_index        bigint,
    rekor_inclusion_proof  jsonb,
    made_at                timestamptz NOT NULL DEFAULT now(),
    CHECK (from_seq <= to_seq)
);

-- Where each importer has read its source to. Rows are appended; the cursor now is a source's last row.
CREATE TABLE audit.import_cursor (
    source      text        NOT NULL,
    cursor      text        NOT NULL CHECK (length(cursor) BETWEEN 1 AND 500),
    state       text        NOT NULL CHECK (state IN ('running', 'waiting_for_credential', 'gap')),
    advanced_at timestamptz NOT NULL,
    PRIMARY KEY (source, advanced_at)
);

CREATE FUNCTION audit.refuse_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: no row is rewritten or removed (root ADR-0569)', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$;

-- Statement-level, so a rewrite is refused even when it matches no row: an
-- `UPDATE`/`DELETE` that "succeeds" on an empty table is still an attempt.
CREATE TRIGGER event_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.event
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();
CREATE TRIGGER digest_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.digest
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();
CREATE TRIGGER import_cursor_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.import_cursor
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();
CREATE TRIGGER source_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON audit.source
    FOR EACH STATEMENT EXECUTE FUNCTION audit.refuse_change();

-- The one rendering of a timestamp in the chain: UTC, microseconds, `Z`.
CREATE FUNCTION audit.canon_time(value timestamptz) RETURNS text
LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
    SELECT to_char(value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
$$;

-- The bytes a row's hash covers: every column but `seq` and `row_hash`, as one jsonb object's text.
-- Postgres prints equal jsonb values as equal text (keys sorted, one spacing), so the same row is
-- the same bytes on every host and release. Computed here only; no other language copies it, and
-- the fixed vector in catalog-infrastructure/tests/audit_event_chain.rs pins it.
CREATE FUNCTION audit.canon(e audit.event) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT jsonb_build_object(
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
        'change_id', e.change_id,
        'prev_hash', encode(e.prev_hash, 'hex')
    )::text
$$;

CREATE FUNCTION audit.row_hash(e audit.event) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT sha256(e.prev_hash || convert_to(audit.canon(e), 'UTF8'))
$$;

-- Chains one event onto the last. Not callable by any application role: `audit.append` (which checks
-- the caller's source) and the operator ledger's trigger (whose INSERT grant is the check) call it.
-- Returns the new `seq`, or NULL when (source, source_event_id) is already in the chain: an importer
-- that reads a source twice writes it once, and spends no number.
CREATE FUNCTION audit.chain(
    p_event_id uuid, p_occurred_at timestamptz, p_source text, p_source_event_id text,
    p_action text, p_operation text, p_actor_kind text, p_actor_staff_id uuid, p_actor_subject text,
    p_actor_external text, p_actor_verified boolean, p_on_behalf_of text, p_object_type text,
    p_object_id text, p_before jsonb, p_after jsonb, p_reason text, p_applied_via_kind text,
    p_applied_via_ref text, p_result text, p_error text, p_change_id uuid
) RETURNS bigint
LANGUAGE plpgsql SET search_path = audit, pg_temp AS $$
DECLARE
    last audit.event;
    e audit.event;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'audit events are appended under READ COMMITTED, not % (root ADR-0569 §3)',
            current_setting('transaction_isolation')
            USING ERRCODE = 'invalid_transaction_state';
    END IF;
    -- One writer at a time, until this transaction ends. 569 is the ADR's number.
    PERFORM pg_advisory_xact_lock(569, 1);
    IF EXISTS (SELECT 1 FROM audit.event WHERE source = p_source AND source_event_id = p_source_event_id) THEN
        RETURN NULL;
    END IF;
    SELECT * INTO last FROM audit.event ORDER BY seq DESC LIMIT 1;
    e.seq := coalesce(last.seq, 0) + 1;
    e.prev_hash := coalesce(last.row_hash, decode(repeat('00', 32), 'hex'));
    e.event_id := p_event_id;
    e.occurred_at := p_occurred_at;
    e.recorded_at := clock_timestamp();
    e.source := p_source;
    e.source_event_id := p_source_event_id;
    e.action := p_action;
    e.operation := p_operation;
    e.actor_kind := p_actor_kind;
    e.actor_staff_id := p_actor_staff_id;
    e.actor_subject := p_actor_subject;
    e.actor_external := p_actor_external;
    e.actor_verified := p_actor_verified;
    e.on_behalf_of := p_on_behalf_of;
    e.object_type := p_object_type;
    e.object_id := p_object_id;
    e.before := p_before;
    e.after := p_after;
    e.reason := p_reason;
    e.applied_via_kind := p_applied_via_kind;
    e.applied_via_ref := p_applied_via_ref;
    e.result := p_result;
    e.error := p_error;
    e.change_id := p_change_id;
    e.row_hash := audit.row_hash(e);
    INSERT INTO audit.event SELECT (e).*;
    RETURN e.seq;
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
    RETURN audit.chain(
        p_event_id, p_occurred_at, p_source, p_source_event_id, p_action, p_operation, p_actor_kind,
        p_actor_staff_id, p_actor_subject, p_actor_external, p_actor_verified AND verifies, p_on_behalf_of,
        p_object_type, p_object_id, p_before, p_after, p_reason, p_applied_via_kind, p_applied_via_ref,
        p_result, p_error, p_change_id);
END;
$$;

-- Moves an importer's cursor, in the transaction that wrote the source's rows.
CREATE FUNCTION audit.advance_cursor(p_source text, p_cursor text, p_state text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, pg_temp AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM audit.source WHERE source = p_source AND writer_role = session_user) THEN
        RAISE EXCEPTION 'role % may not move the cursor of audit source % (root ADR-0569 §6)', session_user, p_source
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    INSERT INTO audit.import_cursor (source, cursor, state, advanced_at)
    VALUES (p_source, p_cursor, p_state, clock_timestamp());
END;
$$;

-- Functions are executable by PUBLIC unless revoked; the grants file gives the two doors to the two roles.
REVOKE ALL ON FUNCTION audit.chain(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION audit.append(uuid, timestamptz, text, text, text, text, text, uuid, text, text, boolean,
    text, text, text, jsonb, jsonb, text, text, text, text, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION audit.advance_cursor(text, text, text) FROM PUBLIC;

-- The operator ledger names its actor by the staff account id (`StaffId`, a UUID). Checked for new
-- rows only: the ledger held 0 rows on 2026-11-19, and an old row is never rewritten.
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_actor_is_a_staff_account
    CHECK (actor ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') NOT VALID;

-- What an operator asked, as the audit event's `after`: only the arguments the action has.
CREATE FUNCTION catalog.operator_action_arguments(a catalog.operator_action) RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_strip_nulls(jsonb_build_object(
        'drop_pending', CASE WHEN a.drop_pending THEN true END,
        'expires_at', audit.canon_time(a.expires_at),
        'window_start', a.window_start,
        'window_end', a.window_end,
        'edition', a.edition,
        'lanes', a.lanes,
        'generation', a.generation))
$$;

-- The ledger's request and its answer become audit events in the ledger's own transaction. SECURITY
-- DEFINER so it may call `audit.chain`; the caller's INSERT or UPDATE grant on the ledger is the check.
CREATE FUNCTION catalog.operator_action_is_audited() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM audit.chain(
            gen_random_uuid(), NEW.requested_at, 'dawneer', 'operator_action:' || NEW.id || ':requested',
            'operator_action.' || NEW.action, 'execute', 'staff', NEW.actor::uuid, NULL, NULL, true, NULL,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN 'lane' ELSE 'job' END,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN coalesce(NEW.lanes, NEW.job_id) ELSE NEW.job_id END,
            NULL, catalog.operator_action_arguments(NEW), NEW.reason, 'api', NEW.id::text, 'requested', NULL, NEW.id);
    ELSIF OLD.consumed_at IS NULL AND NEW.consumed_at IS NOT NULL THEN
        PERFORM audit.chain(
            gen_random_uuid(), NEW.consumed_at, 'dawneer', 'operator_action:' || NEW.id || ':answered',
            'operator_action.' || NEW.action, 'execute', 'system', NULL, NULL, NULL, false, NULL,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN 'lane' ELSE 'job' END,
            CASE WHEN NEW.job_id = 'by_pnu_lane_cutover' THEN coalesce(NEW.lanes, NEW.job_id) ELSE NEW.job_id END,
            NULL, jsonb_build_object('outcome', NEW.outcome), NULL, 'api', NEW.id::text, 'succeeded', NULL, NEW.id);
    END IF;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION catalog.operator_action_is_audited() FROM PUBLIC;

CREATE TRIGGER operator_action_is_audited
AFTER INSERT OR UPDATE ON catalog.operator_action
FOR EACH ROW EXECUTE FUNCTION catalog.operator_action_is_audited();
