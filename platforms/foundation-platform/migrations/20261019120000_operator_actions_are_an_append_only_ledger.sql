-- Every operator switch of a scheduled job is one row, and a row is never rewritten (root ADR-0282 §2).
--
-- foundation-api is the only writer: `pause`, `resume`, `run_now` and `stop` arrive under a staff
-- identity with a reason. A `run_now` request is single use and expires ten minutes after it was
-- asked (§4); the gate that starts the run fills `consumed_at` and `outcome` once. Nothing else of a
-- row ever changes, and no row is deleted, so the ledger answers who switched what, when and why.

CREATE TABLE catalog.operator_action (
    id uuid PRIMARY KEY,
    job_id text NOT NULL CHECK (job_id ~ '^[a-z][a-z0-9_]*$'),
    action text NOT NULL CHECK (action IN ('pause', 'resume', 'run_now', 'stop')),
    drop_pending boolean NOT NULL DEFAULT false,
    actor text NOT NULL CHECK (length(btrim(actor)) > 0),
    reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 300 AND reason !~ '[[:cntrl:]]'),
    requested_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    consumed_at timestamptz,
    outcome text CHECK (outcome IS NULL OR length(btrim(outcome)) > 0),
    -- Only a pause drops what is pending.
    CONSTRAINT operator_action_drop_pending_is_a_pause CHECK (NOT drop_pending OR action = 'pause'),
    -- Only a run-now request expires, and it expires after it was asked.
    CONSTRAINT operator_action_only_run_now_expires
        CHECK ((action = 'run_now') = (expires_at IS NOT NULL) AND (expires_at IS NULL OR expires_at > requested_at)),
    -- An answer is when and what, together.
    CONSTRAINT operator_action_answer_is_whole CHECK ((consumed_at IS NULL) = (outcome IS NULL))
);

CREATE INDEX operator_action_job_requested_idx ON catalog.operator_action (job_id, requested_at DESC);

CREATE FUNCTION catalog.operator_action_is_answered_once() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION 'catalog.operator_action is append-only: no row is removed (root ADR-0282)'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.consumed_at IS NOT NULL THEN
        RAISE EXCEPTION 'catalog.operator_action % was answered already (root ADR-0282)', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.id, NEW.job_id, NEW.action, NEW.drop_pending, NEW.actor, NEW.reason, NEW.requested_at, NEW.expires_at)
        IS DISTINCT FROM
       (OLD.id, OLD.job_id, OLD.action, OLD.drop_pending, OLD.actor, OLD.reason, OLD.requested_at, OLD.expires_at) THEN
        RAISE EXCEPTION 'catalog.operator_action %: only consumed_at and outcome are filled, once (root ADR-0282)', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER operator_action_append_only
BEFORE UPDATE OR DELETE ON catalog.operator_action
FOR EACH ROW EXECUTE FUNCTION catalog.operator_action_is_answered_once();

CREATE TRIGGER operator_action_never_truncated
BEFORE TRUNCATE ON catalog.operator_action
FOR EACH STATEMENT EXECUTE FUNCTION catalog.operator_action_is_answered_once();
