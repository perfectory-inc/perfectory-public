-- An operator may set or clear a job's run window, recorded like every other switch (root ADR-0282).
--
-- A job that may only run inside a window (the court auction collection's 01:00-06:00 KST, its
-- source contract's quiet hours) needed a code change and a deploy to run at another time. Now
-- `set_window` overrides the contract's window for a bounded time (`expires_at`, at most a week:
-- the contract's window always comes back) and `clear_window` ends an override early. The window is
-- two KST clock times; a start equal to its end is the whole day.

ALTER TABLE catalog.operator_action
    ADD COLUMN window_start text CHECK (window_start ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'),
    ADD COLUMN window_end text CHECK (window_end ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$');

ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_action_check;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_action_check
    CHECK (action IN ('pause', 'resume', 'run_now', 'stop', 'set_window', 'clear_window'));

-- A run-now and a window override expire, after they were asked; a window override within a week.
ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_only_run_now_expires;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_expiring_actions_expire
    CHECK ((action IN ('run_now', 'set_window')) = (expires_at IS NOT NULL)
           AND (expires_at IS NULL OR expires_at > requested_at)
           AND (action <> 'set_window' OR expires_at <= requested_at + interval '7 days'));

-- Only a window override names a window, and it names both ends.
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_window_is_a_set_window
    CHECK ((action = 'set_window') = (window_start IS NOT NULL) AND (window_start IS NULL) = (window_end IS NULL));

-- The window is part of what was asked: it is never rewritten either.
CREATE OR REPLACE FUNCTION catalog.operator_action_is_answered_once() RETURNS trigger
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
    IF (NEW.id, NEW.job_id, NEW.action, NEW.drop_pending, NEW.actor, NEW.reason, NEW.requested_at, NEW.expires_at,
        NEW.window_start, NEW.window_end)
        IS DISTINCT FROM
       (OLD.id, OLD.job_id, OLD.action, OLD.drop_pending, OLD.actor, OLD.reason, OLD.requested_at, OLD.expires_at,
        OLD.window_start, OLD.window_end) THEN
        RAISE EXCEPTION 'catalog.operator_action %: only consumed_at and outcome are filled, once (root ADR-0282)', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
