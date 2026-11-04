-- An operator may ask to serve another cadastral edition, recorded like every other switch (root ADR-0381).
--
-- The served edition used to move by a hand edit of the source contract. It now moves only through a
-- promotion that the readiness check grants for a ready edition (root ADR-0379), and a person asks for
-- it in the console: `promote_edition`, of the job that judges readiness (parcel_edition_readiness),
-- naming the edition (YYYYMM). The request expires after the 48 hours a readiness report may be old;
-- the check consumes it by writing the promoted contract for review, and the change is served only
-- through that reviewed change. Who asked, when and why stay here, append-only, like every request.

ALTER TABLE catalog.operator_action
    ADD COLUMN edition text CHECK (edition ~ '^[0-9]{4}(0[1-9]|1[0-2])$');

ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_action_check;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_action_check
    CHECK (action IN ('pause', 'resume', 'run_now', 'stop', 'set_window', 'clear_window', 'promote_edition'));

-- A run-now, a window override and a promotion request expire, after they were asked; a window
-- override within a week, a promotion request within two days.
ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_expiring_actions_expire;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_expiring_actions_expire
    CHECK ((action IN ('run_now', 'set_window', 'promote_edition')) = (expires_at IS NOT NULL)
           AND (expires_at IS NULL OR expires_at > requested_at)
           AND (action <> 'set_window' OR expires_at <= requested_at + interval '7 days')
           AND (action <> 'promote_edition' OR expires_at <= requested_at + interval '2 days'));

-- Only a promotion names an edition, and it must; it is asked of the readiness check only.
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_edition_is_a_promotion
    CHECK ((action = 'promote_edition') = (edition IS NOT NULL)
           AND (action <> 'promote_edition' OR job_id = 'parcel_edition_readiness'));

-- The edition is part of what was asked: it is never rewritten either.
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
        NEW.window_start, NEW.window_end, NEW.edition)
        IS DISTINCT FROM
       (OLD.id, OLD.job_id, OLD.action, OLD.drop_pending, OLD.actor, OLD.reason, OLD.requested_at, OLD.expires_at,
        OLD.window_start, OLD.window_end, OLD.edition) THEN
        RAISE EXCEPTION 'catalog.operator_action %: only consumed_at and outcome are filled, once (root ADR-0282)', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
