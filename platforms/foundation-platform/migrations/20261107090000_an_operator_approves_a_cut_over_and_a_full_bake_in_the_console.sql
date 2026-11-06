-- An operator approves a by-PNU lane cut-over and a lane's full bake in the console, recorded like
-- every other switch (root ADR-0433).
--
-- On 2026-11-06 the parcel-ownership cut-over was started by hand over `ssh -tt ... sudo
-- by-pnu-pack-operator.sh <lanes> cutover <generation>`: a mistyped sudo password, a 45-minute wait
-- and a missed operator window. A full bake was a line a root shell wrote into the bake's environment
-- file and had to remember to delete. Both are now approvals of a staff identity with a reason:
--
-- - `approve_cutover` names the lanes (one lane, `lanes`, or lane,lane,... in order) and the pack
--   generation. Its subject is the cut-over runner, `by_pnu_lane_cutover`, not a scheduled job. The
--   host starts it once, now when the operator window has room for its first step, otherwise when the
--   window next opens. It expires two days after it was asked: the next window always opens within one.
-- - `force_full` names the one lane the next run of its bake re-bases with a full bake (the
--   FOUNDATION_BY_PNU_BAKE_FORCE_FULL of the bake script, root ADR-0141), asked of the job that bakes
--   it. It expires within a week; the bake uses it once.

ALTER TABLE catalog.operator_action
    ADD COLUMN lanes text CHECK (lanes ~ '^[a-z0-9-]+(,[a-z0-9-]+)*$'),
    ADD COLUMN generation integer CHECK (generation BETWEEN 1 AND 9999);

ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_action_check;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_action_check
    CHECK (action IN ('pause', 'resume', 'run_now', 'stop', 'set_window', 'clear_window', 'promote_edition',
                      'approve_cutover', 'force_full'));

-- A run-now, a window override, a promotion request, a cut-over approval and a full bake expire,
-- after they were asked.
ALTER TABLE catalog.operator_action DROP CONSTRAINT operator_action_expiring_actions_expire;
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_expiring_actions_expire
    CHECK ((action IN ('run_now', 'set_window', 'promote_edition', 'approve_cutover', 'force_full'))
               = (expires_at IS NOT NULL)
           AND (expires_at IS NULL OR expires_at > requested_at)
           AND (action <> 'set_window' OR expires_at <= requested_at + interval '7 days')
           AND (action <> 'promote_edition' OR expires_at <= requested_at + interval '2 days')
           AND (action <> 'approve_cutover' OR expires_at <= requested_at + interval '2 days')
           AND (action <> 'force_full' OR expires_at <= requested_at + interval '7 days'));

-- A cut-over approval names its lanes and generation and is the cut-over runner's; a full bake names
-- one lane of a by-PNU bake job; nothing else names a lane or a generation.
ALTER TABLE catalog.operator_action ADD CONSTRAINT operator_action_lanes_are_a_release
    CHECK ((action IN ('approve_cutover', 'force_full')) = (lanes IS NOT NULL)
           AND (action = 'approve_cutover') = (generation IS NOT NULL)
           AND (action <> 'approve_cutover' OR job_id = 'by_pnu_lane_cutover')
           AND (job_id <> 'by_pnu_lane_cutover' OR action IN ('approve_cutover', 'stop'))
           AND (action <> 'force_full' OR (job_id ~ '^by_pnu_[a-z0-9_]+_bake$' AND lanes !~ ',')));

-- The lanes and the generation are part of what was asked: they are never rewritten either.
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
        NEW.window_start, NEW.window_end, NEW.edition, NEW.lanes, NEW.generation)
        IS DISTINCT FROM
       (OLD.id, OLD.job_id, OLD.action, OLD.drop_pending, OLD.actor, OLD.reason, OLD.requested_at, OLD.expires_at,
        OLD.window_start, OLD.window_end, OLD.edition, OLD.lanes, OLD.generation) THEN
        RAISE EXCEPTION 'catalog.operator_action %: only consumed_at and outcome are filled, once (root ADR-0282)', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
