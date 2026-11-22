"""The takeover verdict on a shadow `_versions` table (root ADR-0564 §6-3, the 2026-11-22 revisions).

A lane's shadow `gold.<x>_versions` carries the check `takeover_equal` (its definition's
`asset_checks`, with the 시도 it compares). The verdict is not computed here: it needs the lakehouse
keys and pool of the build, so the shadow build's own unit (scripts/ops/gold-versions-build.sh) runs
it after a shadow commit, and again on each later run until one verdict has run to its end, in that
unit's environment, and reports it through its journal as a Pipes `report_asset_check` line; the op
that ran the unit logs it as this check's evaluation, at the severity the declaration's `blocking`
gives (run_jobs.py, systemd_pipes.SystemdRun.check_evaluations). One mechanism for every lane: a lane
adds the declaration, nothing else.

The evaluation holds nothing back inside Dagster: it is logged by an op beside the materialization it
judges, not by a step downstream of it. The gate is the takeover: the lane's takeover PR cites the run
whose evaluation passed for the current shadow head, and the switch bakes only the judged build pair.
Run on its own (Dagster's UI), the check re-states the newest evaluation a build reported, with that
build's run; it never judges by itself.
"""

from dagster import AssetCheckKey, AssetCheckResult
from dagster._core.storage.asset_check_execution_record import COMPLETED_ASSET_CHECK_EXECUTION_RECORD_STATUSES

from asset_checks import Kind

TAKEOVER_EQUAL = "takeover_equal"
RESTATED_FROM = "restated_from_run"
# How far back a re-statement looks for the build's own evaluation.
HISTORY = 50


def reported_evaluation(instance, key, current_run_id):
    """(run id, evaluation) of the newest completed evaluation a build reported (not a re-statement,
    not this run's own planned one), or None."""
    for record in instance.event_log_storage.get_asset_check_execution_history(
            key, limit=HISTORY, status=COMPLETED_ASSET_CHECK_EXECUTION_RECORD_STATUSES):
        entry = record.event
        evaluation = entry.dagster_event.event_specific_data if entry is not None and entry.dagster_event else None
        if record.run_id == current_run_id or evaluation is None or RESTATED_FROM in (evaluation.metadata or {}):
            continue
        return record.run_id, evaluation
    return None


def restate(context, check):
    """The newest evaluation a shadow build reported, re-stated; a failure when there is none yet."""
    found = reported_evaluation(context.instance, AssetCheckKey(check.key(), check.check), context.run_id)
    if found is None:
        return AssetCheckResult(passed=False, severity=check.severity,
                                metadata={"reason": "no shadow build has reported a takeover verdict yet; "
                                                    "the shadow build's next run reports one",
                                          RESTATED_FROM: ""})
    run_id, evaluation = found
    metadata = {name: value.value for name, value in (evaluation.metadata or {}).items()}
    return AssetCheckResult(passed=evaluation.passed, severity=check.severity,
                            metadata={**metadata, RESTATED_FROM: run_id})


KINDS = {
    TAKEOVER_EQUAL: Kind(
        description=("Are the shadow versions table's current rows the replaced table's rows of the same input "
                     "pins, in the declared 시도, with the whole table's aggregates equal? Reported by the "
                     "shadow build's unit (gold-versions-build.sh); run alone, it re-states the newest report."),
        evaluate=restate,
    ),
}
# The checks a job's unit reports itself (the op logs them; run_jobs.py).
REPORTED = frozenset(KINDS)
