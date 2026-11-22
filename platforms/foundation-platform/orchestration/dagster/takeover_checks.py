"""The takeover verdict on a shadow `_versions` table (root ADR-0564 §6-3, the 2026-11-22 revisions).

A lane's shadow `gold.<x>_versions` carries the check `takeover_equal` (its definition's
`asset_checks`, with the 시도 it compares, default 41). The verdict is not computed here: it needs the
lakehouse keys and pool of the build, so the shadow build's own unit (scripts/ops/gold-versions-build.sh)
runs it right after each shadow commit, in that unit's environment, and reports it through its journal
as a Pipes `report_asset_check` line; the op that ran the unit logs it as this check's evaluation
(run_jobs.py, systemd_pipes.SystemdRun.check_evaluations). One mechanism for every lane: a lane adds
the declaration, nothing else.

The check blocks (severity ERROR): a lane's takeover PR cites the run whose evaluation passed. Run on
its own (Dagster's UI), the check re-states the last evaluation the build reported, with the run that
reported it; it never judges by itself.
"""

from dagster import AssetCheckKey, AssetCheckResult

from asset_checks import Kind

TAKEOVER_EQUAL = "takeover_equal"
# The 시도 a verdict compares when its declaration names none (the largest, 경기도).
DEFAULT_SIDO = "41"


def restate(context, check):
    """The last evaluation the shadow build reported, re-stated; a failure when there is none yet."""
    key = AssetCheckKey(check.key(), check.check)
    record = context.instance.event_log_storage.get_latest_asset_check_execution_by_key([key]).get(key)
    entry = record.event if record is not None else None
    evaluation = entry.dagster_event.event_specific_data if entry is not None and entry.dagster_event else None
    if evaluation is None or not hasattr(evaluation, "passed"):
        return AssetCheckResult(passed=False, severity=check.severity,
                                metadata={"reason": "no shadow build has reported a takeover verdict yet; "
                                                    "the next shadow commit reports one"})
    metadata = {name: value.value for name, value in (evaluation.metadata or {}).items()}
    return AssetCheckResult(passed=evaluation.passed, severity=check.severity,
                            metadata={**metadata, "restated_from_run": entry.run_id})


KINDS = {
    TAKEOVER_EQUAL: Kind(
        description=("Are the shadow versions table's current rows the replaced table's rows of the same input "
                     "pins, in the declared 시도, with the whole table's aggregates equal? Reported by the "
                     "shadow build's unit after each shadow commit (gold-versions-build.sh)."),
        evaluate=restate,
    ),
}
# The checks a job's unit reports itself (the op logs them; run_jobs.py).
REPORTED = frozenset(KINDS)
