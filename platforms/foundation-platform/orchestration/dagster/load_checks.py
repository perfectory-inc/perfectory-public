"""The result checks a Silver load from Bronze reports on every run (root ADR-0559).

A Silver table loaded from Bronze switches engines directly (Bronze is kept and Silver is append-only,
so a wrong load is fixed by loading again from Bronze): the guard is not a comparison with the old
engine but these checks of every load. The load's own unit computes them on the rows it appends of each
object and reports them through its journal as Pipes `report_asset_check` lines (the court auction
DuckDB load, services/foundation-outbox-publisher/src/courtauction_silver_load.rs); the op that ran the
unit logs them at the severity the declaration's `blocking` gives (run_jobs.py,
systemd_pipes.SystemdRun.check_evaluations). Run on its own, a check re-states the newest report.
"""

from dagster import AssetCheckKey, AssetCheckResult

from asset_checks import Kind
from takeover_checks import RESTATED_FROM, reported_evaluation

ROWS = "silver_load_rows"
KEY_UNIQUE = "silver_load_key_unique"
REQUIRED_PRESENT = "silver_load_required_present"


def restate(context, check):
    """The newest evaluation a load reported, re-stated; a failure when there is none yet."""
    found = reported_evaluation(context.instance, AssetCheckKey(check.key(), check.check), context.run_id)
    if found is None:
        return AssetCheckResult(passed=False, severity=check.severity,
                                metadata={"reason": "no load has reported this check yet; the next load reports it",
                                          RESTATED_FROM: ""})
    run_id, evaluation = found
    metadata = {name: value.value for name, value in (evaluation.metadata or {}).items()}
    return AssetCheckResult(passed=evaluation.passed, severity=check.severity,
                            metadata={**metadata, RESTATED_FROM: run_id})


KINDS = {
    ROWS: Kind(
        description=("Did every Bronze object the run loaded commit exactly the rows the load made of it? "
                     "Reported by the load's unit; run alone, it re-states the newest report."),
        evaluate=restate,
    ),
    KEY_UNIQUE: Kind(
        description=("Do the rows each object appended repeat no key of the table's `(…) unique` quality gate? "
                     "Reported by the load's unit; run alone, it re-states the newest report."),
        evaluate=restate,
    ),
    REQUIRED_PRESENT: Kind(
        description=("Do the rows each object appended fill every required column of the table's contract? "
                     "Reported by the load's unit; run alone, it re-states the newest report."),
        evaluate=restate,
    ),
}
# The checks a job's unit reports itself (the op logs them; run_jobs.py).
REPORTED = frozenset(KINDS)
