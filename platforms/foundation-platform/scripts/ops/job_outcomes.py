"""What each job is expected to produce, and whether it did (root ADR-0480).

A unit that succeeded is not a job that did its work. Each enabled job of orchestration/jobs.v1.json
is held to two outcomes, read from the outcome ledger the scheduler's runner appends to
(scripts/deploy/job_units.py OUTCOMES, one line per run, check answer or start it did not make):

- `done`, every job: a run or check that ended `changed` or `unchanged` within one schedule period
  plus its time and wait limits and an hour (or the job's `expected_outcome.done_within_hours`). A job
  that only waits, is skipped, is refused its pool, fails, or is never started misses it. A job the
  asset reconciler reconciles is also done while the reconciler finds every output of it as new as its
  inputs (`current`): nothing was owed, so no run had to happen, and an owed output the job does not
  write is the reconciler's own alert (FoundationAsset*, root ADR-0333). 2026-11-09..10 the by-PNU
  bake's clock starts were deferred by turn-taking and the kind Gold rebuild's waited for the panel
  rebuild while their outputs stayed current, and both read as overdue.
- `change`, a job whose `expected_outcome.changes_within_hours` is a number: a run that ended
  `changed` within that many hours. A job that runs and writes nothing misses it.

A job with no line yet is measured from when it was switched on or from the ledger's first line,
whichever is later, so a deploy does not report the time before the ledger existed. The asset
reconciler writes the result as series (job-outcomes.prom, the thresholds as series of their own,
Robust Perception's "using time series as alert thresholds"); the foundation-job-outcomes rules
compare them, and the daily readiness report shows the same verdicts as its job rows' `outcome`.
"""

from __future__ import annotations

import re

# Outcomes that end a run's work (root ADR-0260 §1): it did what it had to, writing or not.
DONE = ("changed", "unchanged")
# The outcomes that end a run, kept with the code version a failed one carries (`ran`, root ADR-0499).
ENDED = ("changed", "unchanged", "failed")
# Not a run's outcome: the asset reconciler's verdict that a job it reconciles owes nothing (fold_current).
CURRENT = "current"
# A scheduler's lag on top of a job's own limits: Airflow's queue, a deploy's paused DAGs.
GRACE_SECONDS = 3600
EXPECTATIONS = ("done", "change")
METRICS_NAME = "job-outcomes.prom"
SUMMARY_FILE = "job-outcomes.json"
HELP = """# HELP foundation_job_outcome_met_seconds When an enabled job last met an expected outcome (root ADR-0480); a job that never did is measured from when it was switched on or the ledger began.
# TYPE foundation_job_outcome_met_seconds gauge
# HELP foundation_job_outcome_expected_seconds How long an enabled job may go without meeting an expected outcome (jobs.v1.json expected_outcome, root ADR-0480).
# TYPE foundation_job_outcome_expected_seconds gauge
# HELP foundation_job_outcome_info The unit, owner and runbook of an enabled job, from its unit file (root ADR-0445).
# TYPE foundation_job_outcome_info gauge
# HELP foundation_job_outcomes_last_recorded_seconds When the scheduler's runner last recorded an outcome of any job.
# TYPE foundation_job_outcomes_last_recorded_seconds gauge
# HELP foundation_job_outcomes_recorded The outcome lines read from the ledger so far.
# TYPE foundation_job_outcomes_recorded gauge
"""
RELEASE_PREFIX = "file:///opt/foundation-platform/current/"


def cron_period_seconds(schedule: str) -> int | None:
    """The longest gap between fires of a `<minutes> <hour|*> * * *` schedule, or None for any other form."""
    fields = schedule.split()
    if len(fields) != 5 or fields[2:] != ["*", "*", "*"]:
        return None
    try:
        minutes = [int(m) for m in fields[0].split(",")]
        hours = list(range(24)) if fields[1] == "*" else [int(h) for h in fields[1].split(",")]
    except ValueError:
        return None
    fires = sorted(h * 60 + m for h in hours for m in minutes)
    gaps = [b - a for a, b in zip(fires, fires[1:])] + [fires[0] + 1440 - fires[-1]]
    return max(gaps) * 60


def done_budget_seconds(job: dict) -> int:
    """How long `job` may go without a `changed` or `unchanged` run: one schedule period, its time and
    wait limits and GRACE_SECONDS, or its declared done_within_hours when that is longer."""
    period = cron_period_seconds(job["schedule"])
    if period is None:
        raise ValueError(f"{job['id']}: schedule {job['schedule']!r} is not `<minutes> <hour|*> * * *`")
    derived = period + 60 * (int(job.get("time_limit_minutes") or 0) + int(job.get("wait_limit_minutes") or 0)) + GRACE_SECONDS
    declared = (job.get("expected_outcome") or {}).get("done_within_hours")
    return max(derived, int(declared * 3600)) if declared else derived


def fold(summary: dict, records: list[dict]) -> dict:
    """`summary` ({"first_at": .., "last_at": .., "lines": .., "jobs": {job: {outcome: last at}}}) with
    `records` (job_units.outcomes_after) folded in. Only the newest time of each outcome is kept, and
    in `ran` ({job: {"outcome": .., "at": .., "code_version": ..}}) each job's newest run that ended
    (ENDED), with the code version a failed one carried."""
    jobs, ran = summary.setdefault("jobs", {}), summary.setdefault("ran", {})
    for record in records:
        at, outcome = int(record.get("at", 0)), str(record.get("outcome", ""))
        if not at or not outcome:
            continue
        summary["first_at"] = min(summary.get("first_at") or at, at)
        summary["last_at"] = max(summary.get("last_at") or at, at)
        summary["lines"] = int(summary.get("lines", 0)) + 1
        kept = jobs.setdefault(record["job"], {})
        kept[outcome] = max(int(kept.get(outcome, 0)), at)
        if record.get("via", "run") == "run" and outcome in ENDED and at >= int(ran.get(record["job"], {}).get("at", 0)):
            ran[record["job"]] = {"outcome": outcome, "at": at,
                                  **({"code_version": record["code_version"]} if record.get("code_version") else {})}
    return summary


def fold_current(summary: dict, jobs, now: float) -> dict:
    """`summary` with each of `jobs` (the reconciled jobs the asset reconciler found current at `now`)
    kept as `current` at `now`. Not a ledger line: the ledger's first and last times and count stay."""
    for job in jobs:
        summary.setdefault("jobs", {}).setdefault(job, {})[CURRENT] = int(now)
    return summary


def enabled_since(job: dict) -> float | None:
    from datetime import datetime  # noqa: PLC0415

    stamp = job.get("enabled_since")
    if not stamp:
        return None
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()


def expectations(job: dict, summary: dict) -> list[dict]:
    """[{expectation, expected_seconds, met_at, last}] for an enabled job, from the folded ledger."""
    kept = summary.get("jobs", {}).get(job["id"], {})
    baseline = max(filter(None, (enabled_since(job), summary.get("first_at"))), default=None)
    done = max((kept.get(outcome, 0) for outcome in (*DONE, CURRENT)), default=0) or None
    ran = {outcome: stamp for outcome, stamp in kept.items() if outcome != CURRENT}
    rows = [{"expectation": "done", "expected_seconds": done_budget_seconds(job),
             "met_at": done or baseline, "last": max(ran.items(), key=lambda item: item[1])[0] if ran else None}]
    hours = (job.get("expected_outcome") or {}).get("changes_within_hours")
    if hours:
        rows.append({"expectation": "change", "expected_seconds": int(hours * 3600),
                     "met_at": kept.get("changed") or baseline, "last": None})
    return rows


def overdue(jobs: list[dict], summary: dict, now: float) -> list[dict]:
    """Every expected outcome an enabled job has missed: {job, expectation, overdue_seconds, ...}."""
    found = []
    for job in jobs:
        if not job.get("enabled"):
            continue
        for row in expectations(job, summary):
            if row["met_at"] is not None and now - row["met_at"] > row["expected_seconds"]:
                found.append({"job": job["id"], **row, "overdue_seconds": int(now - row["met_at"] - row["expected_seconds"])})
    return found


def owner_and_runbook(unit_text: str) -> tuple[str, str]:
    """The `X-Foundation-Owner=` and first `Documentation=` of a unit file (root ADR-0445 §2)."""
    owner = next((m[1].strip() for m in re.finditer(r"^X-Foundation-Owner=(.*)$", unit_text, re.M)), "")
    runbook = next((m[1].split()[0] for m in re.finditer(r"^Documentation=(.*)$", unit_text, re.M) if m[1].strip()), "")
    return owner, runbook.removeprefix(RELEASE_PREFIX)


def label(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def textfile(jobs: list[dict], summary: dict, unit_text) -> str:
    """job-outcomes.prom: each enabled job's expected outcomes as met time and allowed age, and its
    unit, owner and runbook; `unit_text(service)` reads a service's unit file."""
    lines = []
    for job in jobs:
        if not job.get("enabled"):
            continue
        try:
            owner, runbook = owner_and_runbook(unit_text(job["systemd_service"]))
        except OSError:
            owner, runbook = "", ""
        lines.append(f'foundation_job_outcome_info{{job_id="{label(job["id"])}",unit="{label(job["systemd_service"])}",'
                     f'owner="{label(owner)}",runbook="{label(runbook)}"}} 1')
        for row in expectations(job, summary):
            if row["met_at"] is None:
                continue  # no ledger yet and never switched on: nothing to measure from
            labels = f'job_id="{label(job["id"])}",expectation="{row["expectation"]}"'
            lines.append(f"foundation_job_outcome_met_seconds{{{labels}}} {int(row['met_at'])}")
            lines.append(f"foundation_job_outcome_expected_seconds{{{labels}}} {row['expected_seconds']}")
    if summary.get("last_at"):
        lines.append(f"foundation_job_outcomes_last_recorded_seconds {int(summary['last_at'])}")
    lines.append(f"foundation_job_outcomes_recorded {int(summary.get('lines', 0))}")
    return HELP + "\n".join(lines) + "\n"
