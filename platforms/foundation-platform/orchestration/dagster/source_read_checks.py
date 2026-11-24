"""A source whose provider drops what has ended is held to its reads by Dagster's own freshness check
(root ADR-0614).

The endpoint catalog says which providers keep no history (`provider_retention.keeps_history: false`),
and each collect job that reads such a source declares `expected_outcome.landed_within_hours` in its
definition (orchestration/tests/test_job_outcomes.py derives the jobs from the catalog and refuses a
missing declaration). Here that bound is a freshness check, nothing home-made:

- one external asset per such job, `source_read/<job id>`: "this collector read its source to the end";
- the op that runs the job logs a materialization of it when the run ended `changed` or `unchanged`
  (run_jobs.py): a board with nothing new was still read, a run that failed, waited or was skipped was not;
- `build_last_update_freshness_checks` (dagster 1.13, docs "Data freshness checks", external assets)
  fails the check, severity ERROR, once the newest of those is older than the bound;
- a schedule evaluates every check twice an hour. Dagster's freshness sensor does not evaluate a
  check again after it failed (`build_sensor_for_freshness_checks`: "we will not execute if the
  freshness check has been executed before and the previous result was a failure"), so a second lost
  night would add nothing to the first; the docs' own external-asset example uses a schedule.

The check is there whether or not the job is enabled and reads Dagster's event log, which switching a
job off and on does not reset: 2026-10-21..11-05 the court auction collector was switched off for 16
nights and, being off, was judged by nothing, and the site had dropped those nights' ended listings
by the time it ran again. Nothing here names a host, a unit or a file: the check moves with Dagster.
"""

import datetime

import observe_assets

job_specs = observe_assets.job_specs

GROUP = "source_reads"
KEY_PREFIX = "source_read"
CHECK_JOB = "foundation_source_read_freshness"
EVERY = "5,35 * * * *"
# The outcomes of a run that read its source to the end (job_outcomes.DONE).
READ = ("changed", "unchanged")


def bounds(jobs):
    """job id -> the hours within which it must have read its source, for every job that declares them."""
    return {job["id"]: hours for job in jobs["jobs"]
            if (hours := (job.get("expected_outcome") or {}).get("landed_within_hours"))}


def read_key(job_id):
    from dagster import AssetKey

    return AssetKey([KEY_PREFIX, job_id])


def assets(jobs):
    """One external asset per collector held to its reads."""
    from dagster import AssetSpec

    return [AssetSpec(read_key(job_id), group_name=GROUP, kinds={"source"},
                      description=f"{job_id} read its source to the end (root ADR-0614)",
                      metadata={"job": job_id, "landed_within_hours": hours})
            for job_id, hours in sorted(bounds(jobs).items())]


def read(job_id, outcome):
    """The materialization a run of `job_id` that ended `outcome` logs, or None when it did not read
    its source to the end."""
    from dagster import AssetMaterialization

    if outcome not in READ:
        return None
    return AssetMaterialization(read_key(job_id), metadata={"outcome": outcome, "job": job_id})


def checks(jobs):
    """Dagster's last-update freshness check of each collector's read, at its declared bound."""
    from dagster import AssetCheckSeverity, build_last_update_freshness_checks

    found = []
    for job_id, hours in sorted(bounds(jobs).items()):
        found.extend(build_last_update_freshness_checks(
            assets=[read_key(job_id)], lower_bound_delta=datetime.timedelta(hours=hours),
            severity=AssetCheckSeverity.ERROR))
    return found


def schedule(freshness_checks):
    """(job, schedule) evaluating every freshness check twice an hour; (None, None) when there is none."""
    from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition, define_asset_job

    if not freshness_checks:
        return None, None
    job = define_asset_job(CHECK_JOB, selection=AssetSelection.checks(*freshness_checks),
                           description="Evaluates the source read freshness checks (root ADR-0614).")
    return job, ScheduleDefinition(job=job, cron_schedule=EVERY, default_status=DefaultScheduleStatus.RUNNING)
