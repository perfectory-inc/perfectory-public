"""What the switch of a group to Dagster did, read only (root ADR-0583 §6). Run by
scripts/ops/verify-dagster-group.sh inside the Dagster code server's container:

    verify_group.py <group> <switched at, ISO 8601>   < {"serving": [...]}   (the host's Prometheus answers)

One JSON line per switched-on job of the group, then one line for the group:

- `runs` the job's Dagster runs since the switch, by status; `launch_failed` those that failed before a step
  started (the daemon could not launch them); `duplicate_keys` run keys two runs share (one decision started twice).
- `missed` the expected runs no run answered: a clock tick, or an input event (a materialization of a
  starting input), is answered by a run that was open at that moment or that was created after it, however
  late (the sensor folds what waited into the next run, decide.py: a deploy that held the job for an hour
  missed nothing). One with no run since is missed once `min(two ticks, 48 hours)` passed (§6, so a weekly
  or monthly fallback does not wait weeks): the job stopped running.
- the group line: `stop` names every job that missed two or more, and says the rollback PR (§3) opens first.
- for Gold, `serving`: each by-PNU lane and tile unit whose `behind` series (asset_reconcile.py) is now
  further behind than the same hour a week before by more than its interval (the bake's ticks, two of them).

Nothing is written: Dagster's instance is read, and the Prometheus answers come on stdin.
"""

import datetime
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / "dags")]

import dual_run  # noqa: E402
import observe_assets  # noqa: E402

job_specs = observe_assets.job_specs
DAY = 86400.0
DEADLINE_CAP = 2 * DAY
STOP_AT = 2


def deadline(spec, at):
    """How long after `at` a run may come and still answer it: two of the job's ticks, at most 48 hours."""
    if not spec.schedule:
        return DEADLINE_CAP
    # The ticks after `at`, not `at` itself (a tick is the moment it answers for).
    ticks = dual_run.schedule_ticks(spec.schedule, at + 1, at + DEADLINE_CAP + 1)
    return min(ticks[1] - at, DEADLINE_CAP) if len(ticks) >= 2 else DEADLINE_CAP


def answered(at, runs):
    """Whether a run was open at `at` or was created after it. `runs` are (created, ended)."""
    return any(created <= at <= (ended if ended is not None else float("inf")) or at < created for created, ended in runs)


def missed(expected, runs, now, limit_of):
    """The expected moments whose deadline has passed and no run answered."""
    return [at for at in expected if at + limit_of(at) <= now and not answered(at, runs)]


def behind(serving):
    """The lanes and units further behind than a week before by more than their interval. One with no series
    a week before (a lane registered since) has nothing to be compared with."""
    return [row for row in serving if row.get("now") is not None and row.get("week_before") is not None
            and row["now"] > row["week_before"] + row["interval"]]


def _job_report(instance, spec, since, now):
    from dagster import AssetRecordsFilter, RunsFilter

    after = datetime.datetime.fromtimestamp(since, datetime.UTC)
    records = instance.get_run_records(RunsFilter(job_name=spec.dagster_job, created_after=after), limit=1000)
    runs = [(record.create_timestamp.timestamp(), record.end_time) for record in records]
    statuses, keys, launch_failed = {}, {}, 0
    for record in records:
        status = record.dagster_run.status.value
        statuses[status] = statuses.get(status, 0) + 1
        key = record.dagster_run.tags.get("dagster/run_key")
        if key:
            keys[key] = keys.get(key, 0) + 1
        if status == "FAILURE" and record.start_time is None:
            launch_failed += 1
    expected = dual_run.schedule_ticks(spec.schedule, since, now) if spec.schedule else []
    if spec.started_by == "inputs":
        for dataset in spec.starting_inputs:
            found = instance.fetch_materializations(
                AssetRecordsFilter(asset_key=observe_assets.asset_key(dataset), after_timestamp=since), limit=1000)
            expected += [record.timestamp for record in found.records]
    lost = missed(sorted(expected), runs, now, lambda at: deadline(spec, at))
    return {"job": spec.job_id, "runs": statuses, "launch_failed": launch_failed,
            "duplicate_keys": sorted(key for key, count in keys.items() if count > 1),
            "missed": [datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat() for at in lost]}


def main(argv):
    from dagster import DagsterInstance

    group, since = argv[0], datetime.datetime.fromisoformat(argv[1]).timestamp()
    text = "" if sys.stdin.isatty() else sys.stdin.read()
    answers = json.loads(text) if text.strip() else {}
    jobs, graph = observe_assets.contracts()
    members = {job["id"] for job in jobs["jobs"] if job.get("dagster_group") == group and job["enabled"]}
    specs = [spec for spec in job_specs.load_specs(jobs, graph) if spec.job_id in members]
    now = datetime.datetime.now(datetime.UTC).timestamp()
    instance = DagsterInstance.get()
    reports = [_job_report(instance, spec, since, now) for spec in specs]
    for report in reports:
        print(json.dumps(report, ensure_ascii=False))
    stop = [report["job"] for report in reports if len(report["missed"]) >= STOP_AT]
    summary = {"group": group, "since": argv[1], "jobs": len(reports),
               "not_dagster": sorted(spec.job_id for spec in specs if spec.dagster_stage != "dagster"),
               "duplicate_keys": sum(len(report["duplicate_keys"]) for report in reports),
               "launch_failed": sum(report["launch_failed"] for report in reports),
               "stop": stop}
    if group == "gold":
        by_job = {spec.job_id: spec for spec in specs}
        rows = []
        for row in answers.get("serving", []):
            spec = by_job.get(row.get("job"))
            interval = deadline(spec, now - DAY) if spec is not None else DAY
            rows.append({**row, "interval": interval})
        summary["serving_behind"] = behind(rows)
    if stop:
        summary["verdict"] = "stop: open the rollback PR first, then find the cause (root ADR-0583 §3, §6)"
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if stop or summary["not_dagster"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
