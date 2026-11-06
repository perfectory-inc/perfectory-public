"""The dual run of a job moving to Dagster: both orchestrators decide, Airflow alone runs (root ADR-0431 §2).

A job at `dagster_stage: shadow` (orchestration/jobs.v1.json) is still started and run by Airflow. Dagster
reads those runs from Airflow's API (every state: a failed run was a decision too) and the
materializations of the job's inputs airlift observed (the publish tasks of the jobs that feed it), and
decides beside Airflow by the rules it will use once the job is its own:

- a `schedule` tick: Dagster's own cron iteration of the job's `schedule` in UTC (the iterator Dagster's
  schedules use), against Airflow's `scheduled` runs, whose run id carries the tick. A DAG runs one run
  at a time and catches no tick up (foundation_jobs.py `max_active_runs=1`, `catchup=False`), so ticks
  that come while a run is in progress fold into the latest of them, which runs when it ends
  (`due_ticks`; the recorded day's onbid Silver run 17:50 took 18:05-21:05, and 20:50 ran next, not
  18:50 or 19:50). Dagster's schedule keeps that rule when it takes the job (ADR-0431 §3);
- an input event: a materialization of one of the job's starting inputs (job_specs.JobSpec.starting_inputs)
  is followed by an `asset_triggered` run starting at or after it, within the job's wait and time limits;
  events that arrive while a run is pending fold into it, as Airflow's queued asset events and Dagster's
  declarative automation both do. An `asset_triggered` run with no input event since the previous one is
  a decision Dagster would not have made.

`manual` runs are an operator's and are not compared. A stretch in which Airflow started no DAG at all is
an outage (`outages`): Dagster's decisions there say how available Airflow was, not how it decides, and
are reported apart (`airflow_out`), not as differences. The `dual_run_matches_airflow` check on the
job's DAG asset runs after every Airflow run airlift observes and reports (WARN, never blocking) the
decisions that differ. Every check is generated from the job list: a job enters or leaves the dual run
by its `dagster_stage`.
"""

import datetime
import time
from dataclasses import dataclass, field

import observe_assets

job_specs = observe_assets.job_specs

CHECK_NAME = "dual_run_matches_airflow"
# The DAG asset key airlift gives a DAG (dagster_airlift make_default_dag_asset_key), under the
# code location's instance name (definitions.INSTANCE_NAME).
INSTANCE_NAME = "foundation_airflow"
# How far back a check looks: one day of decisions, the unit the move gate counts (ADR-0431 §4).
WINDOW_SECONDS = 24 * 3600
SCHEDULED_PREFIX = "scheduled__"
DAY_SECONDS = 86400.0


@dataclass(frozen=True)
class Run:
    """One finished Airflow DAG run, as airlift's DAG materialization records it."""

    run_id: str
    run_type: str
    start: float  # epoch seconds
    end: float | None = None  # None while it is queued or running


@dataclass
class Comparison:
    decisions: int = 0
    only_dagster: list = field(default_factory=list)  # what Dagster would start and Airflow did not
    only_airflow: list = field(default_factory=list)  # what Airflow started and Dagster would not
    airflow_out: list = field(default_factory=list)  # Dagster's decisions while Airflow started nothing

    @property
    def matches(self):
        return not self.only_dagster and not self.only_airflow


def tick_of(run_id):
    """The schedule tick (epoch seconds) a `scheduled` run id names, or None."""
    if not run_id.startswith(SCHEDULED_PREFIX):
        return None
    return datetime.datetime.fromisoformat(run_id[len(SCHEDULED_PREFIX):]).timestamp()


def settle_seconds(spec, job):
    """How long after a decision its run has surely finished: the job's wait and time limits."""
    return 60 * (spec.timeout_minutes + int(job.get("wait_limit_minutes", 0)) + int(job.get("stop_grace_minutes", 0)))


def schedule_ticks(schedule, start, end):
    """Dagster's ticks of `schedule` in [start, end), UTC, as its schedules iterate them."""
    from dagster._utils.schedules import cron_string_iterator

    ticks = []
    for tick in cron_string_iterator(start, schedule, "UTC"):
        if tick.timestamp() >= end:
            return ticks
        ticks.append(tick.timestamp())
    return ticks


def due_ticks(ticks, runs):
    """The ticks that start a run: a tick that comes while a run of the job is in progress folds into
    the latest tick before that run ends."""
    busy = [(run.start, DAY_SECONDS * 1e6 if run.end is None else run.end) for run in runs]
    due = []
    for index, tick in enumerate(ticks):
        ends = [end for start, end in busy if start <= tick < end]
        following = ticks[index + 1] if index + 1 < len(ticks) else None
        if not ends or following is None or following >= max(ends):
            due.append(tick)
    return due


def outage_gap_seconds(specs):
    """Twice the shortest gap between two ticks of any enabled scheduled job: no run at all for that
    long means Airflow was out."""
    gaps = []
    for spec in specs:
        if spec.enabled and spec.started_by == "schedule" and spec.schedule:
            ticks = schedule_ticks(spec.schedule, 0.0, 2 * DAY_SECONDS)
            gaps += [later - earlier for earlier, later in zip(ticks, ticks[1:])]
    return 2 * min(gaps, default=DAY_SECONDS)


def outages(run_starts, gap_seconds, start, end):
    """The stretches of [start, end) in which Airflow started no DAG run for longer than `gap_seconds`:
    a deploy that paused every DAG, or a scheduler that was down (2026-10-28 15:50-18:04 and
    19:00-20:47 on the recorded day)."""
    found, previous = [], start
    for at in [*sorted(t for t in run_starts if start <= t < end), end]:
        if at - previous > gap_seconds:
            found.append((previous, at))
        previous = at
    return found


def _inside(at, spans):
    return any(a <= at < b for a, b in spans)


def _deadline(event, settle, spans):
    """`settle` seconds of Airflow's up time after `event`: every outage in between pushes it later."""
    deadline = event + settle
    for a, b in sorted(spans):
        if a < deadline and b > event:
            deadline += b - max(a, event)
    return deadline


def compare(spec, job, runs, input_events, start, end, out=()):
    """The decisions Dagster and Airflow made for one job over [start, end) (epoch seconds).

    `runs` are the job's finished Airflow DAG runs, `input_events` the times its starting inputs were
    materialized, `out` the outages. Decisions whose runs may not have finished by `end` are not
    compared yet.
    """
    found = Comparison()
    settle = settle_seconds(spec, job)
    settled = end - settle
    if spec.schedule:
        airflow = {tick_of(run.run_id) for run in runs if run.run_type == "scheduled"} - {None}
        dagster = set(due_ticks(schedule_ticks(spec.schedule, start, settled), runs))
        found.decisions += len(dagster)
        for tick in sorted(dagster - airflow):
            (found.airflow_out if _inside(tick, out) else found.only_dagster).append(("schedule", tick))
        found.only_airflow += [("schedule", tick) for tick in sorted(airflow - dagster) if start <= tick < settled]
    if spec.started_by == "inputs":
        triggered = sorted(run.start for run in runs if run.run_type == "asset_triggered")
        events = sorted(input_events)
        previous = None
        for run_start in triggered:
            if start <= run_start < settled:
                found.decisions += 1
                if not any((previous is None or previous < event) and event <= run_start for event in events):
                    found.only_airflow.append(("inputs", run_start))
            previous = run_start
        for event in events:
            if start <= event < settled and not any(event <= run_start <= _deadline(event, settle, out)
                                                    for run_start in triggered):
                (found.airflow_out if _inside(event, out) else found.only_dagster).append(("inputs", event))
    return found


def shadow_jobs(jobs, graph):
    """(spec, job) of every job in the dual run, from the job list."""
    by_id = {job["id"]: job for job in jobs["jobs"]}
    return [(spec, by_id[spec.job_id]) for spec in job_specs.load_specs(jobs, graph) if spec.dagster_stage == "shadow"]


def dag_key(spec):
    from dagster import AssetKey

    return AssetKey([INSTANCE_NAME, "dag", spec.dag_id])


def _records(instance, key, after):
    from dagster import AssetRecordsFilter

    return instance.fetch_materializations(AssetRecordsFilter(asset_key=key, after_timestamp=after), limit=1000).records


# Every state a run can be seen in: a failed or still running run was a decision too (airlift's DAG
# materializations count only the successful ones, so the runs are read from Airflow itself).
RUN_STATES = ("queued", "running", "success", "failed")


def airflow_runs(airflow, dag_ids, after):
    """dag id -> its runs that started after `after` (epoch seconds), from Airflow's API, every state."""
    found, offset = {}, 0
    since = datetime.datetime.fromtimestamp(after, datetime.UTC)
    while True:
        runs, total = airflow.get_dag_runs_batch(dag_ids=list(dag_ids), start_date_gte=since, offset=offset,
                                                 states=list(RUN_STATES))
        for run in runs:
            done = run.metadata.get("state") in ("success", "failed")
            found.setdefault(run.dag_id, []).append(Run(run.run_id, run.run_type, run.start_date.timestamp(),
                                                        run.end_date.timestamp() if done else None))
        offset += len(runs)
        if not runs or offset >= total:
            return found


def result(spec, comparison):
    from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue

    def stamped(items):
        return [[kind, datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat()] for kind, at in items]

    return AssetCheckResult(
        passed=comparison.matches,
        severity=AssetCheckSeverity.WARN,
        metadata={"job": spec.job_id, "decisions": comparison.decisions,
                  "only_dagster": MetadataValue.json(stamped(comparison.only_dagster)),
                  "only_airflow": MetadataValue.json(stamped(comparison.only_airflow)),
                  "airflow_out": MetadataValue.json(stamped(comparison.airflow_out))},
    )


def checks(airflow, jobs=None, graph=None):
    """One reporting check per job in the dual run, on the job's DAG asset; `airflow` is the observed
    instance (definitions.airflow_instance), read when a check runs."""
    from dagster import asset_check

    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    specs = job_specs.load_specs(jobs, graph)
    every_dag = [spec.dag_id for spec in specs]
    gap = outage_gap_seconds(specs)
    return [asset_check(asset=dag_key(spec), name=CHECK_NAME, blocking=False,
                        description="Dagster decides as Airflow did: every schedule tick and input event.")(
                _evaluator(airflow, spec, job, every_dag, gap)) for spec, job in shadow_jobs(jobs, graph)]


def _evaluator(airflow, spec, job, every_dag, gap):
    # A closure, not default arguments: Dagster reads every parameter after the context as an input.
    from dagster import AssetCheckExecutionContext

    inputs = [observe_assets.asset_key(dataset) for dataset in spec.starting_inputs]

    def evaluate(context: AssetCheckExecutionContext):
        end = time.time()
        start = end - WINDOW_SECONDS - settle_seconds(spec, job)
        # Read from one settle before the window: a run that began then is the first run's previous.
        after = start - settle_seconds(spec, job)
        runs = airflow_runs(airflow, every_dag, after)
        starts = [run.start for found in runs.values() for run in found]
        events = [record.timestamp for key in inputs for record in _records(context.instance, key, after)]
        return result(spec, compare(spec, job, runs.get(spec.dag_id, []), events, start, end,
                                    outages(starts, gap, start, end)))

    return evaluate
