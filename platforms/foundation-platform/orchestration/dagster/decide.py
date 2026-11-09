"""When Dagster starts a job it decides itself (`dagster_stage: dagster`, root ADR-0431 §3): one sensor per job.

Generated from the job list like everything here: a job at `dagster` gets the sensor `foundation_decide__<id>`,
which asks every SENSOR_SECONDS whether the job's Dagster job (run_jobs.py) is due, by the rules the dual
run proved Airflow keeps (dual_run.py; 0 differences in 4,074 decisions over 8 production days):

- **Clock ticks** come from the job's `schedule` in UTC, by the iterator Dagster's schedules use
  (dual_run.schedule_ticks). A tick while the job is free starts a run. A tick while a run of the job is
  queued or running waits, and when the job is free only the latest waiting tick runs: the rest fold into
  it (`max_active_runs=1`, dual_run.due_ticks).
- **Input events**: a materialization of one of the job's starting inputs (JobSpec.starting_inputs) starts a
  run; events while a run is open fold into the one run after it (Airflow's queued asset events,
  Dagster's declarative automation).

Where Dagster does not do as Airflow did, by decision, with the incident each rule answers:

- **A job's sensor never runs a tick from before it first looked.** 2026-11-05 23:46:32 the deploy unpaused
  a new DAG and Airflow 3.3's CronTriggerTimetable ran that day's 18:45 tick at once (root ADR-0425): the
  unit-price migration took the Spark pool into the night and an owner's cut-over failed waiting for it.
  Here a sensor's first evaluation (a job newly moved to `dagster`, or a sensor reset) starts from now.
- **A tick missed while nothing evaluated (the daemon down, the sensor stopped) runs late only if it would
  end before the job's next tick**, by the job's own run limit (time_limit_minutes + stop_grace_minutes):
  Kubernetes CronJob's `startingDeadlineSeconds`, derived, not written down. Otherwise it waits for that
  next tick, which does the same work. Airflow ran the latest missed tick whatever the hour (the court
  auction collector's 11-05 16:05 tick at 11-06 07:48).
- **An input event missed while nothing evaluated still starts one run**: the data changed, and the one
  run is what reads it (the owner's rule, partial processing). Airflow dropped those (root ADR-0179) and
  the deploy started the job once instead (root ADR-0337).
- **Capacity, not the clock, orders the starts** (root ADR-0435): every run carries its priority class's
  value as `dagster/priority`, the queue starts higher classes first within dagster.yaml's limits, and the
  host's start (job_units.py pool-full) still holds back a start that would leave a higher claim no room,
  whoever starts it: a late run of a low class waits rather than run into a publish.

Each request's run key names its decision (`tick:<iso>`, `inputs:<latest event>`), so a repeated
evaluation never starts a decision twice.
"""

import datetime
import json
import math
import time
from dataclasses import dataclass, field

import dual_run
import observe_assets

job_specs = observe_assets.job_specs

SENSOR_PREFIX = "foundation_decide__"
SENSOR_SECONDS = 30
# Longer than this between two evaluations is an outage of the decider (the daemon was down or the sensor
# stopped), not its loop: ten loops, which a daemon restart for a release (about two minutes) stays inside.
OUTAGE_SECONDS = 10 * SENSOR_SECONDS
# The run states in which a job's run holds it (a tick or an event waits behind it).
OPEN_STATUSES = ("NOT_STARTED", "QUEUED", "STARTING", "STARTED", "CANCELING")


@dataclass
class Cursor:
    """What a job's sensor carries from one evaluation to the next."""

    since: float  # the moment up to which ticks are taken
    event_id: int = 0  # the event log's storage id up to which input events are taken
    waiting: list = field(default_factory=list)  # ticks that came while the job was held
    pending: float | None = None  # the newest input event not yet started, while the job was held

    def dump(self):
        return json.dumps({"since": self.since, "event_id": self.event_id, "waiting": self.waiting,
                           "pending": self.pending})

    @classmethod
    def load(cls, text):
        value = json.loads(text)
        return cls(value["since"], int(value.get("event_id", 0)), list(value.get("waiting", [])), value.get("pending"))


@dataclass(frozen=True)
class Request:
    key: str  # the run key
    kind: str  # what the forced command reads after the job id: "" for a tick, "triggered" for inputs
    at: float  # the tick, or the newest input event, it starts


def _next_tick(spec, after):
    ticks = dual_run.schedule_ticks(spec.schedule, after + 1, after + 400 * dual_run.DAY_SECONDS)
    return ticks[0] if ticks else math.inf


def _iso(at):
    return datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat()


def decide(spec, run_limit_seconds, cursor, now, held, events, newest_event_id=0):
    """(requests, the next cursor) for one evaluation at `now`.

    `cursor` is the previous evaluation's (None on the first look), `held` whether a run of the job is
    open, `events` the (storage id, time) of the starting inputs' materializations after `cursor.event_id`,
    and `newest_event_id` the event log's newest storage id on the first look, where taking starts.
    """
    if cursor is None:
        # A sensor's first look starts from now: no tick or event from before it is a decision of its.
        return [], Cursor(since=now, event_id=newest_event_id)
    gap = now - cursor.since > OUTAGE_SECONDS
    ticks = dual_run.schedule_ticks(spec.schedule, cursor.since, now) if spec.schedule else []
    waiting = sorted({*cursor.waiting, *ticks})
    event_id = max([cursor.event_id, *(storage_id for storage_id, _ in events)])
    newest = max([*(at for _, at in events), *([cursor.pending] if cursor.pending is not None else [])], default=None)
    if newest is not None and spec.started_by != "inputs":
        newest = None
    if held:
        return [], Cursor(since=now, event_id=event_id, waiting=waiting, pending=newest)
    requests = []
    if waiting:
        latest = waiting[-1]
        # Came while nothing evaluated, not while the job was held: late only if it ends before the next tick.
        missed = gap and latest >= cursor.since
        if not missed or now + run_limit_seconds <= _next_tick(spec, latest):
            requests.append(Request(key=f"tick:{_iso(latest)}", kind="", at=latest))
    if newest is not None and not requests:
        requests.append(Request(key=f"inputs:{_iso(newest)}", kind="triggered", at=newest))
    # A tick and an event due together are one run (the tick's, which reads the inputs as they are), as
    # Airflow's AssetOrTimeSchedule starts one run for both.
    return requests, Cursor(since=now, event_id=event_id)


def _newest_event_id(instance, keys):
    from dagster import AssetRecordsFilter

    found = [instance.fetch_materializations(AssetRecordsFilter(asset_key=key), limit=1, ascending=False).records
             for key in keys]
    return max((records[0].storage_id for records in found if records), default=0)


def _events(instance, keys, after_id):
    from dagster import AssetRecordsFilter

    return [(record.storage_id, record.timestamp) for key in keys
            for record in instance.fetch_materializations(
                AssetRecordsFilter(asset_key=key, after_storage_id=after_id), limit=1000).records]


def evaluator(spec, run_limit_seconds, clock=time.time):
    """The sensor function of one job (`clock` stands in for the wall clock in the tests)."""
    from dagster import DagsterRunStatus, RunRequest, RunsFilter, SkipReason

    inputs = [observe_assets.asset_key(dataset) for dataset in spec.starting_inputs]

    def evaluate(context):
        cursor = Cursor.load(context.cursor) if context.cursor else None
        held = context.instance.get_runs_count(RunsFilter(
            job_name=spec.dagster_job, statuses=[DagsterRunStatus(status) for status in OPEN_STATUSES])) > 0
        events = _events(context.instance, inputs, cursor.event_id) if cursor is not None else []
        first = _newest_event_id(context.instance, inputs) if cursor is None else 0
        requests, cursor = decide(spec, run_limit_seconds, cursor, clock(), held, events, first)
        context.update_cursor(cursor.dump())
        if not requests:
            return SkipReason("a run of the job is open; what is due waits" if held else "nothing is due")
        return [RunRequest(run_key=request.key, tags=spec.dagster_tags(request.kind)) for request in requests]

    return evaluate


def sensors(jobs=None, graph=None):
    """One sensor per job Dagster decides, each starting the job's Dagster job (run_jobs.py)."""
    from dagster import DefaultSensorStatus, sensor

    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    by_id = {job["id"]: job for job in jobs["jobs"]}
    return [sensor(name=SENSOR_PREFIX + spec.job_id, job_name=spec.dagster_job, minimum_interval_seconds=SENSOR_SECONDS,
                   default_status=DefaultSensorStatus.RUNNING,
                   description=f"Decides when {spec.job_id} runs (root ADR-0431 §3, orchestration/dagster/decide.py)")(
                evaluator(spec, 60 * job_specs.run_limit_minutes(by_id[spec.job_id])))
            for spec in job_specs.load_specs(jobs, graph) if spec.dagster_stage == "dagster"]
