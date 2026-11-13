"""The dual run of a job moving to Dagster: both orchestrators decide, Airflow alone runs (root ADR-0431 §2).

A job at `dagster_stage: shadow` (orchestration/jobs.v1.json) is still started and run by Airflow. Dagster
reads those runs from Airflow's API (every state: a failed run was a decision too) and the asset
events on the job's inputs, also from Airflow's API (`airflow_input_events`: the fact Airflow decides
from, at the time Airflow recorded it), and decides beside Airflow by the rules it will use once the job
is its own:

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

`manual` runs are an operator's and are not compared, but they hold the job like any run. A stretch in which Airflow started no DAG at all is
an outage (`outages`): Dagster's decisions there say how available Airflow was, not how it decides, and
are reported apart (`airflow_out`), not as differences. A stretch in which the job's own DAG was paused
(`paused_spans`, from Airflow's audit log) is a pause, not a decision: every deploy pauses the DAGs it
drains and unpauses them after the switch (foundation-deploy.sh steps 0 and 7, many times a day). A
tick while paused is not due, except that the latest tick of a pause runs when it ends (`catchup=False`;
the court auction collector's 2026-11-05 16:05 tick ran at 2026-11-06 07:48), and an input event while
paused starts no run (Airflow drops it, root ADR-0179; the deploy starts the job once itself, root
ADR-0337). Both are reported apart (`paused`). The `dual_run_matches_airflow` check on the
job's DAG asset runs after every Airflow run airlift observes and reports (WARN, never blocking) the
decisions that differ. Every check is generated from the job list: a job enters or leaves the dual run
by its `dagster_stage`.

Every comparison is also appended to a ledger (`LEDGER`, one JSON line each, never rewritten), with
the decisions both made (`agreed`), and `python dual_run.py gate` reads it into the move gate of root
ADR-0458 (superseding ADR-0431 §4): a job moves past `shadow` once the ledger holds `GATE_AGREEING`
distinct agreeing decisions of it since its last difference, and one of every kind it makes
(`decision_kinds`: its schedule's cadence, and `event` when its inputs start it). A difference resets
that count and pages (`page`, through Alertmanager).
Replayed over the production Airflow's 2026-10-29..11-05 (54 jobs, 4,074 decisions, read-only), these
rules differ on none; the rules of the first slice differed on 200 (pauses and folds at a resume).
"""

import argparse
import datetime
import json
import math
import os
import pathlib
import sys
import time
import urllib.request
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
# Every comparison, appended as one JSON line and never rewritten (the owner's rule: all data is
# appended): the evidence the move gate reads (ADR-0431 §4, `gate`). On the instance's own volume
# (compose.orchestration.yml `dagster-state`), beside its artifacts and compute logs, so it outlives
# a release switch and the instance's database.
LEDGER = pathlib.Path("/var/lib/dagster/dual-run/comparisons.jsonl")
# The distinct decisions both orchestrators made alike, since the job's last difference, that move it
# past `shadow` (root ADR-0458): more than one, so one tick is not the whole proof, and few, so an
# hourly and a daily job alike move within days; the kinds (`decision_kinds`) carry the rest.
GATE_AGREEING = 3
# Where a difference pages (compose.orchestration.yml): the runtime project's Alertmanager.
ALERTMANAGER_URL = os.environ.get("FOUNDATION_DAGSTER_ALERTMANAGER_URL", "http://alertmanager:9093")
# A page lasts a day past the last difference (the unit-failure alerts' hold), then resolves itself.
PAGE_HOLD_HOURS = 26


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
    agreed: list = field(default_factory=list)  # what both decided: (kind, its tick or run start)
    only_dagster: list = field(default_factory=list)  # what Dagster would start and Airflow did not
    only_airflow: list = field(default_factory=list)  # what Airflow started and Dagster would not
    airflow_out: list = field(default_factory=list)  # Dagster's decisions while Airflow started nothing
    paused: list = field(default_factory=list)  # ticks and input events while the job's DAG was paused

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


# The scheduler sees a run end or an unpause up to a loop later: a tick that close to the moment the
# job is free again runs as its own run after the catch-up (4, 5 and 13 s on the production Airflow,
# 2026-11-03 and 11-04: map_edit_fold_admin 12:35 and 13:35, asset_reconcile 02:25 and 02:30,
# onbid_public_auction_silver 13:50 and 14:50 each ran both).
SCHEDULER_LAG_SECONDS = 60.0


def due_ticks(ticks, runs, paused=()):
    """The ticks that start a run, as Airflow's scheduler takes them (`max_active_runs=1`,
    `catchup=False`): a tick starts a run at once while the job is free; while a run of the job (of
    any type) is in progress or its DAG is paused, ticks wait, and when it is free again only the
    latest waiting tick runs, the rest fold into it. A tick that came within the scheduler's lag of
    that moment waits for the catch-up run instead. A decided tick holds the job until its run starts
    (or a lag passes without one); a run of another type that starts first took the job before the
    scheduler acted (an operator's run 3 s after an unpause, onbid_public_auction_silver 2026-11-04
    08:27), and the ticks wait on. A run still in progress, or a pause still open, holds every tick
    after it.
    """
    return _due(ticks, runs, paused)[0]


def _due(ticks, runs, paused=()):
    """`due_ticks`, and the catch-up ticks among them that Airflow may as well skip: when a tick came
    within the scheduler's lag of the moment the job is free, which of the two the scheduler takes
    first is a race with its own loop. It ran both on 2026-11-03 and 11-04 (above) and only the later
    one on 2026-11-12 (rt_molit_real_transaction_collect, unpaused 18:25:13: the 18:25 tick ran and
    17:25 never did). Either is how Airflow decides; Dagster's own rule runs the latest one."""
    changes = sorted([(run.start, 1, run.run_type) for run in runs]
                     + [(run.end, -1, "") for run in runs if run.end is not None]
                     + [(a, 2, "") for a, _ in paused] + [(b, -2, "") for _, b in paused if b != math.inf])
    running = held = 0
    awaiting = None  # when a decided tick stops holding the job if no run started
    before = None  # the ticks waiting before the last decision, while it awaits its run
    waiting, due, either = [], [], set()

    def free(now):
        nonlocal awaiting, before, waiting
        if running or held or (awaiting is not None and now < awaiting) or not waiting:
            return
        early = [tick for tick in waiting if tick <= now - SCHEDULER_LAG_SECONDS]
        before = waiting
        due.append(max(early or waiting))
        waiting = [tick for tick in waiting if early and tick > now - SCHEDULER_LAG_SECONDS]
        if waiting:
            either.add(due[-1])
        awaiting = now + SCHEDULER_LAG_SECONDS

    index = 0
    for tick in [*ticks, math.inf]:
        while index < len(changes) and changes[index][0] <= tick:
            at, change, run_type = changes[index]
            index += 1
            if change in (1, -1):
                running = max(0, running + change)
                if change == 1 and awaiting is not None and at < awaiting and run_type != "scheduled":
                    either.discard(due.pop())
                    waiting = sorted({*before, *waiting})
                if change == 1:
                    awaiting = None
            else:
                held = max(0, held + change // 2)
            free(at)
        if tick == math.inf:
            break
        free(awaiting if awaiting is not None and not running and not held and awaiting <= tick else tick)
        waiting.append(tick)
        free(tick)
    return due, either


def _freed(at, spans):
    """When a job held at `at` by any of `spans` (its runs and pauses) is free again; `at` if it is not held."""
    held = True
    while held:
        held = False
        for a, b in spans:
            if a <= at < b:
                at, held = b, True
    return at


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


# The audit-log events that pause and unpause a DAG: the deploy's and airflow-runtime.sh's `airflow
# dags pause|unpause` (9,792 of them on the production Airflow 2026-10-27..11-06, and no other kind).
PAUSE_EVENTS = {"cli_dag_pause": True, "cli_dag_unpause": False}


def paused_spans(events, start, end, paused_at_start=False):
    """The stretches of [start, end) in which a DAG was paused, from its (time, paused) audit-log events
    in that stretch and whether it was paused at `start` (the state the last event before it left).

    Both events repeat (every deploy unpauses every enabled DAG, paused or not), so a state is read
    from the last event, never inferred from the next one. A pause still open at `end` lasts to math.inf.
    """
    spans, since = [], start if paused_at_start else None
    for at, paused in sorted(event for event in events if start <= event[0] < end):
        if paused and since is None:
            since = at
        elif not paused and since is not None:
            spans.append((since, at))
            since = None
    if since is not None:
        spans.append((since, math.inf))
    return spans


def _deadline(event, settle, spans):
    """`settle` seconds of Airflow's up time after `event`: every outage in between pushes it later."""
    deadline = event + settle
    for a, b in sorted(spans):
        if a < deadline and b > event:
            deadline += b - max(a, event)
    return deadline


def compare(spec, job, runs, input_events, start, end, out=(), paused=()):
    """The decisions Dagster and Airflow made for one job over [start, end) (epoch seconds).

    `runs` are the job's finished Airflow DAG runs, `input_events` the times its starting inputs were
    materialized, `out` the outages, `paused` the stretches its DAG was paused. Decisions whose runs
    may not have finished by `end` are not compared yet.
    """
    found = Comparison()
    settle = settle_seconds(spec, job)
    settled = end - settle
    if spec.schedule:
        airflow = {tick_of(run.run_id) for run in runs if run.run_type == "scheduled"} - {None}
        # Ticks past `settled` too, up to the latest run end or unpause: whether a tick folds depends on
        # the ticks after it.
        horizon = max([settled, *(run.end for run in runs if run.end is not None), *(b for _, b in paused if b != math.inf)])
        ticks = schedule_ticks(spec.schedule, start, horizon + 1)
        due, either = _due(ticks, runs, paused)
        dagster = {tick for tick in due if tick < settled}
        # Not compared: a catch-up tick Airflow may skip (`_due`), and a tick whose job came free within
        # the scheduler's lag of `end`, whose run Airflow has not created yet (unit_price_versions_migration
        # 2026-11-09: the run that held the 18:45 tick ended 21:49:53, the check read Airflow at 21:50:11.0
        # and the tick's run was queued at 21:50:11.7).
        spans = [(run.start, math.inf if run.end is None else run.end) for run in runs] + list(paused)
        dagster -= {tick for tick in dagster - airflow
                    if tick in either or _freed(tick, spans) > end - SCHEDULER_LAG_SECONDS}
        found.paused += [("schedule", tick) for tick in ticks
                         if tick < settled and tick not in dagster and _inside(tick, paused)]
        found.decisions += len(dagster)
        found.agreed += [("schedule", tick) for tick in sorted(dagster & airflow)]
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
                else:
                    found.agreed.append(("inputs", run_start))
            previous = run_start
        for event in events:
            if start <= event < settled and not any(event <= run_start <= _deadline(event, settle, out)
                                                    for run_start in triggered):
                if _inside(event, paused):
                    found.paused.append(("inputs", event))
                else:
                    (found.airflow_out if _inside(event, out) else found.only_dagster).append(("inputs", event))
    return found


def shadow_jobs(jobs, graph):
    """(spec, job) of every job in the dual run, from the job list: at `shadow`, and at `proxied`, where
    Airflow still decides and the move to `dagster` counts the same decisions (root ADR-0458 §4)."""
    by_id = {job["id"]: job for job in jobs["jobs"]}
    return [(spec, by_id[spec.job_id]) for spec in job_specs.load_specs(jobs, graph)
            if spec.dagster_stage in job_specs.COMPARED]


def dag_key(spec):
    from dagster import AssetKey

    return AssetKey([INSTANCE_NAME, "dag", spec.dag_id])


def airflow_input_events(airflow, datasets, after):
    """When (epoch seconds) Airflow recorded an asset event on any of `datasets` since `after`, from
    Airflow's own asset events.

    Not the materializations airlift observed: airlift's sensor writes those when it next polls, 3 to
    27 s after the event, and Airflow's scheduler starts the `asset_triggered` run in between (source
    sweep 2026-11-13: event 04:14:31.9, the land use plan refresh started 04:14:39.7, materialization
    04:14:59.1). Read from the event log, every such run came before its event: 2026-11-09..13, 81 runs
    "without an event" and 56 events "without a run" over 39 jobs, and 3 input decisions agreed.
    """
    uris = {job_specs.asset_uri(dataset) for dataset in datasets}
    if not uris:
        return []
    found, offset = [], 0
    since = datetime.datetime.fromtimestamp(after, datetime.UTC)
    while True:
        events, total = airflow.get_asset_events(since, offset=offset)
        found += [datetime.datetime.fromisoformat(event["timestamp"]).timestamp() for event in events
                  if event["uri"].rstrip("/") in uris]
        offset += len(events)
        if not events or offset >= total:
            return found


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


def airflow_pauses(airflow, dag_id, after, end):
    """The stretches of [after, end) in which the DAG was paused, from Airflow's audit log: the last
    pause or unpause before `after` says the state then (none at all: Airflow creates a DAG paused,
    `dags_are_paused_at_creation`), and every one after it the changes."""
    since = datetime.datetime.fromtimestamp(after, datetime.UTC)
    last, _ = airflow.get_event_logs(dag_id, PAUSE_EVENTS, before=since, newest_first=True, limit=1)
    paused_at_after = PAUSE_EVENTS[last[0]["event"]] if last else True
    events, offset = [], 0
    while True:
        page, total = airflow.get_event_logs(dag_id, PAUSE_EVENTS, after=since, offset=offset)
        events += [(datetime.datetime.fromisoformat(record["when"]).timestamp(), PAUSE_EVENTS[record["event"]])
                   for record in page]
        offset += len(page)
        if not page or offset >= total:
            return paused_spans(events, after, end, paused_at_after)


def _iso(at):
    return datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat()


def ledger_line(spec, comparison, start, end, run_id, release):
    """One comparison as the ledger keeps it."""
    def stamped(items):
        return [[kind, _iso(at)] for kind, at in items]

    return {"evaluated_at": _iso(end), "job": spec.job_id, "dag_id": spec.dag_id, "window": [_iso(start), _iso(end)],
            "matches": comparison.matches, "decisions": comparison.decisions,
            **{name: stamped(getattr(comparison, name))
               for name in ("agreed", "only_dagster", "only_airflow", "airflow_out", "paused")},
            "dagster_run_id": run_id, "release": release, "stage": spec.dagster_stage}


def append(ledger, line):
    """Appends one line to the ledger and makes it durable before the check reports: a comparison
    the gate cannot read is no evidence, so a write that fails fails the check run."""
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(line, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def page(spec, comparison, url=ALERTMANAGER_URL):
    """Pages a difference through Alertmanager (root ADR-0458): one alert per job, held a day past its
    last difference. A page that cannot be sent is said in the check's metadata; the comparison is not
    lost, its ledger line is already written."""
    now = datetime.datetime.now(datetime.UTC)
    differs = [[kind, _iso(at)] for kind, at in comparison.only_dagster + comparison.only_airflow]
    alert = {"labels": {"alertname": "FoundationDualRunDisagreed", "service": "dual-run", "environment": "prelaunch",
                        "job": spec.job_id},
             "annotations": {"summary": f"🟠 {spec.job_id}: Dagster 와 Airflow 의 결정이 달라요 — 이 작업의 이중 실행 증거를 "
                                        f"처음부터 다시 셉니다 (Dagster 만 {len(comparison.only_dagster)}, Airflow 만 "
                                        f"{len(comparison.only_airflow)}: {differs[:3]})"},
             "startsAt": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
             "endsAt": (now + datetime.timedelta(hours=PAGE_HOLD_HOURS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
    return post_alert(alert, url)


def post_alert(alert, url=ALERTMANAGER_URL):
    """Posts one Alertmanager alert; True when it was taken. A check that pages says so in its metadata."""
    request = urllib.request.Request(f"{url}/api/v2/alerts", data=json.dumps([alert]).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except OSError:
        return False


def result(spec, comparison, paged=None):
    from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue

    def stamped(items):
        return [[kind, datetime.datetime.fromtimestamp(at, datetime.UTC).isoformat()] for kind, at in items]

    return AssetCheckResult(
        passed=comparison.matches,
        severity=AssetCheckSeverity.WARN,
        metadata={"job": spec.job_id, "decisions": comparison.decisions,
                  "only_dagster": MetadataValue.json(stamped(comparison.only_dagster)),
                  "only_airflow": MetadataValue.json(stamped(comparison.only_airflow)),
                  "airflow_out": MetadataValue.json(stamped(comparison.airflow_out)),
                  "paused": MetadataValue.json(stamped(comparison.paused)),
                  **({} if paged is None else {"paged": paged})},
    )


def checks(airflow, jobs=None, graph=None, ledger=LEDGER):
    """One reporting check per job in the dual run, on the job's DAG asset; `airflow` is the observed
    instance (definitions.airflow_instance), read when a check runs, and every comparison is appended
    to `ledger`."""
    from dagster import asset_check

    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    specs = job_specs.load_specs(jobs, graph)
    every_dag = [spec.dag_id for spec in specs]
    gap = outage_gap_seconds(specs)
    return [asset_check(asset=dag_key(spec), name=CHECK_NAME, blocking=False,
                        description="Dagster decides as Airflow did: every schedule tick and input event.")(
                _evaluator(airflow, spec, job, every_dag, gap, ledger)) for spec, job in shadow_jobs(jobs, graph)]


# The release the code location was loaded from (/opt/foundation-platform/releases/<release>/orchestration/dagster).
RELEASE = pathlib.Path(__file__).resolve().parents[2].name


def _run_id(context):
    """The Dagster run the check ran in; a check invoked directly (a test) has none."""
    from dagster._core.errors import DagsterInvalidPropertyError

    try:
        return context.run.run_id
    except DagsterInvalidPropertyError:
        return None


def _evaluator(airflow, spec, job, every_dag, gap, ledger):
    # A closure, not default arguments: Dagster reads every parameter after the context as an input.
    from dagster import AssetCheckExecutionContext

    def evaluate(context: AssetCheckExecutionContext):
        end = time.time()
        start = end - WINDOW_SECONDS - settle_seconds(spec, job)
        # Read from one settle before the window: a run that began then is the first run's previous.
        after = start - settle_seconds(spec, job)
        runs = airflow_runs(airflow, every_dag, after)
        starts = [run.start for found in runs.values() for run in found]
        events = airflow_input_events(airflow, spec.starting_inputs, after)
        comparison = compare(spec, job, runs.get(spec.dag_id, []), events, start, end,
                             outages(starts, gap, start, end), airflow_pauses(airflow, spec.dag_id, after, end))
        append(ledger, ledger_line(spec, comparison, start, end, _run_id(context), RELEASE))
        return result(spec, comparison, None if comparison.matches else page(spec, comparison))

    return evaluate


def schedule_kind(schedule):
    """The cadence of a cron's ticks: `monthly` on given days of the month, `weekly` on given weekdays,
    else `daily` (an hourly job's ticks are daily decisions too)."""
    _, _, day_of_month, _, day_of_week = schedule.split()
    return "monthly" if day_of_month != "*" else "weekly" if day_of_week != "*" else "daily"


def decision_kinds(spec):
    """The ledger's decision kind -> the gate's, for every kind of decision the job makes."""
    found = {}
    if spec.schedule:
        found["schedule"] = schedule_kind(spec.schedule)
    if spec.started_by == "inputs":
        found["inputs"] = "event"
    return found


def _at(text):
    return datetime.datetime.fromisoformat(text)


def gate(lines, specs, need=GATE_AGREEING):
    """job id -> how far each job in the dual run is from moving past `shadow` (root ADR-0458), from the
    ledger's lines: the distinct decisions both made since its last difference (a check's window is a
    day, so one decision is in many lines), whether every kind of decision it makes is among them, and
    the decisions made while Airflow was out or the DAG paused, which count neither way (reported).

    A job counts the lines of its own stage only: one that moved to `proxied` starts counting again from
    the first comparison made there, so what moves it to `dagster` is evidence from Dagster running it
    (root ADR-0458 §4). A line from before stages were recorded is a `shadow` line."""
    by_job = {}
    for line in sorted(lines, key=lambda line: _at(line["evaluated_at"])):
        by_job.setdefault(line["job"], []).append(line)
    found = {}
    for spec in specs:
        agreed, apart, since = set(), set(), None
        for line in by_job.get(spec.job_id, []):
            if line.get("stage", "shadow") != spec.dagster_stage:
                continue
            if not line["matches"]:
                # Nothing up to the latest difference is evidence, not even a decision a later window agrees on.
                since = max(_at(at) for _, at in line["only_dagster"] + line["only_airflow"])
                agreed = {item for item in agreed if _at(item[1]) > since}
            agreed |= {tuple(item) for item in line.get("agreed", []) if since is None or _at(item[1]) > since}
            apart |= {tuple(item) for item in line["airflow_out"] + line["paused"] if since is None or _at(item[1]) > since}
        kinds = decision_kinds(spec)
        missing = sorted(set(kinds.values()) - {kinds[kind] for kind, _ in agreed if kind in kinds})
        found[spec.job_id] = {"agreeing": len(agreed), "kinds": sorted(set(kinds.values())), "missing_kinds": missing,
                              "ready": len(agreed) >= need and not missing,
                              "reset_at": None if since is None else since.isoformat(), "apart": sorted(apart)}
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description="The dual run's move gate (root ADR-0458), read from its ledger.")
    parser.add_argument("command", choices=["gate"])
    parser.add_argument("--ledger", type=pathlib.Path, default=LEDGER)
    parser.add_argument("--family", action="append", default=[], metavar="NAME=JOB,JOB",
                        help="also say whether these jobs, which move together (root ADR-0431 §7), are all ready")
    arguments = parser.parse_args(argv)
    lines = []
    if arguments.ledger.exists():
        with open(arguments.ledger, encoding="utf-8") as handle:
            lines = [json.loads(text) for text in handle if text.strip()]
    found = gate(lines, [spec for spec, _ in shadow_jobs(*observe_assets.contracts())])
    for job, state in found.items():
        print(json.dumps({"job": job, **state}, ensure_ascii=False))
    for family in arguments.family:
        name, _, jobs = family.partition("=")
        # A job no longer in the dual run (moved on, or never in it) is not evidence for its family.
        states = {job: found.get(job, {"ready": False, "agreeing": 0}) for job in jobs.split(",")}
        print(json.dumps({"family": name, "ready": all(state["ready"] for state in states.values()),
                          "agreeing": {job: state["agreeing"] for job, state in states.items()}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
