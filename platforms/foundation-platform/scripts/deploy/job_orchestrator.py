#!/usr/bin/env python3
"""Holds, releases and starts a job Dagster decides (`dagster_stage: dagster`, root ADR-0565).

    job_orchestrator.py decided <jobs.v1.json>                  the enabled job ids Dagster decides, one per line
    job_orchestrator.py pause   <jobs.v1.json> <job id>...      stops each job's sensor: nothing starts it
    job_orchestrator.py unpause <jobs.v1.json> <job id>...      starts the sensors of those that are stopped
    job_orchestrator.py start   <jobs.v1.json> <job id> [triggered | requested <request id>]

What the deploy and an operator's run-now do to a job through its Airflow DAG (`airflow dags pause`,
`unpause`, `trigger`; foundation-deploy.sh, deploy-start-once.sh, run-now-dispatch.sh), for a job that has
none. Which of the two a job is asked through is its `dagster_stage` in the job list and nothing else.

- **pause** stops the job's sensor (orchestration/dagster/decide.py `foundation_decide__<id>`). A run in
  progress runs on, as a paused DAG's does. The sensor keeps its cursor: when it starts again, a tick it
  missed runs only if it still ends before the job's next tick, and an input event it missed starts one
  run, so the deploy asks no "owed by inputs" for these jobs (root ADR-0337 was Airflow dropping the event).
- **unpause** starts every named job's sensor that is not running; a sensor Dagster does not hold yet (the
  code location is still loading the release) is an error, since the job would stay undecided.
- **start** launches the job's Dagster job once (orchestration/dagster/run_jobs.py) with the run kind the
  host's forced command reads: `triggered` (the default: the deploy starting a run it owes) or `requested
  <id>` (an operator's run-now, root ADR-0282 §4). A run already open is joined on the host, not doubled
  (start-scheduled-job.sh). A `triggered` start launches nothing while a run of the job waits to start
  (QUEUED): that run has read nothing yet, so it is the run the deploy owes, as the job's sensor starts
  nothing while a run of it is open (decide.py). A run in progress does not cover it (it began before the
  release), so one run queues behind it. An operator's request is always launched: the host answers it by
  its id. 2026-11-23: a run the deploy owes stays recorded until its unit starts on the host
  (job_units.py deferred), the spark pool's one slot was taken for hours, and eleven deploys each launched
  the same eight Gold rebuilds again: 74 of 93 queued runs were a second to eleventh copy.

Dagster is asked over the writable webserver's unix socket (dagster_writes.WRITER_SOCKET), in a directory
only root opens; its gate answers these documents and nothing else (root ADR-0565 §8). Every reader asks
the read-only webserver instead; no host port reaches this one. Exit 0 done, 64 a request this cannot
serve (a job Dagster does not decide), 1 Dagster did not do it.

A **pause** that finds no Dagster at all is done: when the writable webserver does not answer and the
read-only one shows no healthy sensor daemon (or does not answer either), nothing Dagster decides can start,
so its jobs are held as asked. It says so loudly; FoundationDagsterCodeLocationDown alerts on the same
outage. 2026-11-23 the instance config made the daemon and both webservers restart in a loop; every
deploy then stopped at step 0 asking Dagster to stop its sensors, so the release that fixed the config
could not be deployed. An unpause or a start still needs Dagster: the deploy's step 7 asks the Dagster
the new release brings up.
"""

import json
import pathlib
import sys
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "orchestration" / "dags"))
import dagster_writes  # noqa: E402
import job_specs  # noqa: E402

SENSORS, STOP, START, LAUNCH = (dagster_writes.SENSORS, dagster_writes.STOP_SENSOR, dagster_writes.START_SENSOR,
                                dagster_writes.LAUNCH_RUN)
WAITING = dagster_writes.WAITING_RUN
REPOSITORY = dagster_writes.REPOSITORY


class Refused(Exception):
    """A request this cannot serve (exit 64)."""


class Unanswered(Exception):
    """Dagster did not do what was asked (exit 1)."""


class Unreachable(Unanswered):
    """The writable webserver did not answer at all (its socket absent, the connection refused or cut)."""


def decided(jobs):
    """The enabled job ids Dagster decides, in job-list order."""
    return [job["id"] for job in jobs["jobs"] if job["enabled"] and job.get("dagster_stage", "airflow") == "dagster"]


def poster(socket_path=None):
    """`post(query, variables)` to the writable webserver over its unix socket: the answer's `data`."""
    where = socket_path or dagster_writes.WRITER_SOCKET

    def post(query, variables):
        try:
            status, body = dagster_writes.ask("POST", "/graphql", {"query": query, "variables": variables}, where)
            answer = json.loads(body)
        except (OSError, ValueError) as error:
            raise Unreachable(f"Dagster did not answer at {where}: {error}") from None
        if status != 200 or answer.get("errors") or not isinstance(answer.get("data"), dict):
            raise Unanswered(f"Dagster refused the request ({status}): {json.dumps(answer.get('errors'))[:300]}")
        return answer["data"]

    return post


def _expect(found, typename, what):
    if (found or {}).get("__typename") != typename:
        raise Unanswered(f"{what}: Dagster answered {(found or {}).get('__typename')} {(found or {}).get('message', '')}".rstrip())
    return found


def sensor_states(post):
    """Sensor name -> (its state's id, its status) for every sensor the code location holds."""
    found = _expect(post(SENSORS, {"repository": REPOSITORY})["sensorsOrError"], "Sensors", "the sensors")
    return {sensor["name"]: (sensor["sensorState"]["id"], sensor["sensorState"]["status"]) for sensor in found["results"]}


def _named(jobs, job_ids):
    known = set(decided(jobs))
    unknown = [job_id for job_id in job_ids if job_id not in known]
    if unknown:
        raise Refused(f"Dagster does not decide {', '.join(unknown)}; a job Airflow decides is held through its DAG")
    return [(job_id, job_specs.DAGSTER_SENSOR_PREFIX + job_id) for job_id in job_ids]


def pause(jobs, job_ids, post):
    """Stops each job's sensor; the ids it stopped. A sensor the location does not hold decides nothing."""
    states, stopped = sensor_states(post), []
    for job_id, sensor in _named(jobs, job_ids):
        state_id, status = states.get(sensor, (None, "STOPPED"))
        if status == "RUNNING":
            _expect(post(STOP, {"id": state_id})["stopSensor"], "StopSensorMutationResult", f"stopping {sensor}")
            stopped.append(job_id)
    return stopped


def unpause(jobs, job_ids, post):
    """Starts each job's sensor that is not running; the ids it started. A sensor Dagster does not hold
    fails the call, after every one it does hold is started: one missing job holds back no other."""
    states, started, missing = sensor_states(post), [], []
    for job_id, sensor in _named(jobs, job_ids):
        if sensor not in states:
            missing.append(sensor)
        elif states[sensor][1] != "RUNNING":
            _expect(post(START, {"sensor": {**REPOSITORY, "sensorName": sensor}})["startSensor"], "Sensor", f"starting {sensor}")
            started.append(job_id)
    if missing:
        raise Unanswered(f"Dagster holds no sensor {', '.join(missing)}: the code location has not loaded this "
                         f"release's job list (started {' '.join(started) or 'none'})")
    return started


# What the read-only webserver says of the daemon that ticks the sensors.
DAEMONS = "{ instance { daemonHealth { allDaemonStatuses { daemonType healthy } } } }"


def sensor_daemon_healthy(jobs, ask=None):
    """Whether Dagster's sensor daemon is healthy, as the read-only webserver (jobs.v1.json
    observers.dagster.ui) says; None when that does not answer either."""
    if ask is None:
        url = jobs.get("observers", {}).get("dagster", {}).get("ui", "").rstrip("/")
        if not url:
            return None

        def ask(query):
            request = urllib.request.Request(f"{url}/graphql", data=json.dumps({"query": query}).encode(),
                                             headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 (loopback, from the job list)
                return json.loads(response.read())

    try:
        answer = ask(DAEMONS)
        statuses = answer["data"]["instance"]["daemonHealth"]["allDaemonStatuses"]
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return any(status.get("daemonType") == "SENSOR" and status.get("healthy") for status in statuses)


def held_without_dagster(jobs, job_ids, error, health=sensor_daemon_healthy):
    """For a pause the writable webserver did not answer: True (held, said loudly) when no healthy sensor
    daemon can be seen either, so nothing Dagster decides can start; False when the sensors may still
    run, so they could not be held."""
    if health(jobs) is True:
        return False
    print(f"job-orchestrator: HELD WITHOUT DAGSTER: {error}; its sensor daemon is not seen healthy either, so none "
          f"of the {len(job_ids)} jobs it decides can start while it does not answer: they are held as asked. "
          "The deploy's step 7 starts their sensors against the Dagster the release brings up "
          "(FoundationDagsterCodeLocationDown alerts on this outage).", file=sys.stderr)
    return True


def launch_tags(jobs, job_id, kind):
    """The tags a start of the job carries: its sensor's runs' own (JobSpec.dagster_tags), so its class and
    queue weight order it (dagster_priority) and a job Dagster decides counts under the decided-by limit.
    Until root ADR-0605 a deploy's start-once or an operator's run-now sent the class value alone, so it
    queued behind the sensor runs of its class. Read from the release's whole list and pipeline graph."""
    spec = next(spec for spec in job_specs.load_specs(jobs) if spec.job_id == job_id)
    return spec.dagster_tags(kind)


def start(jobs, job_id, kind, post, tags):
    """Launches the job's Dagster job once with run kind `kind` and `tags` (launch_tags); the run id."""
    _named(jobs, [job_id])
    if not kind or not job_specs.RUN_KIND.fullmatch(kind):
        raise Refused(f"{kind[:60]!r} is not `triggered` or `requested <request id>`")
    found = _expect(post(LAUNCH, {"params": {
        "mode": "default",
        "executionMetadata": {"tags": [{"key": key, "value": value} for key, value in tags.items()]},
        "runConfigData": "{}",
        "selector": {**REPOSITORY, "pipelineName": job_specs.DAGSTER_JOB_PREFIX + job_id},
    }})["launchRun"], "LaunchRunSuccess", f"starting {job_id}")
    return found["run"]["runId"]


def waiting(job_id, post):
    """The id of a run of the job that waits to start, or None. An answer Dagster does not give (a gate of
    a release before this question) is None: the start is launched, as before."""
    try:
        found = _expect(post(WAITING, {"job": job_specs.DAGSTER_JOB_PREFIX + job_id})["runsOrError"], "Runs",
                        f"asking for a waiting run of {job_id}")
    except Unanswered as error:
        print(f"job-orchestrator: {error}; starting {job_id} without knowing", file=sys.stderr)
        return None
    return found["results"][0]["runId"] if found["results"] else None


def main(argv, post=None, health=sensor_daemon_healthy):
    if len(argv) < 2 or argv[0] not in ("decided", "pause", "unpause", "start"):
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 64
    command, jobs = argv[0], json.loads(pathlib.Path(argv[1]).read_text(encoding="utf-8"))
    try:
        if command == "decided":
            print("".join(f"{job_id}\n" for job_id in decided(jobs)), end="")
        elif command in ("pause", "unpause"):
            if argv[2:]:
                done = (pause if command == "pause" else unpause)(jobs, argv[2:], post or poster())
                print(f"job-orchestrator: {command}d {len(done)} of {len(argv) - 2} sensors: {' '.join(done) or 'none'}")
        else:
            if len(argv) < 3:
                raise Refused("start names one job")
            kind = " ".join(argv[3:]) or "triggered"
            _named(jobs, [argv[2]])
            post = post or poster()
            run_id = waiting(argv[2], post) if kind == "triggered" else None
            if run_id is not None:
                print(f"job-orchestrator: {argv[2]} already waits to start as Dagster run {run_id}; not started again")
            else:
                run_id = start(jobs, argv[2], kind, post, launch_tags(jobs, argv[2], kind))
                print(f"job-orchestrator: started {argv[2]} as Dagster run {run_id}")
    except Refused as error:
        print(f"job-orchestrator: refused: {error}", file=sys.stderr)
        return 64
    except Unreachable as error:
        if command == "pause" and held_without_dagster(jobs, argv[2:], error, health):
            return 0
        print(f"job-orchestrator: {error}", file=sys.stderr)
        return 1
    except Unanswered as error:
        print(f"job-orchestrator: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
