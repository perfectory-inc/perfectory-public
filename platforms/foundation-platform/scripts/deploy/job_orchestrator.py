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
  (start-scheduled-job.sh).

Dagster is asked over the writable webserver's unix socket (dagster_writes.WRITER_SOCKET), in a directory
only root opens; its gate answers these documents and nothing else (root ADR-0565 §8). Every reader asks
the read-only webserver instead; no host port reaches this one. Exit 0 done, 64 a request this cannot
serve (a job Dagster does not decide), 1 Dagster did not do it.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "orchestration" / "dags"))
import dagster_writes  # noqa: E402
import job_specs  # noqa: E402

SENSORS, STOP, START, LAUNCH = (dagster_writes.SENSORS, dagster_writes.STOP_SENSOR, dagster_writes.START_SENSOR,
                                dagster_writes.LAUNCH_RUN)
REPOSITORY = dagster_writes.REPOSITORY


class Refused(Exception):
    """A request this cannot serve (exit 64)."""


class Unanswered(Exception):
    """Dagster did not do what was asked (exit 1)."""


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
            raise Unanswered(f"Dagster did not answer at {where}: {error}") from None
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


def main(argv, post=None):
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
            run_id = start(jobs, argv[2], kind, post or poster(), launch_tags(jobs, argv[2], kind))
            print(f"job-orchestrator: started {argv[2]} as Dagster run {run_id}")
    except Refused as error:
        print(f"job-orchestrator: refused: {error}", file=sys.stderr)
        return 64
    except Unanswered as error:
        print(f"job-orchestrator: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
