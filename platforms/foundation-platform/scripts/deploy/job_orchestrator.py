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

Dagster is asked over its webserver's GraphQL, at the address the job list already names
(`observers.dagster.ui`, which the asset reconciler's metrics read too); it is the host's loopback. Exit 0
done, 64 a request this cannot serve (a job Dagster does not decide), 1 Dagster did not do it.
"""

import json
import pathlib
import sys
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "orchestration" / "dags"))
import job_specs  # noqa: E402

REQUEST_SECONDS = 30

SENSORS = """
query Sensors($repository: RepositorySelector!) {
  sensorsOrError(repositorySelector: $repository) {
    __typename
    ... on Sensors { results { name sensorState { id status } } }
    ... on PythonError { message }
  }
}
"""
STOP = """
mutation Stop($id: String!) {
  stopSensor(id: $id) { __typename ... on PythonError { message } }
}
"""
START = """
mutation Start($sensor: SensorSelector!) {
  startSensor(sensorSelector: $sensor) { __typename ... on PythonError { message } }
}
"""
LAUNCH = """
mutation Launch($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message }
  }
}
"""
REPOSITORY = {"repositoryLocationName": job_specs.DAGSTER_LOCATION, "repositoryName": job_specs.DAGSTER_REPOSITORY}


class Refused(Exception):
    """A request this cannot serve (exit 64)."""


class Unanswered(Exception):
    """Dagster did not do what was asked (exit 1)."""


def decided(jobs):
    """The enabled job ids Dagster decides, in job-list order."""
    return [job["id"] for job in jobs["jobs"] if job["enabled"] and job.get("dagster_stage", "airflow") == "dagster"]


def poster(jobs):
    """`post(query, variables)` to Dagster's GraphQL at the job list's `observers.dagster.ui`: its `data`."""
    url = ((jobs.get("observers") or {}).get("dagster") or {}).get("ui") or ""
    if not url.startswith("http://127.0.0.1:"):
        raise Unanswered(f"jobs.v1.json observers.dagster.ui {url!r} is not Dagster on this host's loopback")

    def post(query, variables):
        request = urllib.request.Request(f"{url}/graphql", method="POST", headers={"Content-Type": "application/json"},
                                         data=json.dumps({"query": query, "variables": variables}).encode())
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_SECONDS) as response:
                answer = json.loads(response.read())
        except (OSError, ValueError) as error:
            raise Unanswered(f"Dagster did not answer at {url}: {error}") from None
        if answer.get("errors") or not isinstance(answer.get("data"), dict):
            raise Unanswered(f"Dagster refused the request: {json.dumps(answer.get('errors'))[:300]}")
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
    """Starts each job's sensor that is not running; the ids it started."""
    states, started = sensor_states(post), []
    for job_id, sensor in _named(jobs, job_ids):
        if sensor not in states:
            raise Unanswered(f"Dagster holds no sensor {sensor}: the code location has not loaded this release's job list")
        if states[sensor][1] != "RUNNING":
            _expect(post(START, {"sensor": {**REPOSITORY, "sensorName": sensor}})["startSensor"], "Sensor", f"starting {sensor}")
            started.append(job_id)
    return started


def start(jobs, job_id, kind, post):
    """Launches the job's Dagster job once with run kind `kind`; the run id."""
    _named(jobs, [job_id])
    if not kind or not job_specs.RUN_KIND.fullmatch(kind):
        raise Refused(f"{kind[:60]!r} is not `triggered` or `requested <request id>`")
    job = next(job for job in jobs["jobs"] if job["id"] == job_id)
    priority = jobs["priority_classes"][job_specs.priority_class(jobs, job, job_id)]["value"]
    tags = job_specs.run_tags(job["pool"], priority, kind)
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
                done = (pause if command == "pause" else unpause)(jobs, argv[2:], post or poster(jobs))
                print(f"job-orchestrator: {command}d {len(done)} of {len(argv) - 2} sensors: {' '.join(done) or 'none'}")
        else:
            if len(argv) < 3:
                raise Refused("start names one job")
            run_id = start(jobs, argv[2], " ".join(argv[3:]) or "triggered", post or poster(jobs))
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
