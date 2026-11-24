"""What may change Dagster, and who may ask (root ADR-0565 §8 footnote 2026-11-19).

Dagster OSS's webserver has no sign-in: anything that reaches its GraphQL can launch, terminate and
stop. So every reader asks the read-only webserver (`--read-only`, compose.orchestration.yml), and the
one writable webserver answers only the documents below, each checked against what its caller is
allowed to name (`orchestration/dagster/writer_gate.py`):

- **airflow**, on the compose network: what a proxied job's `run` task sends (dagster-airlift's
  verification, launch and run queries, which the gate reads from the installed package, and
  `TERMINATE_RUN`); a launch names a job of this location and nothing else.
- **host**, on a unix socket in a directory only root (and the container's uid) can open
  (`WRITER_SOCKET`, infra/tmpfiles/foundation-dagster-writer.conf): what the deploy and an operator's
  run-now send (scripts/deploy/job_orchestrator.py; airflow-runtime.sh through `writer_gate.py ask`).

No host port reaches the writable webserver. Plain Python: the host script, the DAG file and the gate
import it.
"""

import http.client
import json
import os
import re
import socket

# The Dagster names of this platform's code (orchestration/dagster/definitions.py), used by the DAG
# file, the deploy and the gate alike.
DAGSTER_JOB_PREFIX = "foundation_job__"
DAGSTER_SENSOR_PREFIX = "foundation_decide__"
DAGSTER_LOCATION = "foundation"
DAGSTER_REPOSITORY = "__repository__"
REPOSITORY = {"repositoryLocationName": DAGSTER_LOCATION, "repositoryName": DAGSTER_REPOSITORY}

# The host's way in, the same path in the container (compose.orchestration.yml mounts the directory).
WRITER_SOCKET = os.environ.get("FOUNDATION_DAGSTER_WRITER_SOCKET", "/run/foundation-dagster/writer.sock")
REQUEST_SECONDS = 30
# A request larger than any document here with its variables is not one of them.
MAX_BODY_BYTES = 64 * 1024

TERMINATE_RUN = """
mutation TerminateRun($runId: String!) {
  terminateRun(runId: $runId) { __typename }
}
"""
SENSORS = """
query Sensors($repository: RepositorySelector!) {
  sensorsOrError(repositorySelector: $repository) {
    __typename
    ... on Sensors { results { name sensorState { id status } } }
    ... on PythonError { message }
  }
}
"""
STOP_SENSOR = """
mutation Stop($id: String!) {
  stopSensor(id: $id) { __typename ... on PythonError { message } }
}
"""
START_SENSOR = """
mutation Start($sensor: SensorSelector!) {
  startSensor(sensorSelector: $sensor) { __typename ... on PythonError { message } }
}
"""
LAUNCH_RUN = """
mutation Launch($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message }
  }
}
"""
# The run of a job that has not started yet, if there is one (job_orchestrator.py waiting): the deploy's
# start joins it instead of queueing another. NOT_STARTED is a run created and not yet handed to the queue.
WAITING_RUN = """
query Waiting($job: String!) {
  runsOrError(filter: {pipelineName: $job, statuses: [QUEUED, NOT_STARTED]}, limit: 1) {
    __typename
    ... on Runs { results { runId } }
    ... on PythonError { message }
  }
}
"""
RELOAD_LOCATION = """
mutation Reload {
  reloadRepositoryLocation(repositoryLocationName: "foundation") { __typename }
}
"""
LOCATION_ERRORS = """
query LocationErrors {
  workspaceOrError {
    ... on Workspace { locationEntries { name locationOrLoadError { __typename ... on PythonError { message } } } }
  }
}
"""
# Dagster's own pool limits (root ADR-0605): the deploy reads them, sets each pool of the job list and deletes
# a pool the list no longer names (writer_gate.py ask pools). `limit` is the set limit, null for one Dagster
# took from `concurrency.pools.default_limit`.
POOL_LIMITS = """
query PoolLimits {
  instance { concurrencyLimits { concurrencyKey limit usingDefaultLimit } }
}
"""
SET_POOL_LIMIT = """
mutation SetPoolLimit($concurrencyKey: String!, $limit: Int!) {
  setConcurrencyLimit(concurrencyKey: $concurrencyKey, limit: $limit)
}
"""
DELETE_POOL_LIMIT = """
mutation DeletePoolLimit($concurrencyKey: String!) {
  deleteConcurrencyLimit(concurrencyKey: $concurrencyKey)
}
"""
# A pool's limit the gate lets the host set: a job list pool's slots (job_specs.declared_pools), never 0,
# which would stop every run of the pool.
MAX_POOL_LIMIT = 16

JOB_ID = re.compile(r"[a-z][a-z0-9_]*")
POOL = re.compile(r"[a-z][a-z0-9_]*")
RUN_ID = re.compile(r"[0-9a-f-]{8,64}")
# A sensor's state id as Dagster's GraphQL gives it: `<origin id>::<selector id>` (1.13.27
# dagster/_core/remote_representation/handle.py CompoundID.to_string, delimiter "::"), or one hex id.
STATE_ID = re.compile(r"[0-9a-f]{8,64}(?:::[0-9a-f]{8,64})?")


def normal(document: str) -> str:
    """A document as compared: its words, whitespace aside (the request's text, not a parse of it)."""
    return " ".join(document.split())


def _names_a_job(params) -> str | None:
    if not isinstance(params, dict):
        return "no execution parameters"
    selector = params.get("selector") or {}
    job = selector.get("pipelineName") or ""
    if {key: selector.get(key) for key in REPOSITORY} != REPOSITORY or set(selector) - {*REPOSITORY, "pipelineName"}:
        return f"a launch outside {DAGSTER_LOCATION}"
    if not (job.startswith(DAGSTER_JOB_PREFIX) and JOB_ID.fullmatch(job[len(DAGSTER_JOB_PREFIX):])):
        return f"a launch of {job[:80]!r}, which is no job's Dagster job"
    if params.get("runConfigData") not in ("{}", {}, None) or set(params) - {"mode", "executionMetadata", "runConfigData", "selector"}:
        return "a launch with run config or a subset"
    return None


def _no_variables(variables) -> str | None:
    return None if not variables else "variables for a document that takes none"


def _one(name, pattern):
    def check(variables) -> str | None:
        value = (variables or {}).get(name)
        if set(variables or {}) != {name} or not isinstance(value, str) or not pattern.fullmatch(value):
            return f"{name} is not one"
        return None
    return check


def _sensors(variables) -> str | None:
    return None if variables == {"repository": REPOSITORY} else f"sensors outside {DAGSTER_LOCATION}"


def _start(variables) -> str | None:
    sensor = (variables or {}).get("sensor") or {}
    name = sensor.get("sensorName") or ""
    if set(variables or {}) != {"sensor"} or {key: sensor.get(key) for key in REPOSITORY} != REPOSITORY \
            or set(sensor) != {*REPOSITORY, "sensorName"} \
            or not (name.startswith(DAGSTER_SENSOR_PREFIX) and JOB_ID.fullmatch(name[len(DAGSTER_SENSOR_PREFIX):])):
        return f"a sensor {name[:80]!r}, which no job decides with"
    return None


def _pool_limit(variables) -> str | None:
    key, limit = (variables or {}).get("concurrencyKey"), (variables or {}).get("limit")
    if set(variables or {}) != {"concurrencyKey", "limit"} or not isinstance(key, str) or not POOL.fullmatch(key):
        return "a pool limit without one pool name"
    if type(limit) is not int or not 1 <= limit <= MAX_POOL_LIMIT:
        return f"a pool limit outside 1..{MAX_POOL_LIMIT}"
    return None


def _a_jobs_runs(variables) -> str | None:
    job = (variables or {}).get("job")
    if set(variables or {}) != {"job"} or not isinstance(job, str) \
            or not (job.startswith(DAGSTER_JOB_PREFIX) and JOB_ID.fullmatch(job[len(DAGSTER_JOB_PREFIX):])):
        return "runs of what is no job's Dagster job"
    return None


def _launch(key):
    def check(variables) -> str | None:
        if set(variables or {}) != {key}:
            return "variables other than the execution parameters"
        return _names_a_job(variables[key])
    return check


HOST = {
    normal(SENSORS): _sensors,
    normal(STOP_SENSOR): _one("id", STATE_ID),
    normal(START_SENSOR): _start,
    normal(LAUNCH_RUN): _launch("params"),
    normal(WAITING_RUN): _a_jobs_runs,
    normal(RELOAD_LOCATION): _no_variables,
    normal(LOCATION_ERRORS): _no_variables,
    normal(POOL_LIMITS): _no_variables,
    normal(SET_POOL_LIMIT): _pool_limit,
    normal(DELETE_POOL_LIMIT): _one("concurrencyKey", POOL),
}


def airlift_queries():
    """dagster-airlift's `in_airflow/gql_queries.py` loaded as one file: the package's `__init__` refuses
    to import where Airflow is absent, as in the Dagster image the gate runs in; the file itself imports
    nothing (1.13.27)."""
    import importlib.util  # noqa: PLC0415

    package = importlib.util.find_spec("dagster_airlift")
    if package is None or not package.submodule_search_locations:
        raise ImportError("dagster-airlift is not installed beside the gate")
    path = os.path.join(next(iter(package.submodule_search_locations)), "in_airflow", "gql_queries.py")
    spec = importlib.util.spec_from_file_location("dagster_airlift_gql_queries", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def airflow_allowed() -> dict:
    """What a proxied `run` task sends: dagster-airlift's own documents, as installed beside the gate (the
    Dagster image and the Airflow image pin the same version), and the terminate the DAG file adds."""
    q = airlift_queries()
    return {
        normal(q.VERIFICATION_QUERY): _no_variables,
        normal(q.TRIGGER_ASSETS_MUTATION): _launch("executionParams"),
        normal(q.RUNS_QUERY): _one("runId", RUN_ID),
        normal(TERMINATE_RUN): _one("runId", RUN_ID),
    }


def refused(allowed: dict, method: str, path: str, body: bytes, ready: bool = False) -> str | None:
    """Why a request is not one `allowed` (document -> check of its variables) holds, or None. With
    `ready`, `GET /server_info` is allowed too (the deploy waits on it)."""
    if ready and method == "GET" and path == "/server_info":
        return None
    if method != "POST" or path != "/graphql":
        return f"{method} {path[:80]} is not a GraphQL request"
    if len(body) > MAX_BODY_BYTES:
        return "a request larger than any allowed document"
    try:
        request = json.loads(body)
    except ValueError:
        return "a body that is not JSON"
    if not isinstance(request, dict) or not isinstance(request.get("query"), str):
        return "no single query"
    if set(request) - {"query", "variables", "operationName"}:
        return "fields other than query, variables and operationName"
    check = allowed.get(normal(request["query"]))
    if check is None:
        return "a document this caller may not send"
    variables = request.get("variables")
    if variables is not None and not isinstance(variables, dict):
        return "variables that are not an object"
    return check(variables)


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def ask(method: str, path: str, payload: dict | None = None, socket_path: str | None = None,
        timeout: float = REQUEST_SECONDS) -> tuple[int, bytes]:
    """One request over the writer socket: (status, body). OSError when nothing answers."""
    connection = _UnixConnection(socket_path or WRITER_SOCKET, timeout)
    try:
        body = None if payload is None else json.dumps(payload).encode()
        connection.request(method, path, body=body, headers={"Content-Type": "application/json"})
        response = connection.getresponse()
        return response.status, response.read()
    except http.client.HTTPException as error:
        raise OSError(f"Dagster's writer socket answered no HTTP: {error}") from None
    finally:
        connection.close()
