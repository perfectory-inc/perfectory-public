"""dagster-airlift's Airflow client, pointed at Airflow 3's REST API (root ADR-0333 stage 0).

dagster-airlift 0.29.27 (the newest, 2026-10) speaks Airflow 2's `/api/v1` with basic auth; its own
test fixtures skip Airflow 3 ("replaced the basic-auth REST API with JWT ... not yet target[ed]",
dagster_airlift/test/shared_fixtures.py). Production runs Airflow 3.3.2, which serves `/api/v2` and
takes a bearer token from `POST /auth/token`. The request and response shapes airlift uses are the
same in v2 (checked against 3.3.2's own v2-rest-api-generated.yaml: DAGRunsBatchBody,
TaskInstancesBatchBody, DAGRunResponse, TaskInstanceResponse), with one difference: a run that an
asset event or an operator started has no `logical_date` in Airflow 3 (19 of 177 runs on
2026-10-28), and airlift reads it. `run_after` is when such a run was due, so it stands in.

This is airlift's documented extension point (`AirflowAuthBackend`, as `MwaaSessionAuthBackend` is),
not a fork: when airlift targets Airflow 3 itself, this file is deleted.
"""

import threading
import time

import requests
from dagster_airlift.core import AirflowAuthBackend, AirflowInstance
from dagster_airlift.core.runtime_representations import DagRun, TaskInstance

# Airflow 3's token lives `[api_auth] jwt_expiration_time` (default 86400 s); a fresh one well before.
TOKEN_SECONDS = 600
REQUEST_SECONDS = 60


class AirflowTokenAuthBackend(AirflowAuthBackend):
    """A session that carries a bearer token from Airflow 3's `POST /auth/token` (the FAB auth manager's)."""

    def __init__(self, webserver_url: str, username: str, password: str):
        self._webserver_url = webserver_url.rstrip("/")
        self._username = username
        self._password = password
        self._lock = threading.Lock()
        self._token = None
        self._token_at = 0.0

    def _bearer(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() - self._token_at > TOKEN_SECONDS:
                response = requests.post(
                    f"{self._webserver_url}/auth/token",
                    json={"username": self._username, "password": self._password},
                    timeout=REQUEST_SECONDS,
                )
                response.raise_for_status()
                self._token = response.json()["access_token"]
                self._token_at = time.monotonic()
            return self._token

    def get_session(self) -> requests.Session:
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {self._bearer()}"
        return session

    def get_webserver_url(self) -> str:
        return self._webserver_url


def _with_logical_date(metadata: dict) -> dict:
    """A run or task instance record with every time airlift reads set.

    `logical_date` is Airflow 3's `run_after` when the run has none. A run or task an operator marks
    success before it ever started has no `start_date` (and a task, no `end_date`): airlift parses
    both, and one such run stopped every sensor tick at the same cursor (2026-10-29, a run marked
    success through the API against Airflow 3.3.2). The other time of the pair stands in.
    """
    found = {**metadata}
    found["logical_date"] = found.get("logical_date") or found["run_after"]
    found["start_date"] = found.get("start_date") or found.get("end_date") or found["run_after"]
    found["end_date"] = found.get("end_date") or found["start_date"]
    return found


class Airflow3Instance(AirflowInstance):
    """AirflowInstance on `/api/v2`, with every run and task instance carrying a logical date."""

    def get_api_url(self) -> str:
        return f"{self.auth_backend.get_webserver_url()}/api/v2"

    def get_dag_runs_batch(self, *args, **kwargs):
        runs, total = super().get_dag_runs_batch(*args, **kwargs)
        return [
            DagRun(webserver_url=run.webserver_url, dag_id=run.dag_id, run_id=run.run_id,
                   metadata=_with_logical_date(run.metadata))
            for run in runs
        ], total

    def _dated(self, instances):
        return [
            TaskInstance(webserver_url=ti.webserver_url, dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id,
                         metadata=_with_logical_date(ti.metadata))
            for ti in instances
        ]

    def get_task_instance_batch(self, *args, **kwargs):
        return self._dated(super().get_task_instance_batch(*args, **kwargs))

    def get_task_instance_batch_time_range(self, *args, **kwargs):
        return self._dated(super().get_task_instance_batch_time_range(*args, **kwargs))
