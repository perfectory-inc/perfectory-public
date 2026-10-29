"""Foundation's Dagster code location, stage 0 of root ADR-0333: observe the Airflow that runs the jobs.

Following dagster-airlift's migration guide (peer -> observe -> migrate): the assets are generated
from jobs.v1.json and the pipeline graph (observe_assets.py), each mapped to the Airflow task that
records it, and airlift's sensor turns every successful mapped task into a materialization. Airflow
keeps running everything; this location starts no job. The unit-price checks report only.

Settings (compose.orchestration.yml, profile `dagster-observe`):
  FOUNDATION_DAGSTER_AIRFLOW_URL       Airflow's API, as the scheduler's API base (http://127.0.0.1:19080)
  FOUNDATION_DAGSTER_AIRFLOW_USERNAME  a Viewer account (airflow-runtime.sh makes it)
  FOUNDATION_DAGSTER_AIRFLOW_PASSWORD
  FOUNDATION_DAGSTER_TRINO_URL         the lakehouse query engine on the host's loopback
"""

import os
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from dagster import Definitions, definitions, in_process_executor  # noqa: E402
from dagster_airlift.core import build_defs_from_airflow_instance  # noqa: E402
from dagster_airlift.core.filter import AirflowFilter  # noqa: E402

import observe_assets  # noqa: E402
import unit_price_checks  # noqa: E402
from airflow3 import Airflow3Instance, AirflowTokenAuthBackend  # noqa: E402

# The prefix of every asset key airlift makes for a DAG itself (`foundation_airflow/dag/<dag id>`).
INSTANCE_NAME = "foundation_airflow"


def airflow_instance():
    return Airflow3Instance(
        auth_backend=AirflowTokenAuthBackend(
            webserver_url=os.environ.get("FOUNDATION_DAGSTER_AIRFLOW_URL", "http://127.0.0.1:19080"),
            username=os.environ["FOUNDATION_DAGSTER_AIRFLOW_USERNAME"],
            password=os.environ["FOUNDATION_DAGSTER_AIRFLOW_PASSWORD"],
        ),
        name=INSTANCE_NAME,
    )


def build(instance, jobs=None, graph=None):
    """The observe definitions over `instance` (an Airflow3Instance, or a test double of one)."""
    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    dags = observe_assets.dag_ids(jobs, graph)
    prefix = jobs["dag_id_prefix"]
    return build_defs_from_airflow_instance(
        airflow_instance=instance,
        defs=Definitions(
            assets=observe_assets.observed_assets(jobs, graph),
            asset_checks=unit_price_checks.CHECKS,
            # A check run's steps run in its own process, not one more process per step: the
            # multiprocess default took the daemon container from 257 MiB to a 664 MiB peak
            # (measured 2026-10-29 against Airflow 3.3.2 with these definitions).
            executor=in_process_executor,
        ),
        # Only the jobs' DAGs; Airflow's other DAGs (none today) are not this contract's.
        dag_selector_fn=lambda dag: dag.dag_id in dags,
        # Airflow's asset list is the same graph read back (foundation_jobs.py makes it from these
        # contracts), and its v1 endpoint is gone in Airflow 3; the source would be 39 more calls per load.
        retrieval_filter=AirflowFilter(dag_id_ilike=f"{prefix}%", retrieve_datasets=False),
        source_code_retrieval_enabled=False,
    )


@definitions
def defs():
    # Built when the code location loads, not on import: airlift reads Airflow's DAGs then.
    return build(airflow_instance())
