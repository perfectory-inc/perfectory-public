"""One Airflow DAG per scheduled job in orchestration/jobs.v1.json (root ADR-0118, ADR-0122).

Each DAG has one task: ask the host, over the scheduler's restricted SSH key, to start the job's
systemd service, and stream its journal until systemd says how it ended. The host decides what may
run; the key can do nothing else. Inputs and outputs, from the pipeline graph, go to the data
catalog as the run's OpenLineage datasets.
"""

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG
from openlineage.client.event_v2 import Dataset

from job_specs import load_specs

START = pendulum.datetime(2026, 10, 1, tz="UTC")
# Defined by airflow-runtime.sh as AIRFLOW_CONN_FOUNDATION_HOST: the host, the scheduler account,
# its private key and the host's own key, so the connection is verified both ways.
CONNECTION = "foundation_host"

for spec in load_specs():
    with DAG(
        dag_id=spec.dag_id,
        description=spec.description,
        schedule=spec.schedule,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        tags=["foundation", "scheduled-job"],
        doc_md=f"{spec.description}\n\nRuns systemd service `{spec.systemd_service}` on the host.",
    ) as dag:
        SSHOperator(
            task_id="run",
            ssh_conn_id=CONNECTION,
            # The host's forced command reads this as the job id and ignores anything else.
            command=spec.job_id,
            # Seconds without output before the run counts as hung; the journal prints as it goes.
            cmd_timeout=spec.timeout_minutes * 60,
            pool=spec.pool,
            retries=1,
            retry_delay=pendulum.duration(minutes=5),
            execution_timeout=pendulum.duration(minutes=spec.timeout_minutes),
            inlets=[Dataset(namespace=namespace, name=name) for namespace, name in spec.inputs],
            outlets=[Dataset(namespace=namespace, name=name) for namespace, name in spec.outputs],
        )
    globals()[spec.dag_id] = dag
