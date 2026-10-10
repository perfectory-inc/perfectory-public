"""One Airflow DAG per scheduled job in orchestration/jobs.v1.json (root ADR-0118, ADR-0122, ADR-0171).

Each DAG asks the host, over the scheduler's restricted SSH key, to start the job's systemd service,
and streams its journal until systemd says how it ended (task `run`). The host decides what may
run; the key can do nothing else. Inputs and outputs, from the pipeline graph, go to the data
catalog as the run's OpenLineage datasets.

A job's outputs are also Airflow assets, and a job started by its inputs (`started_by: inputs`) is
scheduled on them: it runs when another job's run changed one of them, and at its schedule as the
fallback. The run's last `foundation-job-outcome` line says whether it changed its outputs; task
`publish_outputs` then records the output asset events, or is skipped when the run changed nothing,
so nothing downstream starts.
"""

import pendulum
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG, Asset, AssetAny, AssetOrTimeSchedule, BaseOperator, CronTriggerTimetable
from airflow.sdk.exceptions import AirflowSkipException
from openlineage.client.event_v2 import Dataset

from job_specs import RETRY_DELAY_MINUTES, asset_uri, job_outcome, load_specs

START = pendulum.datetime(2026, 10, 1, tz="UTC")
# Defined by airflow-runtime.sh as AIRFLOW_CONN_FOUNDATION_HOST: the host, the scheduler account,
# its private key and the host's own key, so the connection is verified both ways.
CONNECTION = "foundation_host"
# The XCom `run` leaves for `publish_outputs`: one word, never the journal.
OUTCOME_KEY = "outcome"


class RunJob(SSHOperator):
    """The SSH run of a job, keeping only what its last lines say it changed."""

    def run_ssh_client_command(self, ssh_client, command, context=None):
        output = super().run_ssh_client_command(ssh_client, command, context=context)
        context["ti"].xcom_push(key=OUTCOME_KEY, value=job_outcome(output))
        return b""


class PublishOutputs(BaseOperator):
    """Succeeds, and so records an event on each output asset, unless the run changed nothing."""

    def execute(self, context):
        outcome = context["ti"].xcom_pull(task_ids="run", key=OUTCOME_KEY)
        if outcome == "unchanged":
            raise AirflowSkipException("the run changed none of its outputs: no asset events, nothing downstream starts")
        self.log.info("the run reported %s: recording events on %d output assets", outcome or "nothing", len(self.outlets))


def asset(dataset):
    return Asset(uri=asset_uri(dataset), name=asset_uri(dataset))


def schedule_of(spec):
    if spec.started_by == "schedule":
        return spec.schedule
    return AssetOrTimeSchedule(
        timetable=CronTriggerTimetable(spec.schedule, timezone="UTC"),
        assets=AssetAny(*(asset(dataset) for dataset in spec.inputs)),
    )


for spec in load_specs():
    started = (
        f"Starts at `{spec.schedule}` (UTC)."
        if spec.started_by == "schedule"
        else f"Starts when {', '.join(spec.producers)} changed one of its inputs, and at `{spec.schedule}` (UTC) as the fallback."
    )
    with DAG(
        dag_id=spec.dag_id,
        description=spec.description,
        schedule=schedule_of(spec),
        start_date=START,
        catchup=False,
        max_active_runs=1,
        tags=["foundation", "scheduled-job"],
        doc_md=f"{spec.description}\n\nRuns systemd service `{spec.systemd_service}` on the host. {started}",
    ) as dag:
        run = RunJob(
            task_id="run",
            ssh_conn_id=CONNECTION,
            # The host's forced command reads this as the job id, and `triggered` when the run was
            # not started by the clock: a run its inputs started, or one an operator triggered, is
            # never deferred by takes_turns (root ADR-0179). Rendered by Airflow per run.
            command=spec.job_id + "{{ '' if dag_run.run_type == 'scheduled' else ' triggered' }}",
            # Seconds without output before the run counts as hung; the journal prints as it goes.
            cmd_timeout=spec.timeout_minutes * 60,
            # The journal is not kept as an XCom; the outcome is (RunJob).
            do_xcom_push=False,
            pool=spec.pool,
            pool_slots=spec.pool_slots,
            # Airflow starts the heaviest waiting task that fits when slots free; absolute, so the
            # weight is the job's own (root ADR-0138).
            priority_weight=spec.priority_weight,
            weight_rule="absolute",
            retries=spec.retries,
            retry_delay=pendulum.duration(minutes=RETRY_DELAY_MINUTES),
            execution_timeout=pendulum.duration(minutes=spec.timeout_minutes),
            inlets=[Dataset(namespace=namespace, name=name) for namespace, name in spec.inputs],
            outlets=[Dataset(namespace=namespace, name=name) for namespace, name in spec.outputs],
        )
        if spec.outputs:
            publish = PublishOutputs(
                task_id="publish_outputs",
                outlets=[asset(dataset) for dataset in spec.outputs],
                retries=0,
            )
            # The data catalog hears of the run from `run` and the DAG run, as before root ADR-0171;
            # this bookkeeping step reports nothing of its own (Dawneer reads the latest run, ADR-0165).
            extend_global_openlineage_emission_policy(publish, emit_task_events=False)
            run >> publish
    globals()[spec.dag_id] = dag
