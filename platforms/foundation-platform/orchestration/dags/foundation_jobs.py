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

A job that names a `check_service` runs it first, as task `check` in Airflow's default_pool: when it
says the run has nothing to do, `run` is skipped before it waits for the job's pool (root ADR-0277).

The asset reconciler (a job that names `reconciles`, root ADR-0333 §7) runs like any job; what its run
says each reconciled job is owed (`foundation-asset-owed <job> <key>`) goes to task `select`, which
keeps the jobs with no run queued or running, and task `start` triggers each once per key: the run id
is the key, so a repeated tick is a no-op. A triggered run waits for its pool like any other.
"""

import pendulum
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Asset, AssetAny, AssetOrTimeSchedule, BaseOperator, CronTriggerTimetable
from airflow.sdk.exceptions import AirflowSkipException
from openlineage.client.event_v2 import Dataset

from job_specs import (
    DEFAULT_POOL,
    RETRY_DELAY_MINUTES,
    RUN_TASK_ID,
    WITHHOLDS_EVENTS,
    asset_uri,
    check_says_unchanged,
    job_outcome,
    load_specs,
    owed_runs,
)

START = pendulum.datetime(2026, 10, 1, tz="UTC")
# Defined by airflow-runtime.sh as AIRFLOW_CONN_FOUNDATION_HOST: the host, the scheduler account,
# its private key and the host's own key, so the connection is verified both ways.
CONNECTION = "foundation_host"
# The XCom `run` leaves for `publish_outputs`: one word, never the journal.
OUTCOME_KEY = "outcome"
# What the host's forced command reads after the job id, rendered by Airflow per run: nothing for a
# clock start, ` triggered` for any other, ` requested <id>` for an operator's run-now. The id is
# whatever the conf says; the host accepts only a request of its own (root ADR-0282 §4).
RUN_KIND = (
    "{{ ' requested ' ~ dag_run.conf['operator_request'] "
    "if dag_run.conf and dag_run.conf.get('operator_request') "
    "else ('' if dag_run.run_type == 'scheduled' else ' triggered') }}"
)
# A job's check (jobs.v1.json `check_service`, root ADR-0277) and its bound: the slowest lane's
# took 4.2 s (2026-10-14..18), the unit's own TimeoutStartSec is 5 minutes.
CHECK_TASK_ID = "check"
CHECK_TIMEOUT_MINUTES = 6
# The reconciler's XCom from `run` to `select`: [[job id, key]], never the journal (root ADR-0333 §7).
OWED_KEY = "owed"
# A run of a reconciled job that is queued or running already covers what it is owed.
ACTIVE_STATES = ("queued", "running")


class RunJob(SSHOperator):
    """The SSH run of a job, keeping only what its last lines say it changed, and for the asset
    reconciler the runs it says the jobs it reconciles are owed."""

    def __init__(self, *, reconciles=(), **kwargs):
        super().__init__(**kwargs)
        self.reconciles = tuple(reconciles)

    def run_ssh_client_command(self, ssh_client, command, context=None):
        output = super().run_ssh_client_command(ssh_client, command, context=context)
        outcome = job_outcome(output)
        context["ti"].xcom_push(key=OUTCOME_KEY, value=outcome)
        if self.reconciles:
            context["ti"].xcom_push(key=OWED_KEY, value=[list(owed) for owed in owed_runs(output, self.reconciles)])
        if outcome in ("waiting", "skipped"):
            # Not a failure and not a success: skipped, which the data flow map shows as waiting with
            # the reason from this log (root ADR-0260); a paused job or an unusable run-now is
            # skipped too (root ADR-0282). publish_outputs is skipped with it.
            raise AirflowSkipException(f"the run ended {outcome}; its last foundation-job-outcome line says why")
        return b""


class CheckJob(SSHOperator):
    """Asks the job's check service, without a pool slot, whether the run has work (root ADR-0277).

    `unchanged` skips the run, and with it publish_outputs. Anything else, a failed check included,
    lets the run start: the run decides again for itself, so the check only saves its pool wait.
    """

    def execute(self, context):
        try:
            super().execute(context)
            return None  # the answer is the task's state, not an XCom
        except AirflowSkipException:
            raise
        except Exception as error:  # noqa: BLE001 - a check that cannot answer must not stop the run
            self.log.warning("the check did not answer (%s); the run decides for itself", error)
            return None

    def run_ssh_client_command(self, ssh_client, command, context=None):
        output = super().run_ssh_client_command(ssh_client, command, context=context)
        if check_says_unchanged(output.decode("utf-8", "replace")):
            raise AirflowSkipException("the check found nothing to do: the run is skipped and takes no pool slot")
        return b""


class PublishOutputs(BaseOperator):
    """Succeeds, and so records an event on each output asset, unless the run did not change them."""

    def execute(self, context):
        outcome = context["ti"].xcom_pull(task_ids=RUN_TASK_ID, key=OUTCOME_KEY)
        if outcome in WITHHOLDS_EVENTS:
            raise AirflowSkipException(f"the run reported {outcome}: no asset events, nothing downstream starts")
        self.log.info("the run reported %s: recording events on %d output assets", outcome or "nothing", len(self.outlets))


class SelectOwed(BaseOperator):
    """The reconciler's owed runs whose job has no run queued or running, as TriggerDagRunOperator
    arguments; the run id is the key, so the same versions never start a job twice (root ADR-0333 §7).

    Asked through the task SDK (RuntimeTaskInstance.get_dr_count), one count per owed job: the tick
    reads no table of its own. Verified on apache/airflow:3.3.2 (ADR-0333 test (e)).
    """

    def __init__(self, *, dag_id_prefix, **kwargs):
        super().__init__(**kwargs)
        self.dag_id_prefix = dag_id_prefix

    def execute(self, context):
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        owed = context["ti"].xcom_pull(task_ids=RUN_TASK_ID, key=OWED_KEY) or []
        starts = []
        for job, key in owed:
            dag_id = self.dag_id_prefix + job
            active = RuntimeTaskInstance.get_dr_count(dag_id=dag_id, states=list(ACTIVE_STATES))
            self.log.info("%s owes versions %s; %d runs queued or running", job, key, active)
            if not active:
                starts.append({"trigger_dag_id": dag_id, "trigger_run_id": f"reconcile__{job}__{key}"})
        return starts


def asset(dataset):
    return Asset(uri=asset_uri(dataset), name=asset_uri(dataset))


def schedule_of(spec):
    if spec.started_by == "schedule":
        return spec.schedule
    return AssetOrTimeSchedule(
        timetable=CronTriggerTimetable(spec.schedule, timezone="UTC"),
        assets=AssetAny(*(asset(dataset) for dataset in spec.starting_inputs)),
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
            task_id=RUN_TASK_ID,
            ssh_conn_id=CONNECTION,
            # The host's forced command reads this as the job id, and `triggered` when the run was
            # not started by the clock: a run its inputs started, or one an operator triggered, is
            # never deferred by takes_turns (root ADR-0179); `requested <id>` when an operator's
            # run-now started it (conf `operator_request`, root ADR-0282 §4), which the host checks
            # and uses once. Rendered by Airflow per run.
            command=spec.job_id + RUN_KIND,
            reconciles=spec.reconciles,
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
        if spec.reconciles:
            select = SelectOwed(task_id="select", dag_id_prefix=spec.dag_id.removesuffix(spec.job_id), retries=0)
            # Each owed job once per key; a run id that exists already (this key was started) is skipped.
            start = TriggerDagRunOperator.partial(task_id="start", skip_when_already_exists=True,
                                                  wait_for_completion=False, retries=0).expand_kwargs(select.output)
            for task in (select, start):
                extend_global_openlineage_emission_policy(task, emit_task_events=False)
            run >> select
        if spec.check_service:
            check = CheckJob(
                task_id=CHECK_TASK_ID,
                ssh_conn_id=CONNECTION,
                command=f"{spec.job_id} check",
                cmd_timeout=CHECK_TIMEOUT_MINUTES * 60,
                do_xcom_push=False,
                # Airflow's own pool, never the job's: the point is to decide before taking it.
                pool=DEFAULT_POOL,
                retries=0,
                execution_timeout=pendulum.duration(minutes=CHECK_TIMEOUT_MINUTES),
            )
            # Reports nothing to the data catalog; the run reports the job (root ADR-0165).
            extend_global_openlineage_emission_policy(check, emit_task_events=False)
            check >> run
    globals()[spec.dag_id] = dag
