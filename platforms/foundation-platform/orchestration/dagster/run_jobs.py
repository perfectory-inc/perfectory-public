"""The jobs Dagster runs once they are past `shadow` (root ADR-0431 §5): one Dagster job per job.

A job at `dagster_stage: proxied` is still decided by Airflow; its DAG's `run` task launches the Dagster
job named `job_specs.DAGSTER_JOB_PREFIX + <job id>` through Dagster's webserver (foundation_jobs.py,
dagster-airlift's proxied task) and waits for it. The Dagster job is one op that starts the job's systemd
unit through the host's one forced command, as Airflow's SSHOperator did (root ADR-0417, ADR-0122 §2), and
logs a materialization of each of the job's outputs when the run changed them (SystemdRun.materializations).

Why a job and not an asset per output: the collectors and their Silver refreshes write the same Silver
tables, and five jobs write no dataset at all; an asset key has one definition, a job may log any of
them. The outputs stay the observed AssetSpecs of observe_assets.py, so their history is one line
whichever orchestrator ran the job; observe_assets maps a proxied job's DAG to none of them, since its
Dagster run logs them itself.

The run answers Airflow through its own tags (job_specs.OUTCOME_TAG, OWED_TAG), set before the op ends,
failed or not, so the `run` task pushes the same XComs `publish_outputs` and the reconciler's `select`
read today. How it was started (a clock start, `triggered`, `requested <id>`) comes in as RUN_KIND_TAG;
a run launched without it (from Dagster's UI) is an operator's, so `triggered`.

The host connection is the scheduler's (FOUNDATION_HOST_CONNECTION, the same runtime.env entry Airflow's
AIRFLOW_CONN_FOUNDATION_HOST is, compose.orchestration.yml): its key can do nothing but the forced
command, and only from the orchestration project's network (foundation-release.sh `timers`); the daemon
reaches the host from that network (`gw_priority`). paramiko is the client Airflow's SSH provider uses,
so the host key is pinned the same way and the private key never touches a disk.
"""

import base64
import io
import json
import os
import socket

import observe_assets
import systemd_pipes

job_specs = observe_assets.job_specs

# Airflow's connection to the host, as airflow-runtime.sh `init-secrets` writes it into runtime.env.
CONNECTION_ENV = "FOUNDATION_HOST_CONNECTION"
CONNECT_SECONDS = 60
# A run streams its journal for hours; a keepalive keeps the idle stretches of it open.
KEEPALIVE_SECONDS = 30


class ForcedCommand:
    """The host's forced command over SSH: `start(request)` sends the request (the job id and its kind,
    nothing more) and returns the run's output lines and a function that waits for its exit status.

    `connection` is the Airflow SSH connection (conn_type ssh: host, port, login, extra.private_key and
    extra.host_key); a host key that differs from the pinned one is refused, never learnt. A run that
    prints nothing for `idle_seconds` fails, as SSHOperator's cmd_timeout does; its unit is systemd's
    and runs on (start-scheduled-job.sh), and the next start joins it.
    """

    def __init__(self, connection, idle_seconds):
        self.connection, self.idle_seconds = connection, idle_seconds

    @classmethod
    def from_environment(cls, idle_seconds):
        try:
            return cls(json.loads(os.environ[CONNECTION_ENV]), idle_seconds)
        except (KeyError, ValueError) as error:
            raise systemd_pipes.Failure(f"{CONNECTION_ENV} does not hold the host connection: {type(error).__name__}") from None

    def start(self, request):
        import paramiko

        extra = self.connection.get("extra") or {}
        if isinstance(extra, str):
            extra = json.loads(extra)
        host, port = self.connection["host"], int(self.connection.get("port") or 22)
        known = host if port == 22 else f"[{host}]:{port}"
        key_type, key_data = extra["host_key"].split()[:2]
        client = paramiko.SSHClient()
        client.get_host_keys().add(known, key_type, paramiko.PKey.from_type_string(key_type, base64.b64decode(key_data)))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(host, port=port, username=self.connection["login"],
                       pkey=paramiko.Ed25519Key.from_private_key(io.StringIO(extra["private_key"])),
                       allow_agent=False, look_for_keys=False, timeout=CONNECT_SECONDS)
        transport = client.get_transport()
        transport.set_keepalive(KEEPALIVE_SECONDS)
        channel = transport.open_session()
        channel.set_combine_stderr(True)
        channel.settimeout(self.idle_seconds)
        channel.exec_command(request)
        stream = channel.makefile("rb")

        def lines():
            try:
                for line in stream:
                    yield line.decode("utf-8", "replace")
            except socket.timeout:
                client.close()
                raise systemd_pipes.Failure(f"the run printed nothing for {self.idle_seconds} s; its unit runs on") from None

        def wait():
            try:
                return channel.recv_exit_status()
            finally:
                client.close()

        return lines(), wait


def run_job(spec, transport=None):
    """The Dagster job of one job: one op, the unit through the forced command (`transport`, or the
    host's from the environment)."""
    from dagster import Failure, OpExecutionContext, job, op

    keys = [observe_assets.asset_key(dataset) for dataset in spec.outputs]

    @op(name="run", description=f"Runs systemd service {spec.systemd_service} through the host's forced command.")
    def run(context: OpExecutionContext):
        client = systemd_pipes.SystemdUnitPipesClient(
            transport=transport or ForcedCommand.from_environment(spec.timeout_minutes * 60))
        kind = context.run.tags.get(job_specs.RUN_KIND_TAG, "triggered")
        try:
            result = client.run(context=context, job_id=spec.job_id, kind=kind)
        except Failure:
            context.instance.add_run_tags(context.run_id, {job_specs.OUTCOME_TAG: "failed"})
            raise
        tags = {job_specs.OUTCOME_TAG: result.outcome}
        if spec.reconciles:
            tags[job_specs.OWED_TAG] = json.dumps([list(owed) for owed in result.owed])
        context.instance.add_run_tags(context.run_id, tags)
        for materialization in result.materializations(keys):
            context.log_event(materialization)

    def runs_the_unit():
        run()

    # A run launched from Dagster's UI queues like Airflow's: the same pool, priority and job count.
    return job(name=spec.dagster_job, tags={key: value for key, value in spec.dagster_tags().items()
                                             if key != job_specs.RUN_KIND_TAG},
               description=spec.description)(runs_the_unit)


def run_jobs(jobs=None, graph=None, transport=None):
    """The Dagster jobs of every job Dagster runs: those at `proxied` (and, from its slice, `dagster`)."""
    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    return [run_job(spec, transport) for spec in job_specs.load_specs(jobs, graph)
            if spec.dagster_stage in job_specs.DAGSTER_RUNS]
