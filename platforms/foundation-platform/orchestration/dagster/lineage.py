"""The OpenLineage events of a run Dagster decides (root ADR-0565 §7), as Airflow sends them for a DAG.

Airflow's OpenLineage provider reports each DAG run as job `foundation_<id>` and its `run` task as job
`foundation_<id>.run`, the task naming the DAG run as its parent, in namespace `perfectory`, with the job's
inputs and outputs on the task (foundation_jobs.py inlets/outlets). DataHub makes the DAG a DataFlow of
that id and both jobs its children; the operations map reads a job's runs from them (foundation-api
operations.rs). A job at `dagster_stage: dagster` has no DAG, so the op that runs its unit sends the same
two jobs with the standard client (`openlineage-python`, the version Airflow's provider holds):

- the run (`foundation_<id>`): START when the op's first try starts, timed when the run was queued, as a
  DAG run starts when it is created and its task then waits for the pool; COMPLETE or FAIL when the op's
  last try ends. Its run id is the Dagster run's id.
- each try of the op (`foundation_<id>.run`): START and COMPLETE or FAIL, its parent the run, carrying
  the job's inputs and outputs. A run with nothing to do, or one the host held, completes, as Airflow's
  skipped task does.

The processing engine is the one that ran it (`Dagster`), so DataHub's flow is
`urn:li:dataFlow:(dagster,foundation_<id>,perfectory)`: same flow id, same job names, beside Airflow's flow
of the same id; operations.rs joins a job's runs by flow id and job name.

Sending never fails a run: the catalog is told of runs, it does not decide them. A missed event is logged
and the next run reports again.
"""

import json
import os
import uuid
from datetime import datetime, timezone

import observe_assets

job_specs = observe_assets.job_specs

# The same transport Airflow's provider is given (compose.orchestration.yml, one YAML anchor for both).
TRANSPORT_ENV = "FOUNDATION_OPENLINEAGE_TRANSPORT"
NAMESPACE = "perfectory"
ENGINE = "Dagster"
PRODUCER = "https://github.com/perfectory-inc/perfectory-public/tree/main/platforms/foundation-platform/orchestration/dagster"


def _version(package):
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(package)
    except PackageNotFoundError:
        return "unknown"


def try_run_id(run_id, retry_number):
    """The OpenLineage run id of one try of the op: fixed by the Dagster run and the try, so a resent
    event names the same run."""
    return str(uuid.uuid5(uuid.UUID(run_id), f"run.{retry_number}"))


def _at(timestamp=None):
    moment = datetime.now(timezone.utc) if timestamp is None else datetime.fromtimestamp(timestamp, timezone.utc)
    return moment.isoformat()


class Lineage:
    """The events of one Dagster run of `spec`; `client` is an OpenLineage client (the environment's
    transport when None), `log` where a send that failed is told."""

    def __init__(self, spec, run_id, log, client=None):
        self.spec, self.run_id, self.log = spec, run_id, log
        self.client = client

    @classmethod
    def from_environment(cls, spec, run_id, log):
        """The run's lineage, or None when the environment names no transport (a test instance)."""
        config = os.environ.get(TRANSPORT_ENV)
        if not config:
            return None
        from openlineage.client import OpenLineageClient

        return cls(spec, run_id, log, OpenLineageClient(config={"transport": json.loads(config)}))

    def _facets(self):
        from openlineage.client.facet_v2 import processing_engine_run

        return {"processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            version=_version("dagster"), name=ENGINE, openlineageAdapterVersion=_version("openlineage-python"),
            producer=PRODUCER)}

    def _job(self, name, job_type):
        from openlineage.client.event_v2 import Job
        from openlineage.client.facet_v2 import job_type_job

        return Job(namespace=NAMESPACE, name=name, facets={"jobType": job_type_job.JobTypeJobFacet(
            processingType="BATCH", integration="DAGSTER", jobType=job_type, producer=PRODUCER)})

    def _send(self, event):
        try:
            self.client.emit(event)
        except Exception as error:  # noqa: BLE001 - the catalog never decides a run
            self.log.warning(f"the data catalog was not told of {event.job.name} {event.eventType}: {error}")

    def run_event(self, state, at=None):
        """The run's own event (job `foundation_<id>`)."""
        from openlineage.client.event_v2 import Run, RunEvent

        return RunEvent(eventType=state, eventTime=_at(at), producer=PRODUCER,
                        run=Run(runId=self.run_id, facets=self._facets()),
                        job=self._job(self.spec.dag_id, "DAG"), inputs=[], outputs=[])

    def try_event(self, state, retry_number):
        """One try's event (job `foundation_<id>.run`), its parent the run, with the job's datasets."""
        from openlineage.client.event_v2 import InputDataset, OutputDataset, Run, RunEvent
        from openlineage.client.facet_v2 import parent_run

        facets = {**self._facets(), "parent": parent_run.ParentRunFacet(
            run=parent_run.Run(runId=self.run_id), job=parent_run.Job(namespace=NAMESPACE, name=self.spec.dag_id),
            producer=PRODUCER)}
        return RunEvent(eventType=state, eventTime=_at(), producer=PRODUCER,
                        run=Run(runId=try_run_id(self.run_id, retry_number), facets=facets),
                        job=self._job(f"{self.spec.dag_id}.{job_specs.RUN_TASK_ID}", "TASK"),
                        inputs=[InputDataset(namespace=namespace, name=name) for namespace, name in self.spec.inputs],
                        outputs=[OutputDataset(namespace=namespace, name=name) for namespace, name in self.spec.outputs])

    def started(self, retry_number, queued_at):
        """The op's try `retry_number` started; the first also starts the run, at `queued_at`."""
        from openlineage.client.event_v2 import RunState

        if retry_number == 0:
            self._send(self.run_event(RunState.START, queued_at))
        self._send(self.try_event(RunState.START, retry_number))

    def ended(self, retry_number, succeeded, last_try):
        """The op's try ended; the run ends with it when it succeeded or no try is left."""
        from openlineage.client.event_v2 import RunState

        state = RunState.COMPLETE if succeeded else RunState.FAIL
        self._send(self.try_event(state, retry_number))
        if succeeded or last_try:
            self._send(self.run_event(state))
