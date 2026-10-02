"""What each scheduled job is, built from the files that own it (root ADR-0122 §3).

Pure Python with no Airflow import, so the code the DAGs run is the code the tests check:

- the job list:        orchestration/jobs.v1.json
- how a job runs:      its systemd service in infra/systemd (only checked to exist here)
- inputs and outputs:  the endpoints of the pipeline-graph edges a job names
                       (docs/catalog/pipeline-graph.v1.json), named by infra/datahub/dataset_names.py
"""

import json
import pathlib
import re
import sys
from dataclasses import dataclass

PLATFORM_ROOT = pathlib.Path(__file__).resolve().parents[2]
JOBS = PLATFORM_ROOT / "orchestration" / "jobs.v1.json"
GRAPH = PLATFORM_ROOT / "docs" / "catalog" / "pipeline-graph.v1.json"
SYSTEMD = PLATFORM_ROOT / "infra" / "systemd"
CONTRACTS = PLATFORM_ROOT / "contracts" / "data"
# Where a unit's ExecStart finds the release on the host.
RELEASE_PREFIX = "/opt/foundation-platform/current/"
JOBS_SCHEMA = "foundation-platform.orchestration_jobs.v1"
POOLS = {"default_pool", "spark"}
SERVICE_NAME = re.compile(r"foundation-[a-z0-9-]+(?:@([a-z0-9-]+))?\.service")
TIMER_NAME = re.compile(r"foundation-[a-z0-9-]+\.timer")

sys.path.insert(0, str(PLATFORM_ROOT / "infra" / "datahub"))
from dataset_names import name_of, platform_of  # noqa: E402


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    dag_id: str
    description: str
    schedule: str
    timeout_minutes: int
    pool: str
    systemd_service: str
    systemd_timer: str | None  # the timer the job replaced; None for a job that never had one
    enabled: bool
    inputs: list  # (namespace, name)
    outputs: list  # (namespace, name)


class JobListError(ValueError):
    pass


def service_unit_file(service):
    """The unit file a service name is installed from: a template for `name@instance.service`."""
    match = SERVICE_NAME.fullmatch(service)
    if not match:
        raise JobListError(f"{service!r} is not a foundation-*.service name")
    if match.group(1):
        return SYSTEMD / service.replace(f"@{match.group(1)}.", "@.")
    return SYSTEMD / service


# Serving surfaces made by a bake: each has a producing job, or an exemption that says why not.
BAKED_SURFACE_KINDS = {"r2_baked_documents", "tiles"}


def baked_surfaces_without_a_job(jobs=None, graph=None):
    """Every reason a baked serving surface has no producing job (an empty list is a pass).

    A surface is produced by a job when one of the job's pipeline-graph edges ends at it. Before
    2026-10 the by-PNU bakes ran only from a hand-kept server script; this is what makes that
    impossible to repeat unnoticed. An exemption must name a baked surface that really has no job,
    and say why, so it cannot outlive the job that replaces it.
    """
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    edges = {edge["id"]: edge for edge in graph["edges"]}
    produced = {
        edges[edge]["to"]: job["id"]
        for job in jobs["jobs"]
        for edge in job["pipeline_graph_edges"]
        if edge in edges
    }
    baked = {node["id"] for node in graph["nodes"] if node.get("surface_kind") in BAKED_SURFACE_KINDS}
    problems, exempt = [], set()
    for entry in jobs.get("serving_surfaces_without_a_job", []):
        surface, reason = entry.get("surface"), entry.get("reason")
        if surface not in baked:
            problems.append(f"exemption {surface!r} is not a baked serving surface in the pipeline graph")
        elif surface in produced:
            problems.append(f"exemption {surface!r} is stale: job {produced[surface]} produces it")
        elif not (isinstance(reason, str) and reason.strip()):
            problems.append(f"exemption {surface!r} gives no reason")
        if surface in exempt:
            problems.append(f"exemption {surface!r} is listed twice")
        exempt.add(surface)
    for surface in sorted(baked - set(produced) - exempt):
        problems.append(f"baked serving surface {surface!r} has no producing job in jobs.v1.json and no exemption")
    return problems


def load_specs(jobs=None, graph=None):
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    if jobs.get("schema_version") != JOBS_SCHEMA:
        raise JobListError(f"jobs schema_version is not {JOBS_SCHEMA}")

    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}

    def dataset(node_id):
        node = nodes[node_id]
        return (platform_of(node), name_of(node))

    specs, seen_ids, seen_services = [], set(), set()
    for job in jobs["jobs"]:
        job_id = job["id"]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", job_id) or job_id in seen_ids:
            raise JobListError(f"job id {job_id!r} is not a unique lower_snake name")
        seen_ids.add(job_id)
        if job["pool"] not in POOLS:
            raise JobListError(f"{job_id}: pool {job['pool']!r} is not one of {sorted(POOLS)}")
        if not isinstance(job.get("enabled"), bool):
            raise JobListError(f"{job_id}: enabled must be true or false")
        reason = job.get("disabled_reason")
        if job["enabled"] and reason is not None:
            raise JobListError(f"{job_id}: an enabled job has no disabled_reason")
        if not job["enabled"] and job["systemd_timer"] is None and not (isinstance(reason, str) and reason.strip()):
            raise JobListError(
                f"{job_id}: off and without a timer it runs nowhere; disabled_reason must say why and what turns it on"
            )
        service = job["systemd_service"]
        if not service_unit_file(service).is_file():
            raise JobListError(f"{job_id}: infra/systemd has no unit for {service}")
        if service in seen_services:
            raise JobListError(f"{job_id}: {service} already belongs to another job")
        seen_services.add(service)
        timer = job["systemd_timer"]
        if timer is not None and not TIMER_NAME.fullmatch(timer):
            raise JobListError(f"{job_id}: {timer!r} is not a foundation-*.timer name")
        reads_contracts = job.get("reads_data_contracts", False)
        if bool(job["pipeline_graph_edges"]) == bool(reads_contracts):
            raise JobListError(
                f"{job_id}: a job states the pipeline-graph edges it carries out, or that it reads the "
                "data-contract tables (reads_data_contracts) -- exactly one"
            )
        unknown = [edge for edge in job["pipeline_graph_edges"] if edge not in edges]
        if unknown:
            raise JobListError(f"{job_id}: pipeline-graph has no edge {unknown}")

        carried = [edges[edge] for edge in job["pipeline_graph_edges"]]
        if reads_contracts:
            by_table = {node["table_name"]: node["id"] for node in graph["nodes"] if node.get("table_name")}
            tables = sorted(path.name[: -len(".odcs.yaml")] for path in CONTRACTS.glob("*.odcs.yaml"))
            unknown_tables = [table for table in tables if table not in by_table]
            if not tables or unknown_tables:
                raise JobListError(f"{job_id}: data contracts {unknown_tables or 'none'} have no pipeline-graph table")
            inputs = sorted({dataset(by_table[table]) for table in tables})
            outputs = []
        else:
            inputs = sorted({dataset(edge["from"]) for edge in carried})
            outputs = sorted({dataset(edge["to"]) for edge in carried})
        specs.append(
            JobSpec(
                job_id=job_id,
                dag_id=f"foundation_{job_id}",
                description=job["description"],
                schedule=job["schedule"],
                timeout_minutes=int(job["timeout_minutes"]),
                pool=job["pool"],
                systemd_service=service,
                systemd_timer=timer,
                enabled=job["enabled"],
                inputs=inputs,
                outputs=outputs,
            )
        )
    return specs
