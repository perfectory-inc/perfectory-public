"""What each scheduled job is, built from the files that own it (root ADR-0122 §3).

Pure Python with no Airflow import, so the code the DAGs run is the code the tests check:

- the job list:        orchestration/jobs.v1.json
- how a job runs:      its systemd service in infra/systemd (only checked to exist here)
- inputs and outputs:  the endpoints of the pipeline-graph edges a job names
                       (docs/catalog/pipeline-graph.v1.json), named by infra/datahub/dataset_names.py
"""

import itertools
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
# Airflow's own pool; every other pool a job may name is declared in jobs.v1.json `pools`.
DEFAULT_POOL = "default_pool"
# A job may wait for a shared pool's slots at most this many of its own schedule cycles (root
# ADR-0138). The bound is the worst case, not the usual one: an hourly fold behind FLOOR and lineage
# that both time out and are retried waits about 17 hours, which still lands staff edits within
# the day; a 21-hour bake holding a slot a fold needs would add a day more and is refused.
STARVATION_CYCLES = 20
# Every DAG task retries this long after a failure (foundation_jobs.py). A retry holds the slots
# again, so a run can hold them for (retries + 1) x its timeout plus the delays.
RETRY_DELAY_MINUTES = 5
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
    pool_slots: int
    retries: int
    priority_weight: int
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


def declared_pools(jobs):
    """Pool name -> slot count, from jobs.v1.json `pools` (default_pool is Airflow's own)."""
    pools = jobs.get("pools")
    if not isinstance(pools, dict) or not pools:
        raise JobListError("jobs.v1.json declares no pools")
    declared = {}
    for name, pool in pools.items():
        if name == DEFAULT_POOL or not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise JobListError(f"pool {name!r} is not a lower_snake name other than {DEFAULT_POOL}")
        slots = pool.get("slots") if isinstance(pool, dict) else None
        if type(slots) is not int or slots < 1 or not str(pool.get("description", "")).strip():
            raise JobListError(f"pool {name!r} needs a positive integer slots and a description")
        declared[name] = slots
    return declared


def _cron_field(field, low, high):
    if field == "*":
        return list(range(low, high + 1))
    values = []
    for part in field.split(","):
        if not part.isdigit() or not low <= int(part) <= high:
            raise JobListError(f"schedule field {field!r} is not '*' or a list of numbers")
        values.append(int(part))
    return sorted(set(values))


def shortest_interval_minutes(schedule):
    """The shortest gap between two runs of a daily-repeating `M H * * *` schedule."""
    fields = schedule.split()
    if len(fields) != 5 or fields[2:] != ["*", "*", "*"]:
        raise JobListError(f"schedule {schedule!r} is not of the form 'M H * * *' this check reads")
    starts = sorted(hour * 60 + minute for hour in _cron_field(fields[1], 0, 23)
                    for minute in _cron_field(fields[0], 0, 59))
    gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
    return min(gaps + [starts[0] + 24 * 60 - starts[-1]])


def pool_slots(job):
    """Slots a job takes in its pool (Airflow pool_slots); 1 when the job list does not say."""
    slots = job.get("pool_slots", 1)
    if type(slots) is not int or slots < 1:
        raise JobListError(f"{job['id']}: pool_slots must be a positive integer")
    return slots


def retries(job):
    """How often Airflow retries a failed run of the job (`retries`, 1 when absent)."""
    value = job.get("retries", 1)
    if type(value) is not int or not 0 <= value <= 3:
        raise JobListError(f"{job['id']}: retries must be an integer from 0 to 3")
    return value


def priority_weight(job):
    """Which waiting job Airflow starts first when slots free (`priority_weight`, 1 when absent)."""
    value = job.get("priority_weight", 1)
    if type(value) is not int or value < 1:
        raise JobListError(f"{job['id']}: priority_weight must be a positive integer")
    return value


def hold_minutes(job):
    """The longest one scheduled run holds its slots: every try at its timeout, plus the delays."""
    return (retries(job) + 1) * int(job["timeout_minutes"]) + retries(job) * RETRY_DELAY_MINUTES


def longest_wait_minutes(starved, others, slots):
    """An upper bound on how long a job waits for its slots, and which jobs make it up.

    A job that cannot run beside it holds it back: one with an equal or higher priority_weight may
    start ahead of it once each, and one with a lower weight may already be running when its turn
    comes (Airflow starts the heaviest waiting job that fits, so a lighter one never starts ahead
    of it again while it fits). Each counts at its hold_minutes. Jobs that block it only together
    (none of them alone) hold it until enough of them have finished. A `takes_turns` job starts at
    most once between runs of the jobs it cannot run beside (start-scheduled-job.sh), so it counts
    once like the others.
    """
    need = pool_slots(starved)
    blockers = [job for job in others if pool_slots(job) + need > slots]
    total = sum(hold_minutes(job) for job in blockers)
    combined, combined_ids = 0, []
    beside = [job for job in others if job not in blockers]
    for size in range(2, len(beside) + 1):
        for group in itertools.combinations(beside, size):
            held = sum(pool_slots(job) for job in group)
            if held > slots or slots - held >= need:
                continue  # cannot hold the pool together, or leaves room anyway
            freed, missing = 0, need - (slots - held)
            for job in sorted(group, key=hold_minutes):
                freed += pool_slots(job)
                if freed >= missing:
                    if hold_minutes(job) > combined:
                        combined, combined_ids = hold_minutes(job), [member["id"] for member in group]
                    break
    return total + combined, [job["id"] for job in blockers] + combined_ids


def pool_starvation(jobs):
    """Every job that could wait for its pool's slots longer than STARVATION_CYCLES of its runs."""
    problems = []
    for pool, slots in declared_pools(jobs).items():
        members = [job for job in jobs["jobs"] if job["pool"] == pool]
        for starved in members:
            if pool_slots(starved) > slots:
                problems.append(f"{starved['id']} takes {pool_slots(starved)} slots of pool {pool!r}, which has {slots}")
                continue
            others = [job for job in members if job is not starved]
            wait, blockers = longest_wait_minutes(starved, others, slots)
            limit = STARVATION_CYCLES * shortest_interval_minutes(starved["schedule"])
            if wait > limit:
                problems.append(
                    f"{starved['id']} may wait {wait} minutes for pool {pool!r} behind {', '.join(blockers)}, "
                    f"over {STARVATION_CYCLES} of its runs ({limit} minutes); change the slots or the pool"
                )
    return problems


def load_specs(jobs=None, graph=None):
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    if jobs.get("schema_version") != JOBS_SCHEMA:
        raise JobListError(f"jobs schema_version is not {JOBS_SCHEMA}")

    pools = {DEFAULT_POOL, *declared_pools(jobs)}
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
        if job["pool"] not in pools:
            raise JobListError(f"{job_id}: pool {job['pool']!r} is not one of {sorted(pools)}")
        if not isinstance(job.get("takes_turns", False), bool):
            raise JobListError(f"{job_id}: takes_turns must be true or false")
        retries(job)
        priority_weight(job)
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
                pool_slots=pool_slots(job),
                retries=retries(job),
                priority_weight=priority_weight(job),
                systemd_service=service,
                systemd_timer=timer,
                enabled=job["enabled"],
                inputs=inputs,
                outputs=outputs,
            )
        )
    starvation = pool_starvation(jobs)
    if starvation:
        raise JobListError("; ".join(starvation))
    return specs
