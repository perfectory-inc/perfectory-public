"""What each scheduled job is, built from the files that own it (root ADR-0122 §3).

Pure Python with no Airflow import, so the code the DAGs run is the code the tests check:

- the job list:        orchestration/jobs.v1.json
- how a job runs:      its systemd service in infra/systemd (only checked to exist here)
- inputs and outputs:  the endpoints of the pipeline-graph edges a job names
                       (docs/catalog/pipeline-graph.v1.json), named by infra/datahub/dataset_names.py
- what starts a job:   its `started_by` (root ADR-0171): its schedule, or an event on one of its
                       inputs that another job's run changed, with the schedule as the fallback.
                       Which job feeds which is read from the same inputs and outputs, so there is
                       no second list of dependencies.
"""

import datetime
import json
import pathlib
import re
import sys
from dataclasses import dataclass

PLATFORM_ROOT = pathlib.Path(__file__).resolve().parents[2]
JOBS = PLATFORM_ROOT / "orchestration" / "jobs.v1.json"
GRAPH = PLATFORM_ROOT / "docs" / "catalog" / "pipeline-graph.v1.json"
# What each job reads and writes and which jobs feed it, rendered from the two files above by
# dependency_document() for readers that are not Python (foundation-api, root ADR-0188).
DEPENDENCIES = PLATFORM_ROOT / "orchestration" / "job-dependencies.v1.json"
DEPENDENCIES_SCHEMA = "foundation-platform.orchestration_job_dependencies.v1"
# The task of every job DAG that runs the job; the DAG run starts first and waits in the job's pool
# until this task gets its slot. Rendered into the dependencies so the data catalog screen tells
# the run's waiting from its running without a second copy of the name.
RUN_TASK_ID = "run"
# The task that records a job's output asset events when its run changed them (foundation_jobs.py).
# Its success is the job's materialization, which is what Dagster observes (root ADR-0333 stage 0).
PUBLISH_TASK_ID = "publish_outputs"
SYSTEMD = PLATFORM_ROOT / "infra" / "systemd"
CONTRACTS = PLATFORM_ROOT / "contracts" / "data"
# Where a unit's ExecStart finds the release on the host.
RELEASE_PREFIX = "/opt/foundation-platform/current/"
JOBS_SCHEMA = "foundation-platform.orchestration_jobs.v1"
# Airflow's own pool; every other pool a job may name is declared in jobs.v1.json `pools`.
DEFAULT_POOL = "default_pool"
# The most slots a pool may have: what the deploy's gate lets it set as a Dagster pool limit
# (dagster_writes.MAX_POOL_LIMIT, held equal by test_dagster_writer_gate.py; root ADR-0605 §6).
MAX_POOL_SLOTS = 16
# Every run is tried again this long after a failure (foundation_jobs.py, orchestration/dagster/run_jobs.py).
RETRY_DELAY_MINUTES = 5
SERVICE_NAME = re.compile(r"foundation-[a-z0-9-]+(?:@([a-z0-9-]+))?\.service")
TIMER_NAME = re.compile(r"foundation-[a-z0-9-]+\.timer")
ENABLED_SINCE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
# What starts a job (jobs.v1.json `started_by`, root ADR-0171). A `schedule` job starts at its
# schedule. An `inputs` job starts when a run of another job reports that it changed one of the
# job's inputs, and at its schedule as well (the fallback that catches what no event announces).
STARTED_BY = ("schedule", "inputs")
# Which orchestrator runs a job (jobs.v1.json `dagster_stage`, root ADR-0431), in the order a job moves:
# `airflow` (absent) Airflow decides and runs; `shadow` Airflow decides and runs while Dagster decides
# beside it and compares; `proxied` Airflow decides, Dagster runs; `dagster` Dagster decides and runs.
DAGSTER_STAGES = ("airflow", "shadow", "proxied", "dagster")
# The stages something implements (ADR-0431 §5). At `dagster` Dagster decides (orchestration/dagster/decide.py)
# and Airflow builds no DAG; the deploy's drain and start-once (job_orchestrator.py), an operator's run-now,
# the runs' lineage (lineage.py) and the operations map reach the job through Dagster (root ADR-0565, ADR-0583 §9).
IMPLEMENTED_DAGSTER_STAGES = DAGSTER_STAGES
# The stages in which Dagster runs the job's unit, and Dagster's decisions are compared with Airflow's
# while Airflow still decides (root ADR-0458 §4).
DAGSTER_RUNS = ("proxied", "dagster")
# The groups jobs move to Dagster in (jobs.v1.json `dagster_group`, root ADR-0583 §1), downstream first:
# what a group writes is read only by its own jobs or by a group listed before it. A group is one switch:
# its switched-on jobs reach `dagster` in one PR, and reverting that PR is the rollback (dagster_groups).
DAGSTER_GROUPS = ("gold", "silver", "collect", "operations")
COMPARED = ("shadow", "proxied")
# The Dagster names (dagster_writes.py, which the writable webserver's gate also reads, root ADR-0565 §8):
# a `proxied` job's run (root ADR-0431 §5) is Airflow's `run` task asking Dagster to launch the job's
# Dagster job, DAGSTER_JOB_PREFIX + its id, and Dagster runs the unit through the host's forced command
# (orchestration/dagster/run_jobs.py, root ADR-0417); a job at `dagster` is decided by its sensor,
# DAGSTER_SENSOR_PREFIX + its id (orchestration/dagster/decide.py), which a deploy stops to hold it
# (scripts/deploy/job_orchestrator.py); both are named in the code location and repository
# DAGSTER_LOCATION and DAGSTER_REPOSITORY (orchestration/dagster/workspace.yaml). One file, so the Airflow
# image, the Dagster code location and the gate cannot drift apart.
if str(pathlib.Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from dagster_writes import DAGSTER_JOB_PREFIX, DAGSTER_LOCATION, DAGSTER_REPOSITORY, DAGSTER_SENSOR_PREFIX  # noqa: E402, F401
# Run tags. Airflow sends how the run was started (the forced command's words after the job id: empty for
# a clock start, `triggered`, or `requested <id>`); Dagster's run answers with its outcome word and, for
# the asset reconciler, the runs it found owed, which Airflow's `run` task pushes as today's XComs.
RUN_KIND_TAG = "foundation/run-kind"
OUTCOME_TAG = "foundation/job-outcome"
OWED_TAG = "foundation/asset-owed"
# The run queue's view of a job run (orchestration/dagster/dagster.yaml): every job run is counted under
# JOB_RUN_TAG apart from the observer's check runs, and Dagster's own `dagster/priority`, the order the
# queue starts runs in, is dagster_priority (root ADR-0435, ADR-0558). Its pool is not a tag: it is the
# op's own Dagster pool (orchestration/dagster/run_jobs.py, root ADR-0605).
JOB_RUN_TAG = "foundation/run"
# A job's `dagster/priority` is its priority class's value times this, plus its queue weight: the class
# orders first (root ADR-0435), and inside it a job that writes what another reads starts first (root
# ADR-0558), as Airflow orders its pool's tasks by the weight. A weight never reaches this (load_specs).
QUEUE_WEIGHT_SPAN = 1000
# What may follow the job id in a start: nothing (the clock), `triggered`, or `requested <id>` (an
# operator's run-now, root ADR-0282 §4); the host's forced command reads the same words.
RUN_KIND = re.compile(r"(?:triggered|requested [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?")
# A run of a job Dagster decides (`dagster_stage: dagster`) also carries this: dagster.yaml holds such runs to
# the pools' slots together, so the runs Airflow hands Dagster (proxied) always find a place (root ADR-0583 §7).
DECIDED_TAG = "foundation/decided-by"
PRIORITY_TAG = "dagster/priority"
# Dagster's own run-monitoring tag (orchestration/dagster/dagster.yaml run_monitoring): past it a run still
# open is failed. A job's run gets its scheduled start's limit: its wait, its run and its stop grace.
MAX_RUNTIME_TAG = "dagster/max_runtime_seconds"
# The line a job's unit prints last to say how its run ended (root ADR-0171 §3, ADR-0260):
# `changed`, `unchanged`, `waiting <reason>` or `failed <reason>`. Only `changed` records the run's
# output events; a run that does not say counts as changed, and the jobs it starts find nothing to
# do on their own.
OUTCOME_LINE = re.compile(rb"^foundation-job-outcome (changed|unchanged|(?:waiting|failed|skipped) \S[^\r\n]*)\r?$", re.MULTILINE)
# The outcomes after which nothing downstream starts: nothing changed, the run waits on someone
# else, it failed, or it was skipped (an operator paused the job, or a run-now that could not be
# used; root ADR-0282).
WITHHOLDS_EVENTS = ("unchanged", "waiting", "failed", "skipped")
# The line a job's check service prints last (jobs.v1.json `check_service`, root ADR-0277). Only
# `unchanged` skips the run; any other word, or none, lets the run decide for itself.
CHECK_LINE = re.compile(r"^foundation-job-check (\S+)\r?$", re.MULTILINE)
# A job's check and its bound: the slowest lane's took 4.2 s (2026-10-14..18), the unit's own
# TimeoutStartSec is 5 minutes. Airflow's `check` task and Dagster's run of a job it decides both keep it.
CHECK_TIMEOUT_MINUTES = 6
# The run's output is the journal of a run that can last a day; the line is at its end.
OUTCOME_TAIL_BYTES = 64 * 1024
# What the asset reconciler prints for each job it finds owed a run (root ADR-0333 §7): the job and
# a key of the input versions it owes. The DAG starts the job once per key.
OWED_LINE = re.compile(r"^foundation-asset-owed ([a-z][a-z0-9_]*) ([0-9a-f]{16})\r?$", re.MULTILINE)

sys.path.insert(0, str(PLATFORM_ROOT / "infra" / "datahub"))
from dataset_names import name_of, platform_of, urn_of  # noqa: E402


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    dag_id: str
    description: str
    schedule: str
    timeout_minutes: int  # one start's limit: scheduler_timeout_minutes(job)
    pool: str  # its Dagster pool, and Airflow's for a job Airflow runs; a run takes one slot
    retries: int
    priority_weight: int
    systemd_service: str
    systemd_timer: str | None  # the timer the job replaced; None for a job that never had one
    enabled: bool
    inputs: list  # (namespace, name)
    outputs: list  # (namespace, name)
    started_by: str = "schedule"  # STARTED_BY
    producers: tuple = ()  # the other jobs whose outputs are among its inputs
    check_service: str | None = None  # decides, without a pool slot, whether the run has work
    enabled_since: str | None = None  # jobs.v1.json `enabled_since` (root ADR-0239)
    reconciles: tuple = ()  # the jobs this one starts by state, not by event (root ADR-0333 §7)
    requeues: tuple = ()  # the jobs whose refused starts it starts again from the start queue (root ADR-0462)
    dagster_stage: str = "airflow"  # DAGSTER_STAGES
    dagster_group: str = ""  # DAGSTER_GROUPS
    priority: int = 100  # the value of its priority class (root ADR-0435)

    @property
    def dagster_job(self):
        """The Dagster job that runs this job once it is past `shadow` (DAGSTER_JOB_PREFIX)."""
        return DAGSTER_JOB_PREFIX + self.job_id

    def dagster_tags(self, run_kind=""):
        """The tags a Dagster run of this job carries: how it was started, and what the run queue reads."""
        tags = run_tags(dagster_priority(self), run_kind, 60 * self.timeout_minutes)
        if self.dagster_stage == "dagster":
            tags[DECIDED_TAG] = "dagster"
        return tags

    @property
    def starting_inputs(self):
        """The inputs whose change starts a `started_by: inputs` job: all but those it writes itself.

        A job may write a dataset it then reads (the Gold rebuild joins silver.unit_official_price
        before the building panel reads it, root ADR-0182); its own output event must not start it
        again.
        """
        return [dataset for dataset in self.inputs if dataset not in self.outputs]


class JobListError(ValueError):
    pass


def run_tags(priority, run_kind="", max_runtime_seconds=None):
    """The tags a Dagster run of a job carries, whoever launches it (Airflow's proxied task, the job's
    sensor, the deploy or a run-now): how it was started, what the run queue reads, and when run
    monitoring fails a run whose process died (MAX_RUNTIME_TAG; the default launcher cannot tell)."""
    tags = {RUN_KIND_TAG: run_kind, JOB_RUN_TAG: "job", PRIORITY_TAG: str(priority)}
    if max_runtime_seconds is not None:
        tags[MAX_RUNTIME_TAG] = str(max_runtime_seconds)
    return tags


def dagster_priority(spec):
    """The order Dagster's queue starts the job's runs in (QUEUE_WEIGHT_SPAN): its class, then its weight.
    2026-11-20: Gold moved to Dagster (root ADR-0583), whose queue knew the class alone, so a bake could
    have started before the rebuild whose output it reads, and #1515's tile, served Gold and registry order
    (priority_weight 31..33) would have been lost."""
    return spec.priority * QUEUE_WEIGHT_SPAN + spec.priority_weight


def asset_uri(dataset):
    """The Airflow asset of a pipeline-graph dataset: its catalog platform and name, one identity.

    The scheme is the data catalog platform (`iceberg`, `perfectory`). No Airflow provider turns
    these schemes into OpenLineage datasets, so an asset never adds a second lineage entity beside
    the one the run task reports.
    """
    namespace, name = dataset
    return f"{namespace}://{name}"


def outcome_line(output):
    """The last `foundation-job-outcome` line in the tail of what a run printed (bytes or text), or None.

    The host runner asks this of the run's journal (`--outcome-line`) and fails a successful run
    that has none (root ADR-0260 §2); the DAG asks it of what the runner printed.
    """
    if output is None:
        return None
    if isinstance(output, str):
        output = output.encode("utf-8", "replace")
    found = OUTCOME_LINE.findall(bytes(output[-OUTCOME_TAIL_BYTES:]))
    return "foundation-job-outcome " + found[-1].decode("utf-8", "replace") if found else None


def owed_runs(output, reconciled):
    """[(job id, key)] the reconciler's run printed, for the jobs it reconciles, in its order, once each.

    Only a job in `reconciled` (its `reconciles`) may be named: what the host prints never starts
    another DAG. The whole output is read: the lines come before the outcome line, and the run is short.
    """
    if isinstance(output, bytes):
        output = output.decode("utf-8", "replace")
    found = {}
    for job, key in OWED_LINE.findall(output or ""):
        if job in reconciled:
            found.setdefault(job, key)
    return list(found.items())


def check_says_unchanged(output):
    """Whether what a check printed (text) ends `foundation-job-check unchanged`: the run has nothing to do."""
    found = CHECK_LINE.findall(output or "")
    return bool(found) and found[-1] == "unchanged"


def job_outcome(output):
    """`changed`, `unchanged`, `waiting`, `failed` or `skipped`, from the tail of what a run printed (bytes or text).

    The last `foundation-job-outcome` line decides; its reason is not kept. No line counts as
    `changed`: the host runner already failed a successful run without one (root ADR-0260 §2), and
    repeats the line it decided on last, so what is left is output the scheduler cut short.
    """
    line = outcome_line(output)
    return line.split(" ", 2)[1] if line else "changed"


def service_unit_file(service):
    """The unit file a service name is installed from: a template for `name@instance.service`."""
    match = SERVICE_NAME.fullmatch(service)
    if not match:
        raise JobListError(f"{service!r} is not a foundation-*.service name")
    if match.group(1):
        return SYSTEMD / service.replace(f"@{match.group(1)}.", "@.")
    return SYSTEMD / service


def service_unit_text(service):
    """The unit a service runs, as systemd reads it: its file's, or its template's with `%i` the instance
    (systemd.unit(5) specifiers), so a lane's instance of a template states that lane's arguments; then
    each of the instance's drop-ins (`<service>.d/*.conf`, in name order) sets its lines over the unit's
    (root ADR-0574 §8: an instance's own time and memory limit)."""
    text = service_unit_file(service).read_text(encoding="utf-8")
    match = SERVICE_NAME.fullmatch(service)
    if match and match.group(1):
        text = text.replace("%i", match.group(1))
    for drop_in in sorted((SYSTEMD / f"{service}.d").glob("*.conf")):
        for line in drop_in.read_text(encoding="utf-8").splitlines():
            key, equals, _ = line.strip().partition("=")
            if not equals or line.lstrip().startswith(("#", ";", "[")):
                continue
            # shortcut: the drop-ins here set single-valued keys the unit already has; a list key
            # (ExecStart=, Environment=) would need systemd's reset rules.
            setting = line.strip()
            text = re.sub(rf"^{re.escape(key)}=.*$", lambda _, setting=setting: setting, text, flags=re.MULTILINE)
    return text


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


# The lakehouse datasets a job is expected to make (root ADR-0234); source groups, Postgres serving
# groups and serving surfaces are not data-flow steps a scheduled job writes.
DATA_FLOW_TYPES = {"silver_table", "gold_table", "reference_table", "intelligence_table"}
# Why a dataset no job writes has none (pipeline-graph node `unscheduled`, root ADR-0234 §2):
# on_event: made by hand when its input event comes, by the runbook its runbook_ref names;
# not_started: production has never written it; needs_job: it should have a job and has none yet.
UNSCHEDULED = {"on_event", "not_started", "needs_job"}


def datasets_without_a_job(jobs=None, graph=None):
    """Every reason a lakehouse dataset's job coverage is not stated (an empty list is a pass).

    A dataset is written by a job when one of the job's pipeline-graph edges ends at it, enabled
    or not. Every other dataset says why in its node's `unscheduled`, so the flow map can tell a
    dataset that is made on an event from one that is missing its job. The declaration cannot
    outlive the job that replaces it.
    """
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    edges = {edge["id"]: edge for edge in graph["edges"]}
    written = {
        edges[edge]["to"]: job["id"]
        for job in jobs["jobs"]
        for edge in job["pipeline_graph_edges"]
        if edge in edges
    }
    problems = []
    for node in graph["nodes"]:
        node_id, unscheduled = node["id"], node.get("unscheduled")
        if unscheduled is None:
            if node["type"] in DATA_FLOW_TYPES and node.get("status") != "contract_only" and node_id not in written:
                problems.append(f"dataset {node_id!r} has no job in jobs.v1.json and does not say why (unscheduled)")
            continue
        if node["type"] not in DATA_FLOW_TYPES or node.get("status") == "contract_only":
            problems.append(f"{node_id!r} says unscheduled but is not a lakehouse dataset a job could write")
        elif node_id in written:
            problems.append(f"{node_id!r} says unscheduled {unscheduled!r} but job {written[node_id]} writes it")
        elif unscheduled not in UNSCHEDULED:
            problems.append(f"{node_id!r} says unscheduled {unscheduled!r}, not one of {sorted(UNSCHEDULED)}")
        elif unscheduled == "on_event" and not (PLATFORM_ROOT.parents[1] / node.get("runbook_ref", "")).is_file():
            problems.append(f"{node_id!r} is made on an event but its runbook_ref names no file")
    return problems


SECRETS = PLATFORM_ROOT / "config" / "runtime-secrets.contract.json"
# A shell assignment of a state root with a default: `NAME="${SOURCE:-/default}"`.
STATE_ROOT_DEFAULT = re.compile(r'^([A-Z_]*STATE_ROOT[A-Z_]*)="\$\{([A-Z_]+):-([^}]*)\}"', re.MULTILINE)


def _env_file_names(unit_text, secrets):
    """The variable names the unit's EnvironmentFiles hold (runtime-secrets contract groups)."""
    paths = {re.sub(r"^-", "", path) for path in re.findall(r"^EnvironmentFile=(\S+)$", unit_text, flags=re.MULTILINE)}
    names = set()
    for group in secrets["groups"]:
        if group["path"] in paths:
            names.update(name for name in group["holds"] if not name.startswith("@"))
    return names


def state_path_problems(jobs=None, secrets=None, read=None):
    """Every way a job script could write outside its unit's sandbox (an empty list is a pass; root
    ADR-0266).

    - `state_paths` in jobs.v1.json is where a job writes. Its unit's `ReadWritePaths` is rendered
      from it, and a unit that differs is refused, by the crate foundation-jobs (root ADR-0270 §1).
    - A job script's state root (`*STATE_ROOT*="${SOURCE:-default}"`) must not come from a variable
      one of its unit's EnvironmentFiles holds: the file's value is the host's, unseen here, and
      2026-10-16 it pointed parcel_registry outside its sandbox (Read-only file system).
    - A literal default of such a root must lie under the job's `state_paths` or its unit's
      StateDirectory.
    """
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    secrets = secrets if secrets is not None else json.loads(SECRETS.read_text(encoding="utf-8"))
    read = read or (lambda path: path.read_text(encoding="utf-8"))
    problems = []
    for job in jobs["jobs"]:
        job_id, paths = job["id"], job.get("state_paths")
        if not isinstance(paths, list) or not all(isinstance(path, str) and path.startswith("/") for path in paths):
            problems.append(f"{job_id}: state_paths must list the absolute paths the job writes")
            continue
        unit_path = service_unit_file(job["systemd_service"])
        unit = read(unit_path)
        start = re.search(r"^ExecStart=(\S+)", unit, flags=re.MULTILINE)
        prefix = "/opt/foundation-platform/current/"
        if not start or not start.group(1).startswith(prefix) or not start.group(1).endswith(".sh"):
            continue
        script = PLATFORM_ROOT / start.group(1)[len(prefix):]
        held = _env_file_names(unit, secrets)
        # systemd creates and allows /var/lib/<StateDirectory> itself.
        writable = paths + [f"/var/lib/{name}" for name in re.findall(r"^StateDirectory=(\S+)$", unit, flags=re.MULTILINE)]
        for name, source, default in STATE_ROOT_DEFAULT.findall(read(script)):
            if source in held:
                problems.append(f"{job_id}: {script.name} takes {name} from {source}, which an EnvironmentFile of "
                                f"{unit_path.name} holds; a state root comes from the job's state_paths")
            literal = default.split("$", 1)[0]
            if literal.startswith("/") and "$" not in default and not any(
                    literal == path or literal.startswith(path.rstrip("/") + "/") for path in writable):
                problems.append(f"{job_id}: {script.name} defaults {name} to {literal}, outside what {unit_path.name} "
                                f"may write {writable}")
    return problems


def job_state_root(job_id, jobs=None):
    """The first of a job's state_paths: where its script keeps its state (root ADR-0266)."""
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    for job in jobs["jobs"]:
        if job["id"] == job_id and job.get("state_paths"):
            return job["state_paths"][0]
    raise JobListError(f"{job_id}: no such job with state_paths")


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
        if type(slots) is not int or not 1 <= slots <= MAX_POOL_SLOTS or not str(pool.get("description", "")).strip():
            raise JobListError(f"pool {name!r} needs slots of 1 to {MAX_POOL_SLOTS} and a description")
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


def previous_slot(schedule, at):
    """The newest start of a daily-repeating `M H * * *` schedule at or before `at` (UTC epoch seconds),
    as Kubernetes' CronJob controller finds the most recent schedule it missed (root ADR-0467)."""
    fields = schedule.split()
    if len(fields) != 5 or fields[2:] != ["*", "*", "*"]:
        raise JobListError(f"schedule {schedule!r} is not of the form 'M H * * *' this check reads")
    day = int(at) - int(at) % 86400
    starts = sorted(hour * 3600 + minute * 60 for hour in _cron_field(fields[1], 0, 23)
                    for minute in _cron_field(fields[0], 0, 59))
    today = [day + start for start in starts if day + start <= at]
    return today[-1] if today else day - 86400 + starts[-1]


def retries(job):
    """How often Airflow retries a failed run of the job (`retries`, 1 when absent)."""
    value = job.get("retries", 1)
    if type(value) is not int or not 0 <= value <= 3:
        raise JobListError(f"{job['id']}: retries must be an integer from 0 to 3")
    return value


def no_slot_count(entry, name):
    """A run takes one slot of its pool, as a Dagster pool of run granularity counts it (root ADR-0605):
    a job or a capacity claimant states no slot count of its own, so none is read and silently ignored."""
    if "pool_slots" in entry:
        raise JobListError(f"{name}: pool_slots is gone; a run takes one slot of its pool (root ADR-0605)")


def priority_weight(job):
    """The job's tier in its pool's queue (`priority_weight`, 1 when absent); queue_weights raises it
    above the jobs that read what the job writes, and that is the weight Airflow orders by."""
    value = job.get("priority_weight", 1)
    if type(value) is not int or value < 1:
        raise JobListError(f"{job['id']}: priority_weight must be a positive integer")
    return value


def queue_weights(jobs, feeders):
    """Job id -> the weight Airflow orders its pool's waiting tasks by (root ADR-0138, ADR-0558): the
    job's own priority_weight, raised until it is above every job of its pool that reads what it
    writes. The data dependency is the pipeline graph's (`feeders`, producers()), so no weight is kept
    in step with it by hand; Airflow's own default rule does the same inside one DAG (`weight_rule`
    "downstream": a task outranks what waits on it), and every job here is its own DAG. Jobs that read
    each other's outputs in a circle (the two map-edit folds share one ledger) are one group with one
    weight.

    2026-11-11 12:15Z..: data_go_kr_file_data (weight 1) waited over 16 hours in the spark pool's queue
    while gold_parcel_nearby_rebuild (weight 2), which reads the eight Silver tables only it loads, ran
    ahead of it and failed on tables that did not exist yet.
    """
    by_id = {job["id"]: job for job in jobs["jobs"]}
    readers = {job_id: [] for job_id in by_id}
    for reader, writers in feeders.items():
        for writer in writers:
            if by_id[writer]["pool"] == by_id[reader]["pool"]:
                readers[writer].append(reader)

    def reach(start):
        seen, stack = set(), [start]
        while stack:
            for reader in readers[stack.pop()]:
                if reader not in seen:
                    seen.add(reader)
                    stack.append(reader)
        return seen

    downstream = {job_id: reach(job_id) for job_id in by_id}
    group = {job_id: frozenset({job_id} | {other for other in downstream[job_id] if job_id in downstream[other]})
             for job_id in by_id}
    weights = {}
    # A group with fewer jobs downstream reads nothing of one with more, so each is weighed after
    # everything that waits on it.
    for members in sorted(set(group.values()), key=lambda members: len(downstream[min(members)] - members)):
        waiting = {reader for member in members for reader in readers[member]} - members
        weight = max([priority_weight(by_id[member]) for member in members]
                     + [weights[reader] + 1 for reader in waiting])
        weights.update(dict.fromkeys(members, weight))
    return weights


def priority_class(jobs, entry, name):
    """The host's priority class of a job or a capacity claimant (`priority_class`, `default` when
    absent, root ADR-0435): one of the list's priority_classes. A class that yields at a step boundary
    is only for a spark-pool job whose run commits as it goes, so yielding loses no committed step."""
    classes = jobs.get("priority_classes") or {}
    value = entry.get("priority_class", "default")
    if value not in classes or not isinstance(classes[value].get("value"), int):
        raise JobListError(f"{name}: priority_class {value!r} is not one of {sorted(classes)}")
    if classes[value].get("yields_at_step_boundary") and (
            entry.get("pool") != "spark" or entry.get("commits", "as_it_goes") != "as_it_goes"
            or entry.get("interruptible") is False):
        raise JobListError(f"{name}: priority_class {value} yields at a step boundary; only a spark-pool job"
                           " that commits as it goes and is interruptible can")
    return value


def inherited_class(job, by_id, found):
    """A job that names `priority_class_from` takes its class from a job that waits on what it makes
    (root ADR-0439, priority inheritance): the named job reads one of its outputs, and the two classes
    are the same, so the class cannot drift from the bake it serves. 2026-11-07 the panel Gold rebuild
    ran in `default` while two lane cut-overs of class `publish` held the spark pool; the parcel lane
    it refreshes served a 193-hour-old manifest."""
    source = job.get("priority_class_from")
    if source is None:
        return
    other = by_id.get(source)
    if other is None:
        raise JobListError(f"{job['id']}: priority_class_from names {source!r}, which is not a job")
    if not set(found[job["id"]][1]) & set(found[source][0]):
        raise JobListError(f"{job['id']}: priority_class_from names {source}, which reads nothing {job['id']} makes")
    if job.get("priority_class", "default") != other.get("priority_class", "default"):
        raise JobListError(f"{job['id']}: priority_class {job.get('priority_class', 'default')!r} is not "
                           f"{other.get('priority_class', 'default')!r}, the class of {source} it takes it from")


def memory_claims(jobs):
    """Each job's `memory_claim` (root ADR-0563), checked: the systemd units whose MemoryMax its run
    takes, admitted against the host's available memory when it starts (scripts/deploy/job_units.py
    memory_short). A claim needs the list's `memory_claims.keep_available` and names units. It may be in a
    pool of more than one slot (root ADR-0605 §3): memory_short counts what every running claimed unit
    may still take, so two claims started one after the other are never admitted against the same memory."""
    claiming = [job for job in jobs["jobs"] if "memory_claim" in job]
    if not claiming:
        return
    keep = (jobs.get("memory_claims") or {}).get("keep_available")
    if not isinstance(keep, str) or not re.fullmatch(r"[1-9][0-9]*[mg]", keep):
        raise JobListError("memory_claims.keep_available must be a size like 6g when a job states a memory_claim")
    for job in claiming:
        claim = job["memory_claim"]
        units = claim.get("units") if isinstance(claim, dict) else None
        named = isinstance(units, list) and units and all(
            # A template instance too (`foundation-silver-refresh@<lane>.service`): a Silver lane claims
            # its own unit (root ADR-0599). Host admission, deleted with pool-full by root ADR-0583's last PR.
            isinstance(unit, str) and re.fullmatch(r"[a-z0-9-]+(@[a-z0-9-]+)?\.service", unit) for unit in units)
        if not named or not {"units"} <= set(claim) <= {"units", "beside"}:
            raise JobListError(f"{job['id']}: memory_claim must be an object with `units`, systemd service names, and"
                               " optionally `beside`")
        if "beside" in claim and not (isinstance(claim["beside"], str) and re.fullmatch(r"[1-9][0-9]*[mg]", claim["beside"])):
            raise JobListError(f"{job['id']}: memory_claim.beside must be a size like 2560m (its side container's cap)")
    reserve = (jobs.get("memory_claims") or {}).get("static_reserve")
    if not isinstance(reserve, str) or not re.fullmatch(r"[1-9][0-9]*[mg]", reserve):
        raise JobListError("memory_claims.static_reserve must be a size like 21760m when a job states a memory_claim"
                           " (root ADR-0605 §3: what a claim beside another leaves the static pools)")


def capacity_claimants(jobs, pools):
    """The work outside the job list that claims pool slots (root ADR-0435), each checked."""
    claimants = jobs.get("capacity_claimants") or {}
    for name, claimant in claimants.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or any(job["id"] == name for job in jobs["jobs"]):
            raise JobListError(f"capacity claimant {name!r} is not a lower_snake name apart from every job id")
        if claimant.get("pool") not in pools or claimant["pool"] == DEFAULT_POOL:
            raise JobListError(f"capacity claimant {name}: pool {claimant.get('pool')!r} is not a declared pool")
        no_slot_count(claimant, f"capacity claimant {name}")
        if not re.fullmatch(r"[a-z0-9@*-]+\.service", claimant.get("unit") or ""):
            raise JobListError(f"capacity claimant {name}: unit must be a systemd service name or pattern")
        work = claimant.get("work_units", [])
        if not isinstance(work, list) or not all(isinstance(unit, str) and re.fullmatch(r"[a-z0-9@*-]+\.service", unit)
                                                 for unit in work):
            raise JobListError(f"capacity claimant {name}: work_units must be systemd service names or patterns")
        if (jobs["priority_classes"].get(claimant.get("priority_class")) or {}).get("yields_at_step_boundary"):
            raise JobListError(f"capacity claimant {name}: a claimant does not yield; name another class")
        priority_class(jobs, claimant, f"capacity claimant {name}")
    return claimants


def run_limit_minutes(job):
    """One run's limit: its time_limit_minutes plus its stop_grace_minutes (root ADR-0270 §1).

    What a start that does not wait takes: the deploy's wait for a started-once job.
    """
    for field in ("time_limit_minutes", "stop_grace_minutes"):
        if type(job.get(field)) is not int or job[field] < 1:
            raise JobListError(f"{job['id']}: {field} must be a positive integer")
    return job["time_limit_minutes"] + job["stop_grace_minutes"]


def scheduler_timeout_minutes(job):
    """One scheduled start's limit: its wait_limit_minutes plus its run's limit (root ADR-0270 §2b).

    The DAG's execution and SSH timeouts and the operations map's lateness use
    this sum; jobs.v1.json states its three parts. A start may wait for the host before it runs.
    """
    wait = job.get("wait_limit_minutes")
    if type(wait) is not int or wait < 0:
        raise JobListError(f"{job['id']}: wait_limit_minutes must be a whole number of minutes, 0 or more")
    return wait + run_limit_minutes(job)


def dagster_stage(job, jobs=None):
    """Which orchestrator decides and runs a job (jobs.v1.json `dagster_stage`, root ADR-0431).

    A job past `shadow` runs in Dagster, so it needs the Dagster that observers.dagster switches on:
    with it off, every run of a `proxied` job would fail to launch (2026-11-06..09 the code location
    did not load for three days while the deploy said "observing", #1204).
    """
    value = job.get("dagster_stage", "airflow")
    if value not in DAGSTER_STAGES:
        raise JobListError(f"{job['id']}: dagster_stage must be one of {list(DAGSTER_STAGES)}")
    if value not in IMPLEMENTED_DAGSTER_STAGES:
        raise JobListError(f"{job['id']}: dagster_stage {value} has no implementation yet (root ADR-0431 §5)")
    if value != "airflow" and not job["enabled"]:
        raise JobListError(f"{job['id']}: a switched-off job runs nowhere; it stays at dagster_stage airflow")
    observed = jobs is not None and ((jobs.get("observers") or {}).get("dagster") or {}).get("enabled") is True
    if value == "airflow" and job["enabled"] and observed:
        # 2026-11-13: 24 switched-on jobs (the own-producer lanes' Gold and bakes, the intelligence and notice
        # jobs) had no stage, so Airflow alone decided them and the move gate had no line of theirs.
        raise JobListError(f"{job['id']}: a switched-on job is in the dual run from its first release: "
                           "dagster_stage shadow (root ADR-0458)")
    if value in ("proxied", "dagster") and jobs is not None and not observed:
        raise JobListError(f"{job['id']}: dagster_stage {value} runs in Dagster, which observers.dagster.enabled leaves off")
    return value


def dagster_group(job):
    """The group a job moves to Dagster with (jobs.v1.json `dagster_group`, root ADR-0583 §2). Every job
    names one, a switched-off one too: it takes its group's stage when it is switched on."""
    value = job.get("dagster_group")
    if value not in DAGSTER_GROUPS:
        raise JobListError(f"{job['id']}: dagster_group must be one of {list(DAGSTER_GROUPS)} (root ADR-0583)")
    return value


def dagster_groups(jobs):
    """Problems of the group switches (root ADR-0583 §2): in a group, either every switched-on job is at
    `dagster` or none is, so a group moves in one PR and a job switched on later is born where its group is."""
    decided = {}
    for job in jobs["jobs"]:
        if job["enabled"]:
            decided.setdefault(dagster_group(job), set()).add(job.get("dagster_stage", "airflow") == "dagster")
    return [f"group {group}: some of its switched-on jobs are at dagster_stage dagster and some are not; "
            "a group moves in one switch (root ADR-0583 §2)" for group, kinds in sorted(decided.items()) if len(kinds) > 1]


def dagster_order(jobs, feeders):
    """Problems of the move order (root ADR-0431 §3): a job Dagster decides (`dagster`) feeds only jobs
    Dagster decides, since an Airflow job started by its inputs waits for an Airflow asset event that
    a Dagster run never records. Downstream families move first."""
    stages = {job["id"]: job.get("dagster_stage", "airflow") for job in jobs["jobs"]}
    by_id = {job["id"]: job for job in jobs["jobs"]}
    problems = [f"{consumer}: started by the outputs of {producer}, which Dagster decides; it moves to dagster first"
                for consumer, producers in sorted(feeders.items())
                # A switched-off job runs nowhere and stays at `airflow` (dagster_stage); switched on, it is
                # born at its group's side (dagster_groups, root ADR-0583 §2).
                if started_by(by_id[consumer]) == "inputs" and by_id[consumer]["enabled"]
                for producer in producers if stages[producer] == "dagster" and stages[consumer] != "dagster"]
    # A reconciler and the jobs it starts (started_for). A job Dagster decides reads what it is owed from
    # the reconciler's Dagster run, which exists only once Dagster runs the reconciler; a reconciler Dagster
    # decides has no DAG whose `start` task could trigger the DAG of a job Airflow decides. So the
    # reconciler is `proxied` before the first job it starts moves, and `dagster` after the last.
    for job in jobs["jobs"]:
        for started in started_for(jobs, job):
            if stages[started] == "dagster" and stages[job["id"]] not in DAGSTER_RUNS:
                problems.append(f"{started}: Dagster decides it, but {job['id']}, which starts it when it is owed a "
                                f"run, runs in Airflow; {job['id']} moves to proxied first")
            if stages[job["id"]] == "dagster" and stages[started] != "dagster":
                problems.append(f"{job['id']}: Dagster decides it, but it starts {started}, which Airflow decides; "
                                f"{started} moves to dagster first")
    return problems


def requeued(jobs, job):
    """The jobs whose starts their pool refused `job` starts again from the host's start queue
    (`requeues_refused_starts_in`, its pools; root ADR-0462): every enabled job of those pools, besides
    the ones it reconciles. Which orchestrator starts an owed job is the job's stage (started_for)."""
    pools = job.get("requeues_refused_starts_in") or ()
    reconciled = set(job.get("reconciles", ()))
    return tuple(sorted(other["id"] for other in jobs["jobs"]
                        if other["pool"] in pools and other["enabled"] and other["id"] not in reconciled))


def started_for(jobs, job):
    """The jobs `job`'s run may say are owed a run: those it reconciles and those it requeues.

    Its run prints them (OWED_LINE). While Airflow decides the owed job, the reconciler's DAG triggers the
    job's DAG (foundation_jobs.py `select`, `start`); once Dagster decides it, the job's own sensor reads
    the reconciler's Dagster run (OWED_TAG, orchestration/dagster/decide.py). dagster_order holds the two
    to stages in which each can reach the other.
    """
    return tuple(job.get("reconciles", ())) + requeued(jobs, job)


def start_circles(jobs, graph=None):
    """Refuses jobs started by their inputs that start each other in a circle (root ADR-0171 §5): each
    changed run would start the next, forever."""
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    by_id = {job["id"]: job for job in jobs["jobs"]}
    feeders = producers(jobs, graph)
    done = set()

    def visit(job_id, path=()):
        if job_id in path:
            raise JobListError(f"jobs start each other in a circle: {' -> '.join(path + (job_id,))}")
        if job_id in done:
            return
        if started_by(by_id[job_id]) == "inputs":
            for feeder in feeders[job_id]:
                visit(feeder, path + (job_id,))
        done.add(job_id)

    for job_id in by_id:
        visit(job_id)


def started_by(job):
    """What starts a job: `schedule` or `inputs` (jobs.v1.json `started_by`, required)."""
    value = job.get("started_by")
    if value not in STARTED_BY:
        raise JobListError(f"{job['id']}: started_by must be one of {list(STARTED_BY)}")
    return value


def job_nodes(jobs, graph):
    """Job id -> (inputs, outputs): the pipeline-graph node ids each job reads and writes.

    A job's are the endpoints of the pipeline-graph edges it names; a job that checks the
    data-contract tables (`reads_data_contracts`) reads those tables and writes nothing; the asset
    reconciler (`reconciles`, root ADR-0333 §7) reads the inputs and outputs of the jobs it
    reconciles and writes nothing.
    """
    edges = {edge["id"]: edge for edge in graph["edges"]}
    found = {}
    reconcilers = []
    for job in jobs["jobs"]:
        job_id = job["id"]
        reads_contracts = job.get("reads_data_contracts", False)
        if "reconciles" in job:
            if job["pipeline_graph_edges"] or reads_contracts:
                raise JobListError(f"{job_id}: a job that reconciles others carries no pipeline-graph edge of its own")
            reconcilers.append(job)
            continue
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
            found[job_id] = (sorted({by_table[table] for table in tables}), [])
        else:
            found[job_id] = (sorted({edge["from"] for edge in carried}), sorted({edge["to"] for edge in carried}))
    for job in reconcilers:
        names = job["reconciles"]
        if not (isinstance(names, list) and names and all(name in found for name in names)
                and len(set(names)) == len(names)):
            raise JobListError(f"{job['id']}: reconciles must list other jobs of the list, each once")
        found[job["id"]] = (sorted({node for name in names for node in found[name][0] + found[name][1]}), [])
    return found


def by_pnu_lane_of(node_id):
    """The by-PNU lane a pipeline-graph serving node belongs to (`<lane>-by-pnu-section-packs`, or the
    parcel and building lanes' object serving `<lane>-by-pnu-serving`), or None."""
    for suffix in ("-by-pnu-section-packs", "-by-pnu-serving"):
        if node_id.endswith(suffix):
            return node_id.removesuffix(suffix)
    return None


def writer_of(node_id, jobs=None, graph=None):
    """The job whose pipeline-graph edges end at `node_id`, or None (the job that makes that asset)."""
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    # From the edges alone: the host scripts ask this, and nothing else of the job list need hold.
    ends = {edge["id"]: edge["to"] for edge in graph["edges"]}
    return next((job["id"] for job in jobs["jobs"] for edge in job.get("pipeline_graph_edges", [])
                 if ends.get(edge) == node_id), None)


def datasets(jobs, graph):
    """Job id -> (inputs, outputs): the datasets, (namespace, name), each job reads and writes."""
    nodes = {node["id"]: node for node in graph["nodes"]}

    def dataset(node_id):
        node = nodes[node_id]
        return (platform_of(node), name_of(node))

    return {
        job_id: (sorted({dataset(node) for node in inputs}), sorted({dataset(node) for node in outputs}))
        for job_id, (inputs, outputs) in job_nodes(jobs, graph).items()
    }


def producers(jobs, graph):
    """Job id -> the other jobs that write one of its inputs, in job-list order.

    This is the whole dependency graph between jobs: it is read from the edges each job carries
    out, so it cannot disagree with the lineage the runs report (root ADR-0171 §1).
    """
    found = datasets(jobs, graph)
    return {
        job["id"]: [other["id"] for other in jobs["jobs"]
                    if other["id"] != job["id"] and set(found[other["id"]][1]) & set(found[job["id"]][0])]
        for job in jobs["jobs"]
    }


def dependency_document(jobs=None, graph=None):
    """The job dependencies as foundation-api serves them: rendered here, never derived twice.

    Per job, in job-list order: the jobs that feed it (`producers`) and the datasets it reads and
    writes, each with its pipeline-graph node, the node's title and its kind, so a screen can name
    the job by what it makes and place it in its stage. The file is checked in (DEPENDENCIES) and a test fails when it differs from this
    (root ADR-0188).
    """
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in graph["nodes"]}
    feeders = producers(jobs, graph)
    carried = job_nodes(jobs, graph)

    def described(node_ids):
        return [
            {
                "dataset": asset_uri((platform_of(nodes[node_id]), name_of(nodes[node_id]))),
                "catalog_urn": urn_of(platform_of(nodes[node_id]), name_of(nodes[node_id])),
                "node_id": node_id,
                "title": nodes[node_id]["title"],
                "kind": nodes[node_id]["type"],
                "surface_kind": nodes[node_id].get("surface_kind"),
            }
            for node_id in node_ids
        ]

    return {
        "schema_version": DEPENDENCIES_SCHEMA,
        "generated_by": "orchestration/dags/job_specs.py --write-dependencies; do not edit by hand",
        "run_task_id": RUN_TASK_ID,
        "jobs": [
            {
                "id": job["id"],
                "producers": feeders[job["id"]],
                "reads": described(carried[job["id"]][0]),
                "writes": described(carried[job["id"]][1]),
            }
            for job in jobs["jobs"]
        ],
    }


def rendered_dependencies(jobs=None, graph=None):
    """dependency_document() exactly as the checked-in file holds it."""
    return json.dumps(dependency_document(jobs, graph), ensure_ascii=False, indent=2) + "\n"


def unpause_order(jobs=None, graph=None):
    """The enabled job ids in the order a deploy unpauses their DAGs: every job before the jobs it
    starts by its outputs (root ADR-0179).

    Airflow drops an output event for a DAG that is still paused. On 2026-10-10 a deploy unpaused
    the Gold rebuild before the bake; the rebuild's waiting `publish_outputs` ran within a second,
    while the bake was still paused, and the bake never started. Unpausing the consumer first means
    no event can reach a paused reader. Only `inputs` jobs consume events; others keep list order.
    """
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    enabled = [job for job in jobs["jobs"] if job["enabled"]]
    feeds = producers(jobs, graph)
    position = {job["id"]: index for index, job in enumerate(enabled)}
    readers = {job["id"]: [other["id"] for other in enabled
                           if started_by(other) == "inputs" and job["id"] in feeds[other["id"]]]
               for job in enabled}
    depth = {}

    def chain(job_id, path=()):
        if job_id not in depth:
            depth[job_id] = 1 + max((chain(reader, path + (job_id,)) for reader in readers[job_id]
                                     if reader not in path), default=0)
        return depth[job_id]

    return sorted((job["id"] for job in enabled), key=lambda job_id: (chain(job_id), position[job_id]))


def owed_by_inputs(specs, newest_event, newest_run, queued):
    """The enabled `inputs` jobs owed a run: an input changed after the job last started, and
    nothing has queued it (root ADR-0337). In job-list order.

    Airflow queues an output event only for the DAGs scheduled on it that are not paused
    (AssetManager.register_asset_change), and nothing replays it when the DAG is unpaused. On
    2026-10-28 a deploy held by_pnu_public_auction_bake paused from 21:19 to 21:40 while it had given
    gold_public_auction_rebuild its schedule back; the rebuild's 21:38 event (asset event 3912) queued
    nothing and the bake waited 13 hours for its fallback schedule. Unpause order (root ADR-0179)
    cannot prevent this while a deploy holds a reader and not its writers, or a writer's run is in
    flight, so the deploy asks this after it unpauses: what the state is, not which events it missed.

    - newest_event: asset name (asset_uri) -> when its newest event was recorded
    - newest_run: DAG id -> when its newest run was queued, of any kind (a run started after the
      change read it: the fallback schedule, an operator, or the event itself)
    - queued: DAG ids Airflow holds an asset queue entry for (it will start them itself)

    A job switched on later is owed nothing for a change before enabled_since (root ADR-0239).
    """
    owed = []
    for spec in specs:
        # A job Dagster decides has no DAG to miss an event: its sensor keeps the events that came while it
        # was stopped and starts one run for them (orchestration/dagster/decide.py).
        if not spec.enabled or spec.started_by != "inputs" or spec.dag_id in queued or spec.dagster_stage == "dagster":
            continue
        changes = [newest_event[asset_uri(dataset)] for dataset in spec.starting_inputs
                   if newest_event.get(asset_uri(dataset)) is not None]
        if not changes:
            continue
        since = [moment for moment in (newest_run.get(spec.dag_id), _instant(spec.enabled_since)) if moment is not None]
        if not since or max(changes) > max(since):
            owed.append(spec.job_id)
    return owed


def _instant(text):
    """A jobs.v1.json `YYYY-MM-DDTHH:MM:SSZ` as an aware UTC datetime, or None."""
    return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def load_specs(jobs=None, graph=None):
    jobs = jobs if jobs is not None else json.loads(JOBS.read_text(encoding="utf-8"))
    graph = graph if graph is not None else json.loads(GRAPH.read_text(encoding="utf-8"))
    if jobs.get("schema_version") != JOBS_SCHEMA:
        raise JobListError(f"jobs schema_version is not {JOBS_SCHEMA}")
    # The DAG ids are declared once (jobs.v1.json `dag_id_prefix`): foundation-api reads the same
    # field to find each job's runs in the data catalog (root ADR-0165).
    dag_id_prefix = jobs.get("dag_id_prefix")
    if not isinstance(dag_id_prefix, str) or not re.fullmatch(r"[a-z][a-z0-9_]*_", dag_id_prefix):
        raise JobListError("jobs.v1.json dag_id_prefix must be a lower_snake name ending in '_'")

    pools = {DEFAULT_POOL, *declared_pools(jobs)}
    capacity_claimants(jobs, pools)
    memory_claims(jobs)
    found = datasets(jobs, graph)
    feeders = producers(jobs, graph)
    by_id = {job["id"]: job for job in jobs["jobs"]}
    for job in jobs["jobs"]:
        priority_weight(job)
    weights = queue_weights(jobs, feeders)

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
        priority_class(jobs, job, job_id)
        inherited_class(job, by_id, found)
        if not isinstance(job.get("enabled"), bool):
            raise JobListError(f"{job_id}: enabled must be true or false")
        reason = job.get("disabled_reason")
        if job["enabled"] and reason is not None:
            raise JobListError(f"{job_id}: an enabled job has no disabled_reason")
        # When the job was switched on (root ADR-0239): no start before it is a start it missed.
        since = job.get("enabled_since")
        if job["enabled"] and not (isinstance(since, str) and ENABLED_SINCE.fullmatch(since)):
            raise JobListError(f"{job_id}: an enabled job states enabled_since as YYYY-MM-DDTHH:MM:SSZ")
        if not job["enabled"] and since is not None:
            raise JobListError(f"{job_id}: a disabled job has no enabled_since")
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
        check = job.get("check_service")
        if check is not None:
            if not (isinstance(check, str) and SERVICE_NAME.fullmatch(check)):
                raise JobListError(f"{job_id}: check_service {check!r} is not a foundation-*.service name")
            if not service_unit_file(check).is_file():
                raise JobListError(f"{job_id}: infra/systemd has no unit for {check}")
            if check in seen_services:
                raise JobListError(f"{job_id}: {check} already belongs to another job")
            seen_services.add(check)
        timer = job["systemd_timer"]
        if timer is not None and not TIMER_NAME.fullmatch(timer):
            raise JobListError(f"{job_id}: {timer!r} is not a foundation-*.timer name")
        inputs, outputs = found[job_id]
        start = started_by(job)
        if start == "inputs":
            # Started by its inputs, it must have a job that can change one (otherwise only its
            # fallback schedule would ever start it). An input it writes itself does not start it
            # (JobSpec.starting_inputs, root ADR-0182), so a run never starts the next one.
            if not feeders[job_id]:
                raise JobListError(f"{job_id}: started by its inputs, but no job in the list writes any of them")
            if job["enabled"] and not any(by_id[feeder]["enabled"] for feeder in feeders[job_id]):
                raise JobListError(
                    f"{job_id}: enabled and started by its inputs, but every job that writes them "
                    f"({', '.join(feeders[job_id])}) is switched off"
                )
        reconciles = tuple(job.get("reconciles", ()))
        for name in reconciles:
            # Root ADR-0333 §7: the reconciler starts a job whose one asset's inputs moved past what it
            # read. It reads a Gold's recorded pins or a lane's reads; a job making several assets (the
            # 2026-10-28 panel rebuild that cut gold.unit_price off) has no one read version to compare.
            other = by_id[name]
            if started_by(other) != "inputs" or not other["enabled"] and job["enabled"]:
                raise JobListError(f"{job_id}: reconciles {name}, which is not an enabled `inputs` job")
            # A by-PNU bake serving several lanes is the exception: each lane records its own reads. So
            # is a job making several Gold tables (the panel rebuild, root ADR-0333 §7 stage 2): each records its own
            # pins; a table it makes and reads itself (the unit price join) is a step of its run.
            made = found[name][1]
            golds = [node for dataset in made if dataset not in found[name][0] for node in dataset[1:]]
            if len(made) != 1 and not all(by_pnu_lane_of(node) for _, node in made) \
                    and not (golds and all(node.startswith("gold.") for node in golds)):
                raise JobListError(f"{job_id}: reconciles {name}, which does not make exactly one asset")
        requeues = requeued(jobs, job)  # dagster_order holds the stages of these to the reconciler's
        unknown = set(job.get("requeues_refused_starts_in") or ()) - set(declared_pools(jobs))
        if unknown or requeues and not reconciles:
            raise JobListError(f"{job_id}: requeues_refused_starts_in names pools the list bounds"
                               f" ({sorted(unknown) or 'its own'}), and only the reconciler requeues")
        if reconciles and (start != "schedule" or job["pool"] != DEFAULT_POOL):
            raise JobListError(f"{job_id}: a reconciler runs on its own schedule in {DEFAULT_POOL}, never on an event")
        no_slot_count(job, job_id)
        specs.append(
            JobSpec(
                job_id=job_id,
                dag_id=f"{dag_id_prefix}{job_id}",
                description=job["description"],
                schedule=job["schedule"],
                timeout_minutes=scheduler_timeout_minutes(job),
                pool=job["pool"],
                retries=retries(job),
                priority_weight=weights[job_id],
                systemd_service=service,
                systemd_timer=timer,
                enabled=job["enabled"],
                inputs=inputs,
                outputs=outputs,
                started_by=start,
                producers=tuple(feeders[job_id]),
                check_service=check,
                enabled_since=since,
                reconciles=reconciles,
                requeues=requeues,
                dagster_stage=dagster_stage(job, jobs),
                dagster_group=dagster_group(job),
                priority=jobs["priority_classes"][priority_class(jobs, job, job_id)]["value"],
            )
        )
    problems = [f"{spec.job_id}: queue weight {spec.priority_weight} reaches QUEUE_WEIGHT_SPAN; raise the span"
                for spec in specs if spec.priority_weight >= QUEUE_WEIGHT_SPAN]
    start_circles(jobs, graph)
    problems += dagster_order(jobs, feeders) + dagster_groups(jobs)
    if problems:
        raise JobListError("; ".join(problems))
    return specs


if __name__ == "__main__":
    # `--write-dependencies` renders DEPENDENCIES; `--check-dependencies` fails when it is stale.
    if sys.argv[1:] == ["--write-dependencies"]:
        DEPENDENCIES.write_bytes(rendered_dependencies().encode("utf-8"))
    elif sys.argv[1:] == ["--check-dependencies"]:
        if DEPENDENCIES.read_bytes() != rendered_dependencies().encode("utf-8"):
            sys.exit(f"{DEPENDENCIES} is stale; run: python orchestration/dags/job_specs.py --write-dependencies")
    elif sys.argv[1:] == ["--check-state-roots"]:
        found = state_path_problems()
        if found:
            sys.exit("\n".join(found))
    elif len(sys.argv) == 3 and sys.argv[1] == "--state-root":
        print(job_state_root(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "--writer":
        # The job that makes a pipeline-graph node, or nothing (root ADR-0333: one job, one asset).
        print(writer_of(sys.argv[2]) or "")
    elif sys.argv[1:] == ["--outcome-line"]:
        # Reads a run's journal on stdin; prints its outcome line, or nothing (root ADR-0260 §2).
        line = outcome_line(sys.stdin.buffer.read())
        if line:
            print(line)
    else:
        sys.exit("usage: job_specs.py --write-dependencies | --check-dependencies | --check-state-roots"
                 " | --state-root <job id> | --writer <node id> | --outcome-line")
