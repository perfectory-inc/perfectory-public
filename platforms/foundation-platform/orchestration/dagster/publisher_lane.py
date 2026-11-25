"""A Silver load as one Dagster multi-asset that runs the publisher binary, with no host under it
(root ADR-0616 §8: the reference lane's shape, shared by every Silver lane that takes it).

A lane says what it loads: its (plan, table, Bronze source) triples, the load's window, the publisher
command that loads the plans and the prefix of that command's summary line. This module does the
rest, the same for every lane:

- what is owed: `plan-silver-load-objects`, once per (table, source), each into its own directory;
- nothing owed: no table is materialized and nothing downstream starts (every output is skippable);
- the load: the lane's command over the plan root, one process;
- its result checks: the `report_asset_check` lines the load prints (rows per Bronze object, key
  uniqueness, required columns), logged as the load reported them; a report on a table the load
  does not write fails the run;
- a Bronze object that waited longer than the load's window: loaded, then the run fails, so the gap
  is seen.

It reads no host path, unit, journal or state directory: the work directory is the run's own
temporary one, the publisher is the program named by `FOUNDATION_PLATFORM_PUBLISHER_BIN`, and the
object store, catalog and Bronze ledger are whatever the process environment names.
"""

import json
import os
import pathlib
import subprocess
import tempfile

import observe_assets
import systemd_pipes

job_specs = observe_assets.job_specs

PUBLISHER_ENV = "FOUNDATION_PLATFORM_PUBLISHER_BIN"
PLAN_COMMAND = "plan-silver-load-objects"


def output_keys(job_id, jobs=None, graph=None):
    """The asset keys of the tables job `job_id` writes: its outputs in the pipeline graph."""
    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    spec = next(spec for spec in job_specs.load_specs(jobs, graph) if spec.job_id == job_id)
    return [observe_assets.asset_key(dataset) for dataset in spec.outputs]


def publisher(command, env, log):
    """Runs the publisher's `command` with `env` over the process environment; returns its lines.
    Every line that is not a Pipes message is the step's log."""
    from dagster import Failure

    program = os.environ.get(PUBLISHER_ENV)
    if not program:
        raise Failure(f"{PUBLISHER_ENV} names no publisher binary")
    process = subprocess.Popen([program, command], env={**os.environ, **env}, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    lines = []
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        if systemd_pipes.pipes_message(line) is None:
            log.info(line.rstrip("\n"))
    status = process.wait()
    if status != 0:
        raise Failure(f"the publisher's {command} exited {status}", metadata={"last_line": lines[-1] if lines else ""})
    return lines


def owed(work, plans, window, log):
    """Plans every (table, source) of `plans` into `work`; returns (objects owed, objects that waited
    past the window). `window` is (window days, the most objects one plan takes)."""
    days, most = window
    count, late = 0, 0
    for name, table, source in plans:
        directory = pathlib.Path(work) / name
        publisher(PLAN_COMMAND, {
            "FOUNDATION_PLATFORM_SILVER_LOAD_TABLE": table,
            "FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS": source,
            "FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS": str(days),
            "FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS": str(most),
            "FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR": str(directory),
        }, log)
        plan = json.loads((directory / "plan.json").read_text(encoding="utf-8"))
        count += len(plan["objects"])
        late += len((plan.get("selection") or {}).get("late", []))
    return count, late


def reported(lines, keys):
    """(per-table metadata, check evaluations) of the load's Pipes lines. A report on a table the load
    does not write is refused, as run_jobs refuses it."""
    from dagster import AssetCheckEvaluation, AssetKey, Failure

    outputs = {AssetKey(list(key.path)) for key in keys}
    metadata, evaluations = {}, []
    for line in lines:
        message = systemd_pipes.pipes_message(line)
        if message is None:
            continue
        params = message.get("params") or {}
        if message.get("method") not in ("report_asset_materialization", "report_asset_check"):
            continue
        key = AssetKey.from_user_string(params.get("asset_key") or "")
        if key not in outputs:
            raise Failure(f"the load reported on {key.to_user_string()}, which it does not write")
        if message["method"] == "report_asset_materialization":
            metadata.setdefault(key, {}).update(systemd_pipes.raw_metadata(params))
        else:
            evaluations.append(AssetCheckEvaluation(
                asset_key=key, check_name=params.get("check_name") or "", passed=params.get("passed") is True,
                metadata=systemd_pipes.raw_metadata(params)))
    return metadata, evaluations


def summary_of(lines, prefix):
    """The load's summary line (the one that starts with `prefix`) as an object, or {}."""
    found = next((line for line in reversed(lines) if line.startswith(prefix)), None)
    return json.loads(found[len(prefix):]) if found else {}


def silver_load(lane):
    """The multi-asset of `lane`'s Silver tables. `lane` is a mapping: `job_id` (the job whose outputs
    in the pipeline graph are the tables), `group`, `description`, `plans` [(directory, table, source)],
    `window` (days, most objects a plan takes), `load_command`, `plan_root_env` (where the load finds
    the plans), `summary_prefix`, and the contracts `jobs`/`graph` (None reads them)."""
    from dagster import AssetExecutionContext, AssetSpec, Failure, MaterializeResult, multi_asset

    keys = output_keys(lane["job_id"], lane.get("jobs"), lane.get("graph"))

    @multi_asset(name=lane["job_id"], group_name=lane["group"], can_subset=False,
                 specs=[AssetSpec(key, skippable=True, kinds={"iceberg"}) for key in keys],
                 description=lane["description"])
    def silver_tables(context: AssetExecutionContext):
        with tempfile.TemporaryDirectory(prefix=f"{lane['job_id']}-") as work:
            count, late = owed(work, lane["plans"], lane["window"], context.log)
            if count == 0:
                context.log.info("nothing is owed: no table is written and nothing downstream starts")
                return
            lines = publisher(lane["load_command"], {lane["plan_root_env"]: work}, context.log)
        metadata, evaluations = reported(lines, keys)
        summary = summary_of(lines, lane["summary_prefix"])
        for key in keys:
            yield MaterializeResult(asset_key=key, metadata={**metadata.get(key, {}), "objects_owed": count})
        for evaluation in evaluations:
            context.log_event(evaluation)
        if late:
            raise Failure(f"{late} Bronze object(s) waited more than {lane['window'][0]} days unloaded; they are "
                          "loaded now, and this run fails so the gap is seen", metadata={"late": late, "summary": summary})

    return silver_tables
