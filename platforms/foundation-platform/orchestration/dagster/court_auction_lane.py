"""The court auction Silver load as Dagster assets, with no host under it (root ADR-0616's reference lane).

Today the load is a systemd unit Dagster starts over SSH; scripts/ops/court-auction-silver-load.sh
plans what is owed, runs the publisher, and says how it went on an outcome line (run_jobs.py). Here
the same publisher binary is one multi-asset: Dagster starts it as a subprocess, reads the Pipes
lines it already prints, and records each table it wrote. What the wrapper decided is the asset's:

- what is owed: `plan-silver-load-objects`, once per (table, source), as the wrapper asked it;
- nothing owed: no table is materialized and nothing downstream starts (every output is skippable);
- the load: `load-courtauction-go-kr-silver` over the plans, one process, a commit per table;
- its result checks: the `report_asset_check` lines the load prints (rows per Bronze object, key
  uniqueness, required columns), logged as the load reported them;
- a Bronze object that waited longer than the load's window: loaded, then the run fails, so the gap
  is seen (the wrapper's rule).

It reads no host path, unit, journal or state directory: the work directory is the run's own
temporary one, the publisher is the program named by `FOUNDATION_PLATFORM_PUBLISHER_BIN`, and the
object store, catalog and Bronze ledger are whatever the process environment names. The same
definition runs the binary in a container once the Pipes client is the container platform's.

NOT part of the code location's definitions (definitions.py): the tables are observed there, written
by the job `court_auction_silver`, and an asset key has one definition. `lane_definitions()` is this
lane's own, for the day the job leaves the host.
"""

import json
import os
import pathlib
import subprocess
import tempfile

import observe_assets
import systemd_pipes

job_specs = observe_assets.job_specs

JOB_ID = "court_auction_silver"
PUBLISHER_ENV = "FOUNDATION_PLATFORM_PUBLISHER_BIN"
CONTRACT = job_specs.PLATFORM_ROOT / "infra" / "lakehouse" / "contracts" / "courtauction-go-kr.contract.json"
SOURCE_PREFIX = "courtauctiongokr__"
PLAN_COMMAND = "plan-silver-load-objects"
LOAD_COMMAND = "load-courtauction-go-kr-silver"
PLAN_ROOT_ENV = "FOUNDATION_PLATFORM_COURT_AUCTION_PLAN_ROOT"
SUMMARY_PREFIX = "courtauction-go-kr-duckdb-summary-json "


def plans(contract=None):
    """[(plan directory name, the table whose load record says what is owed, the Bronze source slug)],
    from the source contract: the plans court-auction-silver-load.sh made, in its order."""
    contract = json.loads(CONTRACT.read_text(encoding="utf-8")) if contract is None else contract
    slug = lambda section: SOURCE_PREFIX + section["dataset_slug"]  # noqa: E731
    case, survey, appraisal, filing, tenant = (slug(document) for document in contract["case_documents"]["documents"])
    read = "silver.court_auction_case_document_read"
    return [
        ("items", "silver.court_auction_item", slug(contract["item_search"])),
        ("results", "silver.court_auction_sale_result", slug(contract["sale_result_search"])),
        ("details", "silver.court_auction_item_detail", slug(contract["item_detail"])),
        ("documents", "silver.court_auction_appraisal_point", slug(contract["item_detail"])),
        ("codes", "silver.court_auction_code", slug(contract["common_codes"])),
        ("cases", read, case),
        ("surveys", read, survey),
        ("appraisals", read, appraisal),
        ("filings", read, filing),
        ("tenants", read, tenant),
        ("reports", "silver.court_auction_appraisal_report_read", slug(contract["appraisal_reports"])),
        ("specs", "silver.court_auction_sale_specification_read", slug(contract["sale_specifications"])),
        ("notices", "silver.court_auction_sale_notice_item", slug(contract["sale_notices"])),
        ("dividends", "silver.court_auction_dividend_object", slug(contract["dividend_notices"])),
    ]


def window(contract=None):
    """(window days, the most objects one plan takes) of the load, from the source contract."""
    contract = json.loads(CONTRACT.read_text(encoding="utf-8")) if contract is None else contract
    return int(contract["silver_load"]["window_days"]), int(contract["silver_load"]["max_objects"])


def output_keys(jobs=None, graph=None):
    """The asset keys of the tables the load writes: the job's outputs in the pipeline graph."""
    if jobs is None or graph is None:
        jobs, graph = observe_assets.contracts()
    spec = next(spec for spec in job_specs.load_specs(jobs, graph) if spec.job_id == JOB_ID)
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


def owed(work, log, contract=None):
    """Plans every (table, source) into `work`; returns (objects owed, objects that waited past the window)."""
    days, most = window(contract)
    count, late = 0, 0
    for name, table, source in plans(contract):
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


def summary_of(lines):
    """The load's summary line as an object, or {}."""
    found = next((line for line in reversed(lines) if line.startswith(SUMMARY_PREFIX)), None)
    return json.loads(found[len(SUMMARY_PREFIX):]) if found else {}


def silver(jobs=None, graph=None, contract=None):
    """The multi-asset of the court auction Silver tables."""
    from dagster import AssetExecutionContext, AssetSpec, Failure, MaterializeResult, multi_asset

    keys = output_keys(jobs, graph)

    @multi_asset(name=JOB_ID, group_name="court_auction", can_subset=False,
                 specs=[AssetSpec(key, skippable=True, kinds={"iceberg"}) for key in keys],
                 description="Loads the court auction Bronze objects no load has recorded into the Silver tables, "
                             "in the publisher's own process.")
    def court_auction_silver(context: AssetExecutionContext):
        with tempfile.TemporaryDirectory(prefix="court-auction-silver-") as work:
            count, late = owed(work, context.log, contract)
            if count == 0:
                context.log.info("nothing is owed: no table is written and nothing downstream starts")
                return
            lines = publisher(LOAD_COMMAND, {PLAN_ROOT_ENV: work}, context.log)
        metadata, evaluations = reported(lines, keys)
        summary = summary_of(lines)
        for key in keys:
            yield MaterializeResult(asset_key=key, metadata={**metadata.get(key, {}), "objects_owed": count})
        for evaluation in evaluations:
            context.log_event(evaluation)
        if late:
            days, _most = window(contract)
            raise Failure(f"{late} Bronze object(s) waited more than {days} days unloaded; they are loaded now, "
                          "and this run fails so the gap is seen", metadata={"late": late, "summary": summary})

    return court_auction_silver


def lane_definitions(jobs=None, graph=None, contract=None):
    """The lane's own Dagster definitions (not the code location's: see the module docstring)."""
    from dagster import Definitions, in_process_executor

    return Definitions(assets=[silver(jobs, graph, contract)], executor=in_process_executor)
