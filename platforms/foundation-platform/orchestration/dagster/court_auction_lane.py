"""The court auction Silver load as Dagster assets, with no host under it (root ADR-0616's reference lane).

Today the load is a systemd unit Dagster starts over SSH; scripts/ops/court-auction-silver-load.sh
plans what is owed, runs the publisher, and says how it went on an outcome line (run_jobs.py). Here
the same publisher binary is one multi-asset (publisher_lane.silver_load, which every Silver lane of
this shape shares): Dagster starts it as a subprocess, reads the Pipes lines it already prints, and
records each table it wrote. This module says only what the wrapper asked for: the (table, source)
plans in its order, the window from the source contract, and `load-courtauction-go-kr-silver` over
the plans, one process, a commit per table. The same definition runs the binary in a container once
the Pipes client is the container platform's.

NOT part of the code location's definitions (definitions.py): the tables are observed there, written
by the job `court_auction_silver`, and an asset key has one definition. `lane_definitions()` is this
lane's own, for the day the job leaves the host.
"""

import json

import publisher_lane

job_specs = publisher_lane.job_specs

JOB_ID = "court_auction_silver"
PUBLISHER_ENV = publisher_lane.PUBLISHER_ENV
CONTRACT = job_specs.PLATFORM_ROOT / "infra" / "lakehouse" / "contracts" / "courtauction-go-kr.contract.json"
SOURCE_PREFIX = "courtauctiongokr__"
PLAN_COMMAND = publisher_lane.PLAN_COMMAND
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
    return publisher_lane.output_keys(JOB_ID, jobs, graph)


def silver(jobs=None, graph=None, contract=None):
    """The multi-asset of the court auction Silver tables."""
    return publisher_lane.silver_load({
        "job_id": JOB_ID, "group": "court_auction", "jobs": jobs, "graph": graph,
        "description": "Loads the court auction Bronze objects no load has recorded into the Silver tables, "
                       "in the publisher's own process.",
        "plans": plans(contract), "window": window(contract), "load_command": LOAD_COMMAND,
        "plan_root_env": PLAN_ROOT_ENV, "summary_prefix": SUMMARY_PREFIX,
    })


def lane_definitions(jobs=None, graph=None, contract=None):
    """The lane's own Dagster definitions (not the code location's: see the module docstring)."""
    from dagster import Definitions, in_process_executor

    return Definitions(assets=[silver(jobs, graph, contract)], executor=in_process_executor)
