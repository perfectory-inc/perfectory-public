"""The 국토교통부 실거래가 Silver load as a Dagster multi-asset, with no host under it (root ADR-0348,
ADR-0349, ADR-0559; the shape of root ADR-0616 §8, publisher_lane.silver_load).

`DuckDB` inside the publisher loads it (`load-rt-molit-real-transaction-silver`): the Bronze exports no
commit of silver.real_transaction records, oldest landing first, each in its own commit, the lines not
loaded first to silver.real_transaction_rejected. This module says only what is owed: one plan of
silver.real_transaction over every dataset of the source contract, within its window.

NOT part of the code location's definitions (definitions.py): until the lane moves to AWS (root
ADR-0616 §7 A4, after its collector moves) the Spark job `rt_molit_real_transaction_silver` loads these
tables on the data host, and an asset key has one definition. `lane_definitions()` is this lane's own,
for the day the job leaves the host; the pull request that moves it turns the Spark job off.
"""

import json

import publisher_lane

job_specs = publisher_lane.job_specs

JOB_ID = "rt_molit_real_transaction_silver"
CONTRACT = job_specs.PLATFORM_ROOT / "infra" / "lakehouse" / "contracts" / "rt-molit-real-transaction.contract.json"
SOURCE_PREFIX = "rtmolitgokr__"
TABLE = "silver.real_transaction"
LOAD_COMMAND = "load-rt-molit-real-transaction-silver"
PLAN_ROOT_ENV = "FOUNDATION_PLATFORM_RT_MOLIT_PLAN_ROOT"
SUMMARY_PREFIX = "rt-molit-real-transaction-summary-json "


def plans(contract=None):
    """[(plan directory name, the table whose load record says what is owed, the Bronze source slugs)]:
    one plan of every dataset the collector lands, sorted, as the Spark job's `tables` named them."""
    contract = json.loads(CONTRACT.read_text(encoding="utf-8")) if contract is None else contract
    return [("real_transaction", TABLE, ",".join(SOURCE_PREFIX + name for name in sorted(contract["datasets"])))]


def window(contract=None):
    """(window days, the most objects one plan takes) of the load, from the source contract."""
    contract = json.loads(CONTRACT.read_text(encoding="utf-8")) if contract is None else contract
    return int(contract["silver_load"]["window_days"]), int(contract["silver_load"]["max_objects"])


def output_keys(jobs=None, graph=None):
    """The asset keys of the tables the load writes: the job's outputs in the pipeline graph."""
    return publisher_lane.output_keys(JOB_ID, jobs, graph)


def silver(jobs=None, graph=None, contract=None):
    """The multi-asset of the 실거래가 Silver tables."""
    return publisher_lane.silver_load({
        "job_id": JOB_ID, "group": "real_transaction", "jobs": jobs, "graph": graph,
        "description": "Loads the 국토교통부 실거래가 exports no load has recorded into silver.real_transaction, "
                       "and the lines not loaded into silver.real_transaction_rejected, in the publisher's own process.",
        "plans": plans(contract), "window": window(contract), "load_command": LOAD_COMMAND,
        "plan_root_env": PLAN_ROOT_ENV, "summary_prefix": SUMMARY_PREFIX,
    })


def lane_definitions(jobs=None, graph=None, contract=None):
    """The lane's own Dagster definitions (not the code location's: see the module docstring)."""
    from dagster import Definitions, in_process_executor

    return Definitions(assets=[silver(jobs, graph, contract)], executor=in_process_executor)
