"""The data contracts' machine rules as Dagster asset checks (root ADR-0609 §2, ADR-0432 §5).

Every contract file (contracts/data/*.odcs.yaml, generated from the lakehouse contracts and the pipeline
graph, root ADR-0123) declares, on the graph node whose `table_name` is its `physicalName`:

- `contract_columns_present`, when it names columns: every column it names is in the physical table;
- `contract_allowed_values_<column>`, one per `invalidValues` rule: every present value of the column
  is one of its `validValues`;
- `contract_region_codes_paired`, when it is a Silver table keyed by `pnu`: no row's PNU is under an
  abolished 법정동 code the change table has no pair for (a Gold reader translates a code only through
  a pair, so such a row reaches no served parcel).

No asset key, table or column is written here: a rule is added, changed or removed by its contract.
The quality job's unit (scripts/ops/data-quality-check.py) runs the rules with the publisher's DuckDB
and reports each result through its journal as a Pipes `report_asset_check` line; the op that ran the
unit logs it as the check's evaluation (run_jobs.py, systemd_pipes.SystemdRun.check_evaluations). That
job, REPORTER, is the one run that judges tables it does not write: judging what others write is its
work. The unit gives the severity (WARN while a contract is report-only, ERROR once enforced); a check
never blocks, since no run that materializes the table runs it. Run on its own, a check re-states the
newest evaluation the quality job reported.
"""

import functools
import pathlib

import yaml
from dagster import AssetCheckKey, AssetCheckResult

import observe_assets
from asset_checks import Declared, Kind
from takeover_checks import RESTATED_FROM, reported_evaluation

# The definition group the checks are declared under, and the one job whose unit reports them.
GROUP = "data-quality"
REPORTER = "data_quality"
# Every contract check's name starts so, and no other check's does.
PREFIX = "contract_"
COLUMNS_PRESENT = PREFIX + "columns_present"
ALLOWED_VALUES = PREFIX + "allowed_values"
REGION_CODES_PAIRED = PREFIX + "region_codes_paired"
PNU = "pnu"


def allowed_values_check(column):
    return f"{ALLOWED_VALUES}_{column}"


def is_allowed_values(rule):
    """An `invalidValues` rule with its validValues: the one rule kind the quality job runs
    (data-quality-check.py reads it the same way)."""
    return rule.get("metric") == "invalidValues" and "validValues" in (rule.get("arguments") or {})


def has_region_rule(table):
    """Whether the region rule holds for a contract's table: a Silver table keyed by `pnu`
    (data-quality-check.py reads it the same way)."""
    return table["physicalName"].startswith("silver.") and any(
        prop["name"] == PNU for prop in table.get("properties") or [])


def rules_of(contract):
    """(physical table, the checks its contract's machine rules make), in the file's column order; a
    column with two allowed-values rules is one check."""
    (table,) = contract["schema"]
    properties = table.get("properties") or []
    checks = [COLUMNS_PRESENT] if properties else []
    checks += [allowed_values_check(prop["name"]) for prop in properties
               for rule in prop.get("quality") or [] if is_allowed_values(rule)]
    checks += [REGION_CODES_PAIRED] if has_region_rule(table) else []
    return table["physicalName"], list(dict.fromkeys(checks))


def contract_rules(directory=None):
    """Physical table -> its contract's checks, of every contract file. Read once per process: every
    run of every job asks for the checks it reports (run_jobs.reported_checks), and the files change
    only with a release, which reloads the code location."""
    return dict(_contract_rules(str(observe_assets.job_specs.CONTRACTS if directory is None else directory)))


@functools.cache
def _contract_rules(directory):
    found = {}
    for path in sorted(pathlib.Path(directory).glob("*.odcs.yaml")):
        table, checks = rules_of(yaml.safe_load(path.read_text(encoding="utf-8")))
        if table in found:
            raise observe_assets.job_specs.JobListError(f"two contracts describe {table}")
        found[table] = checks
    return tuple(found.items())


def kind_of(check):
    """The kind a contract check is of, or None for any other check. Every check named `contract_*` is
    one: no definition may declare such a name (asset_checks.declared), so the prefix is the rule
    foundation-api reads them by too (facts/dagster.rs)."""
    if check in (COLUMNS_PRESENT, REGION_CODES_PAIRED):
        return check
    if check.startswith(ALLOWED_VALUES + "_"):
        return ALLOWED_VALUES
    return None


def declared(graph, keys, rules=None):
    """The contract checks, each on the graph node its contract's table is; a contract whose table no
    node names is refused (the contracts are generated from the graph, so it is drift)."""
    rules = contract_rules() if rules is None else rules
    nodes = {node["table_name"]: node for node in graph["nodes"] if node.get("table_name")}
    missing = sorted(set(rules) - set(nodes))
    if missing:
        raise observe_assets.job_specs.JobListError(f"contracts describe tables no pipeline-graph node names: {missing}")
    return [Declared(GROUP, check, nodes[table], (), False, keys, kind=kind_of(check))
            for table, checks in sorted(rules.items()) for check in checks]


def restate(context, check):
    """The newest evaluation the quality job reported, re-stated; a failure when there is none yet."""
    found = reported_evaluation(context.instance, AssetCheckKey(check.key(), check.check), context.run_id)
    if found is None:
        return AssetCheckResult(passed=False, severity=check.severity,
                                metadata={"reason": "the quality job has not reported this check yet; its next run does",
                                          RESTATED_FROM: ""})
    run_id, evaluation = found
    metadata = {name: value.value for name, value in (evaluation.metadata or {}).items()}
    return AssetCheckResult(passed=evaluation.passed, severity=evaluation.severity,
                            metadata={**metadata, RESTATED_FROM: run_id})


KINDS = {
    COLUMNS_PRESENT: Kind(
        description=("Does the physical table hold every column its data contract names? Reported by the "
                     "quality job (data-quality-check.py); run alone, it re-states the newest report."),
        evaluate=restate,
    ),
    ALLOWED_VALUES: Kind(
        description=("Is every present value of the column one of the values its data contract allows? "
                     "Reported by the quality job (data-quality-check.py); run alone, it re-states the newest report."),
        evaluate=restate,
    ),
    REGION_CODES_PAIRED: Kind(
        description=("Is every row's PNU under a 법정동 code that is current, or abolished with a pair in the "
                     "change table? An abolished code with no pair reaches no served parcel. Reported by the "
                     "quality job (data-quality-check.py); run alone, it re-states the newest report."),
        evaluate=restate,
    ),
}
