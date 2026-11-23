#!/usr/bin/env python3
"""Runs the data contracts' executable checks and reports each result as a Dagster asset check
(root ADR-0609 §2, ADR-0123).

For every contract in contracts/data, its machine rules are read from the contract file itself and
run against the lakehouse with the publisher's DuckDB (`lakehouse-query`: the catalog's read-only token
and the read-only key pair, root ADR-0565), each table read at its `main` snapshot:

- schema (superset, `contract_columns_present`): every column the contract names must exist in the
  physical table. Every contract that names a column has this rule.
- allowed values (`contract_allowed_values_<column>`, `invalidValues` with validValues): rows whose
  value is present and not in the contract's validValues are counted; any such row fails the rule.

Each result is one Dagster Pipes `report_asset_check` line on stdout, the unit's journal; the op that
ran the unit logs it as the evaluation of the check contract_checks.py declares from the same file
(orchestration/dagster/run_jobs.py). Dagster's event log keeps every evaluation: that is the
append-only record of what was checked. The results went to DataHub's assertions until 2026-11-23, and
DataHub held 35 of the contracts, so the others' results were only printed
(docs/designs/data-catalog-datahub-vs-dagster.md).

A contract checked for the first time is report-only for REPORT_ONLY_DAYS: its failures are reported at
WARN, printed as `warned`, and do not fail the run, so a rule nobody ran before does not page on its
first day. After that, and for a contract checked before, a failure is reported at ERROR and fails the
run. The first check's time is kept in the unit's state directory (first-checked.json).

One publisher run describes every contract's table (its `main` snapshot, and its current schema's
columns: a column added after the newest snapshot exists); a second counts the tables whose snapshot or
rules changed, at the snapshot the first read, so a count is always of the snapshot its result records.
Each allowed-values result records the table's Iceberg snapshot and a fingerprint of the rules it
counted. A table whose current snapshot and rules equal those of every rule's latest evaluation in
Dagster (its read-only webserver, jobs.v1.json observers.dagster.ui) is not counted again (a full scan
of an unchanged table answers the same), and those results still count toward the run: a failure
stays a failure until the data or the rule changes. Rules written as text are not run and are not
reported: nothing pretends to have checked them. A table the lakehouse does not hold yet is counted
and named, not failed: a declared status of "implemented" means the code path exists (pipeline graph
status policy), not that a load has run.

Exits 1 when any enforced rule failed, so the scheduler's run fails and alerts. The summary counts
contracts with machine rules, contracts checked (their table present) and checks run.
"""

import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request

import yaml

import internal_http  # beside this file: the daily jobs' one retry for the services they call

RELEASE = pathlib.Path(os.environ.get("FOUNDATION_DATA_QUALITY_RELEASE", "/opt/foundation-platform/current"))
CONTRACTS = RELEASE / "contracts" / "data"
JOBS = RELEASE / "orchestration" / "jobs.v1.json"
# Only to learn once which contracts DataHub had judged before (root ADR-0609 §2; removed with DataHub).
GMS = os.environ.get("FOUNDATION_DATA_QUALITY_GMS_URL", "http://127.0.0.1:18095")
# The admitted release's publisher (admitted-writer-runtime.sh), and where its DuckDB keeps its
# database and spill: the unit's StateDirectory. Its memory leaves the script room in the unit's 512M.
ADMITTED_RUNTIME = pathlib.Path(__file__).resolve().parent / "admitted-writer-runtime.sh"
STATE = pathlib.Path(os.environ.get("STATE_DIRECTORY", "/var/lib/foundation-data-quality"))
WORK = STATE / "duckdb"
FIRST_CHECKED = STATE / "first-checked.json"
REPORT_ONLY_DAYS = 7
DUCKDB_MEMORY_MIB = "300"


# The contract checks' names (orchestration/dagster/contract_checks.py), and the Pipes protocol version.
COLUMNS_PRESENT = "contract_columns_present"
ALLOWED_VALUES = "contract_allowed_values"
PIPES_VERSION = "0.1"
# A lakehouse table's Dagster key is on the `iceberg` platform, any other on `perfectory`
# (infra/datahub/dataset_names.py, observe_assets.asset_key).
LAKEHOUSE_NAMESPACES = ("silver.", "gold.", "reference.")


def graphql(url, query, variables=None, describe="the data catalog"):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    answer = internal_http.open_json(request, timeout=60)
    if answer.get("errors"):
        raise RuntimeError(f"{describe} answered errors: {answer['errors'][:1]}")
    return answer["data"]


def asset_key(table):
    """The Dagster asset key of a contract's table, as a path."""
    return ["iceberg" if table.startswith(LAKEHOUSE_NAMESPACES) else "perfectory", *table.split(".")]


def allowed_values_check(field):
    return f"{ALLOWED_VALUES}_{field}"


def report(table, check, passed, severity, metadata):
    """One evaluation, as a Pipes `report_asset_check` line on stdout (the unit's journal)."""
    print(json.dumps({"__dagster_pipes_version": PIPES_VERSION, "method": "report_asset_check", "params": {
        "asset_key": "/".join(asset_key(table)), "check_name": check, "passed": passed, "severity": severity,
        "metadata": {name: {"raw_value": value, "type": "__infer__"} for name, value in metadata.items()},
    }}, ensure_ascii=False), flush=True)


def dagster_ui():
    return json.loads(JOBS.read_text(encoding="utf-8"))["observers"]["dagster"]["ui"].rstrip("/")


def ask_dagster(query):
    return graphql(f"{dagster_ui()}/graphql", query, describe="Dagster")


def last_evaluations(wanted, ask=None):
    """{(table, check): (passed, {label: value})} of each check's newest evaluation in Dagster, one query
    for all of them; a check with none is missing."""
    if not wanted:
        return {}
    fields = "".join(
        f" c{index}: assetCheckExecutions(assetKey: {{path: {json.dumps(asset_key(table))}}},"
        f" checkName: {json.dumps(check)}, limit: 1) {{ evaluation {{ success"
        " metadataEntries { label ... on TextMetadataEntry { text } ... on IntMetadataEntry { intValue } } } }"
        for index, (table, check) in enumerate(wanted))
    data = (ask or ask_dagster)("{" + fields + " }")
    found = {}
    for index, item in enumerate(wanted):
        executions = data.get(f"c{index}") or []
        evaluation = executions[0].get("evaluation") if executions else None
        if evaluation:
            found[item] = (evaluation["success"], {entry["label"]: entry.get("text", entry.get("intValue"))
                                                   for entry in evaluation.get("metadataEntries") or []})
    return found


def checked_in_datahub(contract_id):
    """Whether DataHub holds a result of the contract (it judged it before 2026-11-23). Asked once per
    contract missing from first-checked.json, so the contracts it enforced stay enforced; removed with
    DataHub (root ADR-0609 §2, PR 4)."""
    urn = f"urn:li:dataset:(urn:li:dataPlatform:odcs,{contract_id},PROD)"
    data = graphql(f"{GMS}/api/graphql",
                   """query($urn: String!) { dataset(urn: $urn) { assertions(start: 0, count: 200) {
                        assertions { runEvents(limit: 1) { runEvents { timestampMillis } } } } } }""", {"urn": urn})
    dataset = data.get("dataset")
    return dataset is not None and any((assertion.get("runEvents") or {}).get("runEvents")
                                       for assertion in dataset["assertions"]["assertions"])


def run_publisher(lines):
    """What the current release's publisher's `lakehouse-query` answers to `lines` (JSON queries)."""
    WORK.mkdir(parents=True, exist_ok=True)
    environment = {**os.environ, "FOUNDATION_PLATFORM_LAKEHOUSE_QUERY_WORK": str(WORK),
                   "FOUNDATION_PLATFORM_LAKEHOUSE_QUERY_MEMORY_MIB": DUCKDB_MEMORY_MIB}
    return subprocess.run(
        ["bash", "-c", 'source "$1" --current && exec "${PUBLISHER_BIN}" lakehouse-query', "-",
         str(ADMITTED_RUNTIME)],
        input="".join(f"{line}\n" for line in lines), capture_output=True, text=True, check=True,
        timeout=1500, env=environment).stdout


def lakehouse(queries, run=run_publisher):
    """One answer per query, in order: {table, snapshot_id, rows, absent?, error?}. `queries` are
    (table, sql reading `{source}`, snapshot id or None)."""
    if not queries:
        return []
    lines = [json.dumps({"table": table, "sql": sql, **({"snapshot_id": snapshot} if snapshot else {})})
             for table, sql, snapshot in queries]
    answered = internal_http.retry_run(lambda: run(lines), describe="lakehouse-query")
    answers = [json.loads(line) for line in answered.splitlines() if line.strip()]
    if len(answers) != len(queries):
        raise RuntimeError(f"the publisher answered {len(answers)} of {len(queries)} queries")
    return answers


def rules_fingerprint(fields, allowed):
    """Which rules a count answered: the fields and their allowed values, order-free."""
    rules = sorted((field, sorted(allowed[field])) for field in fields)
    return hashlib.sha256(json.dumps(rules, ensure_ascii=False).encode()).hexdigest()[:16]


def unchanged_results(table, fields, snapshot, rules, latest):
    """{field: (passed, metadata)} when every field's newest evaluation counted this snapshot under
    these rules, else None."""
    if snapshot is None:
        return None
    found = {}
    for field in fields:
        result = latest.get((table, allowed_values_check(field)))
        if result is None or result[1].get("snapshot_id") != snapshot or result[1].get("rules") != rules:
            return None
        found[field] = result
    return found


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def contract_of(path):
    """(contract, its table, `namespace.name`) of a contract file."""
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    (table,) = contract["schema"]
    return contract, table, table["physicalName"]


def allowed_values(table):
    return {
        prop["name"]: rule["arguments"]["validValues"]
        for prop in table["properties"]
        for rule in prop.get("quality", [])
        if rule.get("metric") == "invalidValues"
    }


def count_sql(fields, allowed):
    """Rows, and per field the present values outside the allowed ones, of `{source}`."""
    counts = ", ".join(
        f"count_if({field} IS NOT NULL AND {field} NOT IN ({', '.join(quote(v) for v in allowed[field])})) AS f{index}"
        for index, field in enumerate(fields)
    )
    return f"SELECT count(*) AS total, {counts} FROM {{source}}"


def new_totals():
    return {"passed": [], "failed": [], "warned": [], "absent": [], "unchanged": [],
            "with_rules": [], "checked": [], "checks": []}


def enforced(contract_id, first_checked, now, judged_before=checked_in_datahub):
    """Whether a failure of this contract fails the run: its first check here is REPORT_ONLY_DAYS old.
    Records the first check of one never checked; one DataHub had judged is recorded as checked long
    ago (0), once."""
    if contract_id not in first_checked:
        first_checked[contract_id] = 0 if judged_before(contract_id) else int(now)
    return now - first_checked[contract_id] >= REPORT_ONLY_DAYS * 86400


def outcome(totals, passed, enforce, text):
    totals["checks"].append(text)
    totals["passed" if passed else ("failed" if enforce else "warned")].append(text)


def severity_of(passed, enforce):
    """A failure of an enforced rule is ERROR; a report-only one, and any pass, WARN."""
    return "ERROR" if enforce and not passed else "WARN"


def check_contracts(paths, totals, read=lakehouse, first_checked=None, now=None, latest=last_evaluations,
                    judged_before=checked_in_datahub, publish=report):
    """Every contract in `paths` with a machine rule in its file: described in one read, its changed
    allowed-values rules counted in a second, each result published as its check's evaluation."""
    first_checked = {} if first_checked is None else first_checked
    now = time.time() if now is None else now
    ruled = []
    for path in paths:
        contract, table, physical = contract_of(path)
        columns = {prop["name"] for prop in table.get("properties") or []}
        allowed = allowed_values(table)
        if not columns and not allowed:
            continue
        totals["with_rules"].append(contract["id"])
        ruled.append((contract, physical, columns, allowed))

    # The table's current schema (`{table}`), and the snapshot a count then pins to.
    described = read([(physical, "DESCRIBE {table}", None) for _, physical, _, _ in ruled])
    # Each allowed-values rule's newest evaluation, for the unchanged-table skip.
    previous_of = latest([(physical, allowed_values_check(field))
                          for _, physical, _, allowed in ruled for field in sorted(allowed)])
    counted = []
    for (contract, physical, columns, allowed), answer in zip(ruled, described, strict=True):
        if answer.get("absent"):
            totals["absent"].append(contract["id"])
            continue
        if answer.get("error"):
            raise RuntimeError(f"{physical} could not be described: {answer['error']}")
        totals["checked"].append(contract["id"])
        enforce = enforced(contract["id"], first_checked, now, judged_before)
        present = {row["column_name"] for row in answer["rows"]}
        snapshot = answer["snapshot_id"]
        # The results record the snapshot as text.
        snapshot_text = str(snapshot) if snapshot is not None else None
        fields = sorted(allowed)
        rules = rules_fingerprint(fields, allowed) if fields else None
        previous = unchanged_results(physical, fields, snapshot_text, rules, previous_of) if fields else None
        if previous is not None:
            totals["unchanged"].append(f"{contract['id']} snapshot={snapshot_text}")
            for field in fields:
                passed, values = previous[field]
                outcome(totals, passed, enforce,
                        f"{contract['id']}.{field} invalid={values.get('invalid_rows')} (unchanged since snapshot {snapshot_text})")
        elif fields:
            counted.append((contract, physical, snapshot, fields, rules, enforce, count_sql(fields, allowed)))

        if columns:
            missing = sorted(columns - present)
            publish(physical, COLUMNS_PRESENT, not missing, severity_of(not missing, enforce),
                    {"contract": contract["id"], "missing_columns": ",".join(missing) or "none",
                     "snapshot_id": "none" if snapshot is None else str(snapshot)})
            outcome(totals, not missing, enforce, f"{contract['id']} schema missing={missing}")

    answers = read([(physical, sql, snapshot) for _, physical, snapshot, _, _, _, sql in counted])
    for (contract, physical, snapshot, fields, rules, enforce, _), answer in zip(counted, answers, strict=True):
        if answer.get("error") or len(answer.get("rows") or []) != 1:
            raise RuntimeError(f"{physical} could not be counted: {answer.get('error') or 'no row'}")
        (row,) = answer["rows"]
        rows = int(row["total"])
        for index, field in enumerate(fields):
            bad = int(row[f"f{index}"])
            publish(physical, allowed_values_check(field), bad == 0, severity_of(bad == 0, enforce),
                    {"contract": contract["id"], "field": field, "rows": rows, "invalid_rows": bad,
                     "snapshot_id": "none" if snapshot is None else str(snapshot), "rules": rules})
            outcome(totals, bad == 0, enforce, f"{contract['id']}.{field} invalid={bad}")


def read_first_checked():
    """Contract id -> when it was first checked here (epoch seconds)."""
    try:
        return {str(key): int(value) for key, value in json.loads(FIRST_CHECKED.read_text(encoding="utf-8")).items()}
    except FileNotFoundError:
        return {}


def write_first_checked(first_checked):
    """Replaced whole by a rename, so a run that stops halfway leaves the previous file."""
    FIRST_CHECKED.parent.mkdir(parents=True, exist_ok=True)
    temporary = FIRST_CHECKED.with_name(FIRST_CHECKED.name + ".next")
    temporary.write_text(json.dumps(first_checked, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(FIRST_CHECKED)


def main():
    totals = new_totals()
    contracts = sorted(CONTRACTS.glob("*.odcs.yaml"))
    if not contracts:
        sys.exit(f"data-quality: no contracts under {CONTRACTS}")
    first_checked = read_first_checked()
    try:
        check_contracts(contracts, totals, first_checked=first_checked)
    finally:
        write_first_checked(first_checked)
    print(
        f"data-quality: contracts={len(contracts)} with_machine_rules={len(totals['with_rules'])} "
        f"checked={len(totals['checked'])} checks={len(totals['checks'])} passed={len(totals['passed'])} "
        f"failed={len(totals['failed'])} warned={len(totals['warned'])} absent_tables={len(totals['absent'])} "
        f"unchanged_tables={len(totals['unchanged'])}"
    )
    for label in ("failed", "warned", "absent"):
        for item in totals[label]:
            print(f"data-quality: {label}: {item}")
    # The run's last line (root ADR-0260). It writes no dataset, so a clean run changed nothing.
    if totals["failed"]:
        print(f"foundation-job-outcome failed {len(totals['failed'])} contract checks failed")
        sys.exit(1)
    print("foundation-job-outcome unchanged")


if __name__ == "__main__":
    main()
