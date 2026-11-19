#!/usr/bin/env python3
"""Runs the data contracts' executable checks and reports each result to the data catalog
(root ADR-0117 §6, ADR-0123).

For every contract in contracts/data, its machine rules are read from the contract file itself and
run against the lakehouse with the publisher's DuckDB (`lakehouse-query`: the catalog's read-only token
and the read-only key pair, root ADR-0565; it asked Trino's CLI before), each table read at its `main`
snapshot:

- allowed values (`invalidValues` with validValues): rows whose value is present and not in the
  contract's validValues are counted; any such row fails the rule.
- schema (superset): every column the contract names must exist in the physical table. Every contract
  that names a column has this rule, whether or not DataHub holds an assertion for it.

The rules were read from DataHub's assertions until 2026-11-19, and DataHub held 35 of 143 contracts, so
110 were never checked (docs/designs/data-catalog-datahub-vs-dagster.md). The files are the rules'
source (root ADR-0123); DataHub stays where a result is written, on the assertion it made from the same
rule, as long as it keeps the run history (coordinator decision 2026-11-19, option C). A rule DataHub
holds no assertion for is still run and counted; its result is printed, not written there.

A contract checked for the first time is report-only for REPORT_ONLY_DAYS: its failures are printed as
`warned` and do not fail the run, so a rule nobody ran before does not page on its first day. A contract
with any result in DataHub was checked before and is enforced at once. The first check's time is kept
in the unit's state directory (first-checked.json).

One publisher run describes every contract's table (its `main` snapshot, and its current schema's
columns, as Trino's SHOW COLUMNS answered: a column added after the newest snapshot exists); a second
counts the tables whose snapshot or rules changed, at the snapshot the first read, so a count is
always of the snapshot its result records.
Measured 2026-11-19 on production against Trino at the same snapshots: all 137 present contract tables
with the same columns, 6 absent from both, and the one allowed-values count equal (4,326 rows, 0
invalid); 34 Trino statements a run before, none now.

The result (SUCCESS or FAILURE, with the counts behind it) is reported on the assertion, so the
catalog shows when each rule last passed. Each allowed-values result also records the table's Iceberg
snapshot and a fingerprint of the rules it counted; the catalog keeps every result as a run event, so
that is the append-only record of what was checked. A table whose current snapshot and rules equal
those of every field's latest result is not counted again (a full scan of an unchanged table answers
the same), and those results still count toward the run: a failure stays a failure until the data or
the rule changes. Rules written as text are not run and are not reported:
nothing pretends to have checked them. A table the lakehouse does not hold yet is counted and
named, not failed: a declared status of "implemented" means the code path exists (pipeline graph
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
GMS = os.environ.get("FOUNDATION_DATA_QUALITY_GMS_URL", "http://127.0.0.1:18095")
# The admitted release's publisher (admitted-writer-runtime.sh), and where its DuckDB keeps its
# database and spill: the unit's StateDirectory. Its memory leaves the script room in the unit's 512M.
ADMITTED_RUNTIME = pathlib.Path(__file__).resolve().parent / "admitted-writer-runtime.sh"
STATE = pathlib.Path(os.environ.get("STATE_DIRECTORY", "/var/lib/foundation-data-quality"))
WORK = STATE / "duckdb"
FIRST_CHECKED = STATE / "first-checked.json"
REPORT_ONLY_DAYS = 7
DUCKDB_MEMORY_MIB = "300"


def graphql(query, variables=None):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(
        f"{GMS}/api/graphql", data=body, headers={"Content-Type": "application/json"}
    )
    answer = internal_http.open_json(request, timeout=60)
    if answer.get("errors"):
        raise RuntimeError(f"data catalog answered errors: {answer['errors'][:1]}")
    return answer["data"]


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


def assertions_of(contract_id):
    urn = f"urn:li:dataset:(urn:li:dataPlatform:odcs,{contract_id},PROD)"
    data = graphql(
        """query($urn: String!) { dataset(urn: $urn) { assertions(start: 0, count: 200) {
             assertions { urn info { type fieldAssertion { fieldValuesAssertion { field { path } } } }
               runEvents(limit: 1) { runEvents { result { type nativeResults { key value } } } } } } } }""",
        {"urn": urn},
    )
    dataset = data.get("dataset")
    return [] if dataset is None else dataset["assertions"]["assertions"]


def report(assertion_urn, passed, properties):
    data = graphql(
        """mutation($urn: String!, $result: AssertionResultInput!) {
             reportAssertionResult(urn: $urn, result: $result) }""",
        {
            "urn": assertion_urn,
            "result": {
                "timestampMillis": int(time.time() * 1000),
                "type": "SUCCESS" if passed else "FAILURE",
                "properties": [{"key": k, "value": str(v)} for k, v in properties.items()],
            },
        },
    )
    if not data.get("reportAssertionResult"):
        raise RuntimeError(f"the data catalog did not record {assertion_urn}")


def rules_fingerprint(field_assertions, allowed):
    """Which rules a count answered: the fields and their allowed values, order-free."""
    rules = sorted((field, sorted(allowed[field])) for _, field in field_assertions)
    return hashlib.sha256(json.dumps(rules, ensure_ascii=False).encode()).hexdigest()[:16]


def last_result(assertion):
    """(type, {key: value}) of the assertion's latest reported result, or None."""
    events = ((assertion.get("runEvents") or {}).get("runEvents")) or []
    if not events or not events[0].get("result"):
        return None
    result = events[0]["result"]
    return result.get("type"), {item["key"]: item["value"] for item in result.get("nativeResults") or []}


def unchanged_results(assertions, field_assertions, snapshot, rules):
    """{assertion urn: latest result} when every field's latest result counted this snapshot under
    these rules, else None."""
    if snapshot is None:
        return None
    latest = {a["urn"]: last_result(a) for a in assertions}
    found = {}
    for urn, _ in field_assertions:
        result = latest.get(urn)
        if result is None or result[1].get("snapshot_id") != snapshot or result[1].get("rules") != rules:
            return None
        found[urn] = result
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


def field_assertions_of(assertions):
    return [
        (a["urn"], a["info"]["fieldAssertion"]["fieldValuesAssertion"]["field"]["path"])
        for a in assertions
        if a["info"]["type"] == "FIELD" and (a["info"].get("fieldAssertion") or {}).get("fieldValuesAssertion")
    ]


def count_sql(field_assertions, allowed):
    """Rows, and per field the present values outside the allowed ones, of `{source}`."""
    counts = ", ".join(
        f"count_if({field} IS NOT NULL AND {field} NOT IN ({', '.join(quote(v) for v in allowed[field])})) AS f{index}"
        for index, (_, field) in enumerate(field_assertions)
    )
    return f"SELECT count(*) AS total, {counts} FROM {{source}}"


def new_totals():
    return {"passed": [], "failed": [], "warned": [], "absent": [], "unregistered": [], "unchanged": [],
            "with_rules": [], "checked": [], "checks": []}


def enforced(contract_id, assertions, first_checked, now):
    """Whether a failure of this contract fails the run: it was checked before (a result in DataHub),
    or its first check here is REPORT_ONLY_DAYS old. Records the first check of one never checked."""
    if any(last_result(assertion) for assertion in assertions):
        return True
    first = first_checked.setdefault(contract_id, int(now))
    return now - first >= REPORT_ONLY_DAYS * 86400


def outcome(totals, passed, enforce, text):
    totals["checks"].append(text)
    totals["passed" if passed else ("failed" if enforce else "warned")].append(text)


def check_contracts(paths, totals, read=lakehouse, first_checked=None, now=None):
    """Every contract in `paths` with a machine rule in its file: described in one read, its changed
    allowed-values rules counted in a second."""
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
        assertions = assertions_of(contract["id"])
        if not assertions:
            totals["unregistered"].append(contract["id"])
        ruled.append((contract, physical, columns, allowed, assertions))

    # The table's current schema (`{table}`), and the snapshot a count then pins to.
    described = read([(physical, "DESCRIBE {table}", None) for _, physical, _, _, _ in ruled])
    counted = []
    for (contract, physical, columns, allowed, assertions), answer in zip(ruled, described):
        if answer.get("absent"):
            totals["absent"].append(contract["id"])
            continue
        if answer.get("error"):
            raise RuntimeError(f"{physical} could not be described: {answer['error']}")
        totals["checked"].append(contract["id"])
        enforce = enforced(contract["id"], assertions, first_checked, now)
        present = {row["column_name"] for row in answer["rows"]}
        snapshot = answer["snapshot_id"]
        # The results record the snapshot as text (DataHub's properties are strings).
        snapshot_text = str(snapshot) if snapshot is not None else None
        # Each allowed-values rule of the file, with DataHub's assertion for it when it holds one.
        urns = {field: urn for urn, field in field_assertions_of(assertions)}
        fields = [(urns.get(field), field) for field in sorted(allowed)]
        rules = rules_fingerprint(fields, allowed) if fields else None
        previous = None
        if fields and all(urn for urn, _ in fields):
            previous = unchanged_results(assertions, fields, snapshot_text, rules)
        if previous is not None:
            totals["unchanged"].append(f"{contract['id']} snapshot={snapshot_text}")
            for urn, field in fields:
                kind, values = previous[urn]
                outcome(totals, kind == "SUCCESS", enforce,
                        f"{contract['id']}.{field} invalid={values.get('invalid_rows')} (unchanged since snapshot {snapshot_text})")
        elif fields:
            counted.append((contract, physical, snapshot, fields, rules, enforce, count_sql(fields, allowed)))

        if columns:
            missing = sorted(columns - present)
            for assertion in assertions:
                if assertion["info"]["type"] == "DATA_SCHEMA":
                    report(assertion["urn"], not missing, {"missing_columns": ",".join(missing) or "none"})
            outcome(totals, not missing, enforce, f"{contract['id']} schema missing={missing}")

    answers = read([(physical, sql, snapshot) for _, physical, snapshot, _, _, _, sql in counted])
    for (contract, physical, snapshot, fields, rules, enforce, _), answer in zip(counted, answers):
        if answer.get("error") or len(answer.get("rows") or []) != 1:
            raise RuntimeError(f"{physical} could not be counted: {answer.get('error') or 'no row'}")
        (row,) = answer["rows"]
        rows = int(row["total"])
        for index, (urn, field) in enumerate(fields):
            bad = int(row[f"f{index}"])
            if urn is not None:
                report(urn, bad == 0, {"rows": rows, "invalid_rows": bad, "field": field,
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
        f"unchanged_tables={len(totals['unchanged'])} not_in_datahub={len(totals['unregistered'])}"
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
