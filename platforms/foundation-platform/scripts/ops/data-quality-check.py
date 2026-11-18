#!/usr/bin/env python3
"""Runs the data contracts' executable checks and reports each result to the data catalog
(root ADR-0117 §6, ADR-0123).

For every contract in contracts/data, the assertions DataHub made from it are read back and the
two kinds a machine can decide are run against the lakehouse with the publisher's DuckDB
(`lakehouse-query`: the catalog's read-only token and the read-only key pair, root ADR-0565; it asked
Trino's CLI before), each table read at its `main` snapshot:

- FIELD_VALUES (allowed values): rows whose value is present and not in the contract's
  validValues are counted; any such row fails the assertion.
- DATA_SCHEMA (SUPERSET): every column the contract names must exist in the physical table.

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

Exits 1 when any assertion failed, so the scheduler's run fails and alerts.
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
WORK = pathlib.Path(os.environ.get("STATE_DIRECTORY", "/var/lib/foundation-data-quality")) / "duckdb"
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


def check_contracts(paths, totals, read=lakehouse):
    """Every contract in `paths`: described in one read, counted where changed in a second."""
    registered = []
    for path in paths:
        contract, table, physical = contract_of(path)
        assertions = assertions_of(contract["id"])
        if not assertions:
            totals["unregistered"].append(contract["id"])
            continue
        registered.append((contract, table, physical, assertions))

    # The table's current schema (`{table}`), and the snapshot a count then pins to.
    described = read([(physical, "DESCRIBE {table}", None)
                      for _, _, physical, _ in registered])
    counted = []
    for (contract, table, physical, assertions), answer in zip(registered, described):
        if answer.get("absent"):
            totals["absent"].append(contract["id"])
            continue
        if answer.get("error"):
            raise RuntimeError(f"{physical} could not be described: {answer['error']}")
        columns = {row["column_name"] for row in answer["rows"]}
        snapshot = answer["snapshot_id"]
        # The results record the snapshot as text (DataHub's properties are strings).
        snapshot_text = str(snapshot) if snapshot is not None else None
        allowed = allowed_values(table)
        field_assertions = field_assertions_of(assertions)
        rules = rules_fingerprint(field_assertions, allowed) if field_assertions else None
        previous = unchanged_results(assertions, field_assertions, snapshot_text, rules) if field_assertions else None
        if previous is not None:
            totals["unchanged"].append(f"{contract['id']} snapshot={snapshot_text}")
            for urn, field in field_assertions:
                kind, values = previous[urn]
                totals["passed" if kind == "SUCCESS" else "failed"].append(
                    f"{contract['id']}.{field} invalid={values.get('invalid_rows')} (unchanged since snapshot {snapshot_text})")
        elif field_assertions:
            counted.append((contract, physical, snapshot, field_assertions, rules,
                            count_sql(field_assertions, allowed)))

        for assertion in assertions:
            if assertion["info"]["type"] != "DATA_SCHEMA":
                continue
            missing = sorted({prop["name"] for prop in table["properties"]} - columns)
            report(assertion["urn"], not missing, {"missing_columns": ",".join(missing) or "none"})
            totals["passed" if not missing else "failed"].append(f"{contract['id']} schema missing={missing}")

    answers = read([(physical, sql, snapshot) for _, physical, snapshot, _, _, sql in counted])
    for (contract, physical, snapshot, field_assertions, rules, _), answer in zip(counted, answers):
        if answer.get("error") or len(answer.get("rows") or []) != 1:
            raise RuntimeError(f"{physical} could not be counted: {answer.get('error') or 'no row'}")
        (row,) = answer["rows"]
        rows = int(row["total"])
        for index, (urn, field) in enumerate(field_assertions):
            bad = int(row[f"f{index}"])
            report(urn, bad == 0, {"rows": rows, "invalid_rows": bad, "field": field,
                                   "snapshot_id": "none" if snapshot is None else str(snapshot), "rules": rules})
            totals["passed" if bad == 0 else "failed"].append(f"{contract['id']}.{field} invalid={bad}")


def main():
    totals = {"passed": [], "failed": [], "absent": [], "unregistered": [], "unchanged": []}
    contracts = sorted(CONTRACTS.glob("*.odcs.yaml"))
    if not contracts:
        sys.exit(f"data-quality: no contracts under {CONTRACTS}")
    check_contracts(contracts, totals)
    print(
        f"data-quality: contracts={len(contracts)} passed={len(totals['passed'])} failed={len(totals['failed'])} "
        f"absent_tables={len(totals['absent'])} unregistered={len(totals['unregistered'])} "
        f"unchanged_tables={len(totals['unchanged'])}"
    )
    for label in ("failed", "absent", "unregistered"):
        for item in totals[label]:
            print(f"data-quality: {label}: {item}")
    if totals["unregistered"]:
        print("data-quality: register the contracts first: datahub-runtime.sh ingest data-contracts")
    # The run's last line (root ADR-0260). It writes no dataset, so a clean run changed nothing.
    if totals["failed"]:
        print(f"foundation-job-outcome failed {len(totals['failed'])} contract checks failed")
        sys.exit(1)
    print("foundation-job-outcome unchanged")


if __name__ == "__main__":
    main()
