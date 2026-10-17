#!/usr/bin/env python3
"""Runs the data contracts' executable checks and reports each result to the data catalog
(root ADR-0117 §6, ADR-0123).

For every contract in contracts/data, the assertions DataHub made from it are read back and the
two kinds a machine can decide are run against the lakehouse through Trino:

- FIELD_VALUES (allowed values): rows whose value is present and not in the contract's
  validValues are counted; any such row fails the assertion.
- DATA_SCHEMA (SUPERSET): every column the contract names must exist in the physical table.

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

import csv
import hashlib
import io
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
TRINO_CONTAINER = os.environ.get("FOUNDATION_DATA_QUALITY_TRINO_CONTAINER", "foundation-platform-trino")
TRINO_CATALOG = "r2"


def graphql(query, variables=None):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(
        f"{GMS}/api/graphql", data=body, headers={"Content-Type": "application/json"}
    )
    answer = internal_http.open_json(request, timeout=60)
    if answer.get("errors"):
        raise RuntimeError(f"data catalog answered errors: {answer['errors'][:1]}")
    return answer["data"]


def trino(sql):
    """Rows of a Trino query, as lists of strings. A query error raises LookupError at once; a
    server that did not answer is asked again (internal_http)."""
    def once():
        completed = subprocess.run(
            ["docker", "exec", TRINO_CONTAINER, "trino", "--catalog", TRINO_CATALOG,
             "--output-format", "CSV", "--execute", sql],
            capture_output=True, text=True, timeout=900,
        )
        if completed.returncode == 0:
            return list(csv.reader(io.StringIO(completed.stdout)))
        last = completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else "trino failed"
        if internal_http.trino_transient(completed.stderr):
            raise internal_http.TrinoUnavailable(last)
        raise LookupError(last)

    return internal_http.retry_trino(once)


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


def current_snapshot(namespace, name):
    """The table's `main` snapshot id, or None when it has no snapshot."""
    rows = trino(f"SELECT snapshot_id FROM {TRINO_CATALOG}.{namespace}.\"{name}$refs\" WHERE name = 'main'")
    return rows[-1][0] if rows and rows[-1] and rows[-1][0].isdigit() else None


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


def check_contract(path, totals):
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    (table,) = contract["schema"]
    physical = table["physicalName"]
    namespace, name = physical.split(".", 1)
    assertions = assertions_of(contract["id"])
    if not assertions:
        totals["unregistered"].append(contract["id"])
        return
    try:
        columns = {row[0] for row in trino(f"SHOW COLUMNS FROM {TRINO_CATALOG}.{namespace}.{name}")}
    except LookupError:
        totals["absent"].append(contract["id"])
        return

    allowed = {
        prop["name"]: rule["arguments"]["validValues"]
        for prop in table["properties"]
        for rule in prop.get("quality", [])
        if rule.get("metric") == "invalidValues"
    }
    field_assertions = [
        (a["urn"], a["info"]["fieldAssertion"]["fieldValuesAssertion"]["field"]["path"])
        for a in assertions
        if a["info"]["type"] == "FIELD" and (a["info"].get("fieldAssertion") or {}).get("fieldValuesAssertion")
    ]
    snapshot = current_snapshot(namespace, name) if field_assertions else None
    rules = rules_fingerprint(field_assertions, allowed) if field_assertions else None
    previous = unchanged_results(assertions, field_assertions, snapshot, rules) if field_assertions else None
    if previous is not None:
        totals["unchanged"].append(f"{contract['id']} snapshot={snapshot}")
        for urn, field in field_assertions:
            kind, values = previous[urn]
            totals["passed" if kind == "SUCCESS" else "failed"].append(
                f"{contract['id']}.{field} invalid={values.get('invalid_rows')} (unchanged since snapshot {snapshot})")
    elif field_assertions:
        counts = ", ".join(
            f"count_if({field} IS NOT NULL AND {field} NOT IN ({', '.join(quote(v) for v in allowed[field])}))"
            for _, field in field_assertions
        )
        (row,) = trino(f"SELECT count(*), {counts} FROM {TRINO_CATALOG}.{namespace}.{name}")[-1:]
        rows, invalid = int(row[0]), [int(value) for value in row[1:]]
        for (urn, field), bad in zip(field_assertions, invalid):
            report(urn, bad == 0, {"rows": rows, "invalid_rows": bad, "field": field,
                                   "snapshot_id": snapshot or "none", "rules": rules})
            totals["passed" if bad == 0 else "failed"].append(f"{contract['id']}.{field} invalid={bad}")

    for assertion in assertions:
        if assertion["info"]["type"] != "DATA_SCHEMA":
            continue
        missing = sorted({prop["name"] for prop in table["properties"]} - columns)
        report(assertion["urn"], not missing, {"missing_columns": ",".join(missing) or "none"})
        totals["passed" if not missing else "failed"].append(f"{contract['id']} schema missing={missing}")


def main():
    totals = {"passed": [], "failed": [], "absent": [], "unregistered": [], "unchanged": []}
    contracts = sorted(CONTRACTS.glob("*.odcs.yaml"))
    if not contracts:
        sys.exit(f"data-quality: no contracts under {CONTRACTS}")
    for path in contracts:
        check_contract(path, totals)
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
