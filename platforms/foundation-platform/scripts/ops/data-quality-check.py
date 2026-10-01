#!/usr/bin/env python3
"""Runs the data contracts' executable checks and reports each result to the data catalog
(root ADR-0117 §6, ADR-0123).

For every contract in contracts/data, the assertions DataHub made from it are read back and the
two kinds a machine can decide are run against the lakehouse through Trino:

- FIELD_VALUES (allowed values): rows whose value is present and not in the contract's
  validValues are counted; any such row fails the assertion.
- DATA_SCHEMA (SUPERSET): every column the contract names must exist in the physical table.

The result (SUCCESS or FAILURE, with the counts behind it) is reported on the assertion, so the
catalog shows when each rule last passed. Rules written as text are not run and are not reported:
nothing pretends to have checked them. A table the lakehouse does not hold yet is counted and
named, not failed: a declared status of "implemented" means the code path exists (pipeline graph
status policy), not that a load has run.

Exits 1 when any assertion failed, so the scheduler's run fails and alerts.
"""

import csv
import io
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request

import yaml

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
    with urllib.request.urlopen(request, timeout=60) as response:
        answer = json.load(response)
    if answer.get("errors"):
        raise RuntimeError(f"data catalog answered errors: {answer['errors'][:1]}")
    return answer["data"]


def trino(sql):
    """Rows of a Trino query, as lists of strings."""
    completed = subprocess.run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--catalog", TRINO_CATALOG,
         "--output-format", "CSV", "--execute", sql],
        capture_output=True, text=True, timeout=900,
    )
    if completed.returncode != 0:
        raise LookupError(completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else "trino failed")
    return list(csv.reader(io.StringIO(completed.stdout)))


def assertions_of(contract_id):
    urn = f"urn:li:dataset:(urn:li:dataPlatform:odcs,{contract_id},PROD)"
    data = graphql(
        """query($urn: String!) { dataset(urn: $urn) { assertions(start: 0, count: 200) {
             assertions { urn info { type fieldAssertion { fieldValuesAssertion { field { path } } } } } } } }""",
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
    if field_assertions:
        counts = ", ".join(
            f"count_if({field} IS NOT NULL AND {field} NOT IN ({', '.join(quote(v) for v in allowed[field])}))"
            for _, field in field_assertions
        )
        (row,) = trino(f"SELECT count(*), {counts} FROM {TRINO_CATALOG}.{namespace}.{name}")[-1:]
        rows, invalid = int(row[0]), [int(value) for value in row[1:]]
        for (urn, field), bad in zip(field_assertions, invalid):
            report(urn, bad == 0, {"rows": rows, "invalid_rows": bad, "field": field})
            totals["passed" if bad == 0 else "failed"].append(f"{contract['id']}.{field} invalid={bad}")

    for assertion in assertions:
        if assertion["info"]["type"] != "DATA_SCHEMA":
            continue
        missing = sorted({prop["name"] for prop in table["properties"]} - columns)
        report(assertion["urn"], not missing, {"missing_columns": ",".join(missing) or "none"})
        totals["passed" if not missing else "failed"].append(f"{contract['id']} schema missing={missing}")


def main():
    totals = {"passed": [], "failed": [], "absent": [], "unregistered": []}
    contracts = sorted(CONTRACTS.glob("*.odcs.yaml"))
    if not contracts:
        sys.exit(f"data-quality: no contracts under {CONTRACTS}")
    for path in contracts:
        check_contract(path, totals)
    print(
        f"data-quality: contracts={len(contracts)} passed={len(totals['passed'])} failed={len(totals['failed'])} "
        f"absent_tables={len(totals['absent'])} unregistered={len(totals['unregistered'])}"
    )
    for label in ("failed", "absent", "unregistered"):
        for item in totals[label]:
            print(f"data-quality: {label}: {item}")
    if totals["unregistered"]:
        print("data-quality: register the contracts first: datahub-runtime.sh ingest data-contracts")
    sys.exit(1 if totals["failed"] else 0)


if __name__ == "__main__":
    main()
