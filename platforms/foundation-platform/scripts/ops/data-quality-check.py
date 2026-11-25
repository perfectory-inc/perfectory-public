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
- region codes paired (`contract_region_codes_paired`, every Silver contract with a `pnu` column): rows
  whose PNU is under a 법정동 code the newest full table (`reference.legal_dong_code_snapshot`) holds only
  as abolished and the change table (`reference.legal_dong_code_change`) has no pair for are counted;
  any such row fails the rule. A Gold reader translates a code only through a pair, so such a row
  reaches no served parcel: 전북's demolitions (45, abolished 2024-01) were in Silver and in no Gold
  while the change table held the changes of 2026 only (measured 2026-11-25). It is judged at the 동·리
  code, not the 시군구: a split 시군구 has no pair of its own and is covered by its 동's. The rule is
  report-only for its own first REPORT_ONLY_DAYS, whatever the contract's age.

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
# database and spill: the unit's StateDirectory. Its DuckDB takes the one share of what the unit has
# left when it opens (duckdb_parquet_stage::open_engine), so this script keeps its room in the 512M.
ADMITTED_RUNTIME = pathlib.Path(__file__).resolve().parent / "admitted-writer-runtime.sh"
STATE = pathlib.Path(os.environ.get("STATE_DIRECTORY", "/var/lib/foundation-data-quality"))
WORK = STATE / "duckdb"
FIRST_CHECKED = STATE / "first-checked.json"
REPORT_ONLY_DAYS = 7


# The contract checks' names (orchestration/dagster/contract_checks.py), and the Pipes protocol version.
COLUMNS_PRESENT = "contract_columns_present"
ALLOWED_VALUES = "contract_allowed_values"
REGION_CODES_PAIRED = "contract_region_codes_paired"
# The column the region rule reads, and the tables it reads the codes from: the full table of every
# code, current and abolished (root ADR-0143), and the one record of code changes (root ADR-0145).
PNU = "pnu"
CODE_SNAPSHOT_TABLE = "reference.legal_dong_code_snapshot"
CODE_CHANGE_TABLE = "reference.legal_dong_code_change"
# Codes named in a result (the ones with the most rows), and tables counted per publisher run.
REGION_CODES_NAMED = 10
REGION_TABLES_PER_RUN = 8
PIPES_VERSION = "0.1"
# The metadata key of a check run on its own in Dagster: a re-statement of the newest report
# (orchestration/dagster/takeover_checks.py RESTATED_FROM), never published again as this run's.
RESTATED_FROM = "restated_from_run"
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
    environment = {**os.environ, "FOUNDATION_PLATFORM_LAKEHOUSE_QUERY_WORK": str(WORK)}
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


def is_allowed_values(rule):
    """An `invalidValues` rule with its validValues: the one rule kind run here (contract_checks.py reads
    it the same way)."""
    return rule.get("metric") == "invalidValues" and "validValues" in (rule.get("arguments") or {})


def allowed_values(table):
    return {
        prop["name"]: rule["arguments"]["validValues"]
        for prop in table.get("properties") or []
        for rule in prop.get("quality") or []
        if is_allowed_values(rule)
    }


def has_region_rule(table):
    """Whether the region rule holds for a contract's table: a Silver table keyed by `pnu`
    (contract_checks.py reads it the same way)."""
    return table["physicalName"].startswith("silver.") and any(
        prop["name"] == PNU for prop in table.get("properties") or [])


def unpaired_abolished_codes(read):
    """{code: abolished date} of every code the newest full table holds only as abolished and the
    change table pairs with no code, or None when either table is not there yet."""
    snapshot, changes = read([
        (CODE_SNAPSHOT_TABLE,
         "SELECT region_cd, max(abolished_date) AS abolished_date FROM {source} "
         "WHERE snapshot_date = (SELECT max(snapshot_date) FROM {source}) "
         "GROUP BY region_cd HAVING count_if(status = '존재') = 0", None),
        (CODE_CHANGE_TABLE, "SELECT DISTINCT old_code FROM {source} WHERE kind = 'pair'", None),
    ])
    for answer in (snapshot, changes):
        if answer.get("error"):
            raise RuntimeError(f"{answer['table']} could not be read: {answer['error']}")
    if snapshot.get("absent") or changes.get("absent"):
        return None
    paired = {row["old_code"] for row in changes["rows"]}
    return {row["region_cd"]: row["abolished_date"] or "" for row in snapshot["rows"]
            if row["region_cd"] not in paired}


def region_sql(codes):
    """Rows of `{source}` per unpaired abolished code its PNUs are under."""
    return (f"SELECT substr({PNU}, 1, 10) AS code, count(*) AS n FROM {{source}} "
            f"WHERE substr({PNU}, 1, 10) IN ({', '.join(quote(code) for code in sorted(codes))}) GROUP BY 1")


def region_first_checked_key(contract_id):
    """The rule's own first check (first-checked.json), apart from its contract's."""
    return f"{contract_id}#{REGION_CODES_PAIRED}"


def check_region_codes(paths, totals, read=lakehouse, first_checked=None, now=None, latest=last_evaluations,
                       publish=report):
    """The region rule of every Silver contract keyed by `pnu`: its table counted at its `main`
    snapshot unless the newest evaluation counted that snapshot against the same codes."""
    first_checked = {} if first_checked is None else first_checked
    now = time.time() if now is None else now
    ruled = [(contract, physical) for contract, table, physical in map(contract_of, paths) if has_region_rule(table)]
    if not ruled:
        return
    codes = unpaired_abolished_codes(read)
    if codes is None:
        print("data-quality: the 법정동 code tables are not in the lakehouse yet; the region rule is not run",
              file=sys.stderr)
        return
    # Which codes a count answered: a pair appended or a code abolished changes it, and counts again.
    rules = hashlib.sha256("\n".join(sorted(codes)).encode()).hexdigest()[:16]
    described = read([(physical, "DESCRIBE {table}", None) for _, physical in ruled])
    try:
        previous_of = latest([(physical, REGION_CODES_PAIRED) for _, physical in ruled])
    except (OSError, RuntimeError, ValueError) as error:
        print(f"data-quality: Dagster's last evaluations could not be read ({type(error).__name__}); "
              "every table is counted", file=sys.stderr)
        previous_of = {}
    counted = []
    for (contract, physical), answer in zip(ruled, described, strict=True):
        if answer.get("absent"):
            continue
        if answer.get("error"):
            raise RuntimeError(f"{physical} could not be described: {answer['error']}")
        enforce = enforced(region_first_checked_key(contract["id"]), first_checked, now, lambda _key: False)
        snapshot = answer["snapshot_id"]
        previous = previous_of.get((physical, REGION_CODES_PAIRED))
        if (snapshot is not None and previous is not None and previous[1].get("snapshot_id") == str(snapshot)
                and previous[1].get("rules") == rules):
            passed, values = previous
            publish(physical, REGION_CODES_PAIRED, passed, severity_of(enforce),
                    {name: value for name, value in values.items() if name != RESTATED_FROM})
            outcome(totals, passed, enforce,
                    f"{contract['id']} unpaired_region_rows={values.get('unpaired_rows')} (unchanged since snapshot {snapshot})")
        else:
            counted.append((contract, physical, snapshot, enforce))
    sql = region_sql(codes) if codes else None
    for start in range(0, len(counted), REGION_TABLES_PER_RUN):
        batch = counted[start:start + REGION_TABLES_PER_RUN]
        # No unpaired abolished code: nothing to count, every table passes.
        answers = (read([(physical, sql, snapshot) for _, physical, snapshot, _ in batch]) if sql
                   else [{"rows": []} for _ in batch])
        for (contract, physical, snapshot, enforce), answer in zip(batch, answers, strict=True):
            if answer.get("error"):
                raise RuntimeError(f"{physical} could not be counted: {answer['error']}")
            found = sorted(((int(row["n"]), row["code"]) for row in answer["rows"]), reverse=True)
            rows = sum(n for n, _ in found)
            named = ",".join(f"{code}({codes[code] or '-'}):{n}" for n, code in found[:REGION_CODES_NAMED])
            publish(physical, REGION_CODES_PAIRED, not found, severity_of(enforce),
                    {"contract": contract["id"], "unpaired_rows": rows, "unpaired_codes": len(found),
                     "codes": named or "none", "snapshot_id": "none" if snapshot is None else str(snapshot),
                     "rules": rules})
            outcome(totals, not found, enforce,
                    f"{contract['id']} unpaired_region_rows={rows} codes={len(found)} {named}".rstrip())


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
    ago (0). DataHub is asked only for a contract the file does not hold; when it does not answer,
    nothing is recorded and the contract only reports this run, so it is asked again on the next."""
    if contract_id not in first_checked:
        try:
            judged = judged_before(contract_id)
        except (OSError, RuntimeError, ValueError) as error:
            print(f"data-quality: DataHub did not say whether it judged {contract_id} before "
                  f"({type(error).__name__}); it only reports this run", file=sys.stderr)
            return False
        first_checked[contract_id] = 0 if judged else int(now)
    return now - first_checked[contract_id] >= REPORT_ONLY_DAYS * 86400


def outcome(totals, passed, enforce, text):
    totals["checks"].append(text)
    totals["passed" if passed else ("failed" if enforce else "warned")].append(text)


def severity_of(enforce):
    """ERROR for an enforced contract's rule, WARN while the contract only reports (Dagster shows a
    severity on a failure; on a pass it says which the rule would be)."""
    return "ERROR" if enforce else "WARN"


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
    # Each allowed-values rule's newest evaluation, for the unchanged-table skip. Dagster not answering
    # only costs the skip: every table is counted.
    try:
        previous_of = latest([(physical, allowed_values_check(field))
                              for _, physical, _, allowed in ruled for field in sorted(allowed)])
    except (OSError, RuntimeError, ValueError) as error:
        print(f"data-quality: Dagster's last evaluations could not be read ({type(error).__name__}); "
              "every table is counted", file=sys.stderr)
        previous_of = {}
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
                # The result stands for this run too, at the contract's severity today: a rule that
                # was report-only and is enforced now says so.
                publish(physical, allowed_values_check(field), passed, severity_of(enforce),
                        {name: value for name, value in values.items() if name != RESTATED_FROM})
                outcome(totals, passed, enforce,
                        f"{contract['id']}.{field} invalid={values.get('invalid_rows')} (unchanged since snapshot {snapshot_text})")
        elif fields:
            counted.append((contract, physical, snapshot, fields, rules, enforce, count_sql(fields, allowed)))

        if columns:
            missing = sorted(columns - present)
            publish(physical, COLUMNS_PRESENT, not missing, severity_of(enforce),
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
            publish(physical, allowed_values_check(field), bad == 0, severity_of(enforce),
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
        check_region_codes(contracts, totals, first_checked=first_checked)
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
