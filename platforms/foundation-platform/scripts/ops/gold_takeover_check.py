"""one-off: until the last served Gold has moved to its _versions table

The `takeover_equal` asset check of a shadow build (root ADR-0564 §6-3, revision 2026-11-22), the two
steps gold-versions-build.sh asks of it:

    gold_takeover_check.py declared <summary.json> <definitions dir> <state file>
        prints "<versions table> <시도>", one line a table the run built (its summary's `tables`, or
        its one `table`), when the run is a shadow build whose definition declares the check on that
        table (the 시도 it names) and its head wants a verdict: a commit always, a run that kept the
        head only until one verdict of that head and of the build it shadows has run to its end (the
        state file says which did, table by table); prints nothing otherwise. The build judges the first line, one verdict a run,
        so the lines start after the table judged last: each has its turn even when every run commits.
    gold_takeover_check.py report <versions table> <시도> <verdict exit status> <verdict work dir> <state file>
        prints the Dagster Pipes `report_asset_check` line the op that ran the unit logs as the check's
        evaluation (orchestration/dagster/takeover_checks.py; the op gives it its declared severity),
        and records in the state file whether the verdict ran to its end. It passes only on `equal`.
"""

import json
import pathlib
import sys

CHECK = "takeover_equal"
PIPES_PROTOCOL_VERSION = "0.1"
# A verdict that ran to its end: equal, not equal, or a result check failed (a national aggregate, or a
# document of the 시도 its schema refuses). Anything else did not run.
OUTCOMES = {0: "equal", 1: "not equal", 3: "a result check failed"}
# The state's entry naming the table judged last; a commit keeps it.
LAST = "last_judged"


def recorded(state, table):
    """The record of `table`'s last verdict. The state keeps one a table; a state of one record (a
    lane's, written before a run built two tables) is the record of the run's table."""
    return state if "completed" in state else state.get(table) or {}


def declared_check(table, definitions):
    """The 시도 the check declared on `table` names, or None when no definition declares one."""
    for definition in definitions:
        assets = {asset["id"]: asset.get("table_name") for asset in definition.get("assets", [])}
        for check in definition.get("asset_checks", []):
            if check["check"] == CHECK and assets.get(check["asset"]) == table:
                if "sido" not in check:
                    raise ValueError(f"{CHECK} on {check['asset']} names no sido: a verdict compares the one it names")
                return check["sido"]
    return None


def declared(summary, definitions, state):
    """[(table, 시도)] of the tables this run built whose shadow head wants a verdict, starting after
    the table judged last (the summary's order otherwise)."""
    if not summary.get("shadow_of") or summary.get("outcome") not in ("committed", "unchanged"):
        return []
    # A head one verdict judged to its end is not judged again, unless the build it shadows moved
    # under it: a shadow that writes nothing (the unit prices) keeps its head while Spark builds anew.
    # A record from before it named the reference names none, and stands for this one.
    reference = str(summary["shadow_of"].get("build"))
    found = []
    for table in summary.get("tables") or [summary["table"]]:
        record = recorded(state, table)
        if (summary["outcome"] == "unchanged" and record.get("completed")
                and str(record.get("reference_snapshot", reference)) == reference):
            continue
        sido = declared_check(table, definitions)
        if sido is not None:
            found.append((table, sido))
    tables = [table for table, _ in found]
    turn = tables.index(state[LAST]) + 1 if state.get(LAST) in tables else 0
    return found[turn:] + found[:turn]


def completed(status, verdict):
    """Whether the verdict ran to its end: an outcome it can exit with, and a comparison it recorded
    (an uncaught error also exits 1, and leaves no comparison)."""
    return status in OUTCOMES and (status == 3 or "same" in verdict)


def report(table, sido, status, verdict, checks):
    """The Pipes message of one verdict: its outcome, the 시도's comparison, the national checks and the
    documents of the 시도 held to their schema."""
    ran = completed(status, verdict)
    scope = verdict.get("scope") or {}
    documents = (checks or {}).get("documents") or {}
    metadata = {
        "outcome": OUTCOMES[status] if ran else f"the verdict did not run to its end (exit {status})",
        "exit_status": status, "sido": sido,
        "sido_rows": scope.get("sido_rows"), "rows_nationally": scope.get("rows_nationally"),
        "rows_compared": verdict.get("reference_rows"), "same": verdict.get("same"),
        "different": verdict.get("different"), "only_reference": verdict.get("only_reference"),
        "only_candidate": verdict.get("only_candidate"),
        "result_checks_failed": checks.get("failed") if checks else None,
        "documents_rendered": documents.get("rendered"), "documents_refused": documents.get("refused"),
        "reference_snapshot": verdict.get("reference_snapshot"),
        "candidate_snapshot": verdict.get("candidate_snapshot"),
    }
    return {"__dagster_pipes_version": PIPES_PROTOCOL_VERSION, "method": "report_asset_check", "params": {
        "asset_key": "iceberg/" + table.replace(".", "/"), "check_name": CHECK,
        "passed": ran and status == 0,
        "metadata": {name: {"raw_value": value if isinstance(value, (str, int, float, bool))
                            else json.dumps(value, ensure_ascii=False), "type": "__infer__"}
                     for name, value in metadata.items() if value is not None},
    }}


def _read(path):
    try:
        return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def main(argv):
    if argv[:1] == ["declared"] and len(argv) == 4:
        summary, state = _read(argv[1]), _read(argv[3])
        if summary.get("outcome") == "committed":
            # A new head: no verdict of it has run yet. Which table was judged last stays.
            state = {LAST: state[LAST]} if state.get(LAST) else {}
            if state:
                pathlib.Path(argv[3]).write_text(json.dumps(state) + "\n", encoding="utf-8")
            else:
                pathlib.Path(argv[3]).unlink(missing_ok=True)
        definitions = [_read(path) for path in sorted(pathlib.Path(argv[2]).glob("*.definition.json"))]
        for table, sido in declared(summary, definitions, state):
            print(table, sido)
        return 0
    if argv[:1] == ["report"] and len(argv) == 6:
        table, sido, status, work, state = argv[1], argv[2], int(argv[3]), pathlib.Path(argv[4]), pathlib.Path(argv[5])
        verdict = _read(work / "verdict.json")
        print(json.dumps(report(table, sido, status, verdict, _read(work / "checks.json")), ensure_ascii=False))
        records = _read(state)
        records = {} if "completed" in records else records
        records[table] = {"completed": completed(status, verdict), "exit_status": status,
                          "reference_snapshot": verdict.get("reference_snapshot"),
                          "candidate_snapshot": verdict.get("candidate_snapshot")}
        records[LAST] = table
        state.write_text(json.dumps(records) + "\n", encoding="utf-8")
        return 0
    print(__doc__, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
