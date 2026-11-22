"""one-off: until the last served Gold has moved to its _versions table

The `takeover_equal` asset check of a shadow build (root ADR-0564 §6-3, revision 2026-11-22), the two
steps gold-versions-build.sh asks of it:

    gold_takeover_check.py declared <summary.json> <definitions dir> <state file>
        prints "<versions table> <시도>" when the run is a shadow build whose definition declares the
        check (the 시도 it names) and its head wants a verdict: a commit always, a run that kept the
        head only until one verdict of that head has run to its end (the state file says which did);
        prints nothing otherwise.
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
# A verdict that ran to its end: equal, not equal, or a national check failed. Anything else did not run.
OUTCOMES = {0: "equal", 1: "not equal", 3: "a national check failed"}


def declared(summary, definitions, state):
    """(table, 시도) when this run's shadow head wants a verdict, or None."""
    if not summary.get("shadow_of") or summary.get("outcome") not in ("committed", "unchanged"):
        return None
    if summary["outcome"] == "unchanged" and state.get("completed"):
        return None
    table = summary["table"]
    for definition in definitions:
        assets = {asset["id"]: asset.get("table_name") for asset in definition.get("assets", [])}
        for check in definition.get("asset_checks", []):
            if check["check"] == CHECK and assets.get(check["asset"]) == table:
                if "sido" not in check:
                    raise ValueError(f"{CHECK} on {check['asset']} names no sido: a verdict compares the one it names")
                return table, check["sido"]
    return None


def completed(status, verdict):
    """Whether the verdict ran to its end: an outcome it can exit with, and a comparison it recorded
    (an uncaught error also exits 1, and leaves no comparison)."""
    return status in OUTCOMES and (status == 3 or "same" in verdict)


def report(table, sido, status, verdict, checks):
    """The Pipes message of one verdict: its outcome, the 시도's comparison and the national checks."""
    ran = completed(status, verdict)
    scope = verdict.get("scope") or {}
    metadata = {
        "outcome": OUTCOMES[status] if ran else f"the verdict did not run to its end (exit {status})",
        "exit_status": status, "sido": sido,
        "sido_rows": scope.get("sido_rows"), "rows_nationally": scope.get("rows_nationally"),
        "rows_compared": verdict.get("reference_rows"), "same": verdict.get("same"),
        "different": verdict.get("different"), "only_reference": verdict.get("only_reference"),
        "only_candidate": verdict.get("only_candidate"),
        "national_checks_failed": checks.get("failed") if checks else None,
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
            # A new head: no verdict of it has run yet.
            pathlib.Path(argv[3]).unlink(missing_ok=True)
            state = {}
        definitions = [_read(path) for path in sorted(pathlib.Path(argv[2]).glob("*.definition.json"))]
        found = declared(summary, definitions, state)
        if found:
            print(*found)
        return 0
    if argv[:1] == ["report"] and len(argv) == 6:
        table, sido, status, work, state = argv[1], argv[2], int(argv[3]), pathlib.Path(argv[4]), pathlib.Path(argv[5])
        verdict = _read(work / "verdict.json")
        print(json.dumps(report(table, sido, status, verdict, _read(work / "checks.json")), ensure_ascii=False))
        state.write_text(json.dumps({"completed": completed(status, verdict), "exit_status": status,
                                     "candidate_snapshot": verdict.get("candidate_snapshot")}) + "\n",
                         encoding="utf-8")
        return 0
    print(__doc__, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
