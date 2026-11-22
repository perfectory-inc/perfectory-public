"""one-off: until the last served Gold has moved to its _versions table

The `takeover_equal` asset check of a shadow commit (root ADR-0564 §6-3, revision 2026-11-22), the two
steps gold-versions-build.sh asks of it:

    gold_takeover_check.py declared <summary.json> <definitions dir>
        prints "<versions table> <시도>" when the run committed a shadow and the table's definition
        declares the check (the 시도 it names, default 41); prints nothing otherwise.
    gold_takeover_check.py report <versions table> <시도> <verdict exit status> <verdict work dir>
        prints the Dagster Pipes `report_asset_check` line the op that ran the unit logs as the
        check's evaluation (orchestration/dagster/takeover_checks.py): passed only on exit 0.
"""

import json
import pathlib
import sys

CHECK = "takeover_equal"
DEFAULT_SIDO = "41"
PIPES_PROTOCOL_VERSION = "0.1"
OUTCOMES = {0: "equal", 1: "not equal", 3: "a national check failed"}


def declared(summary, definitions):
    """(table, 시도) of the check a committed shadow run's table declares, or None."""
    if summary.get("outcome") != "committed" or not summary.get("shadow_of"):
        return None
    table = summary["table"]
    for definition in definitions:
        assets = {asset["id"]: asset.get("table_name") for asset in definition.get("assets", [])}
        for check in definition.get("asset_checks", []):
            if check["check"] == CHECK and assets.get(check["asset"]) == table:
                return table, check.get("sido", DEFAULT_SIDO)
    return None


def report(table, sido, status, verdict, checks):
    """The Pipes message of one verdict: its outcome, the 시도's comparison and the national checks."""
    scope = verdict.get("scope") or {}
    metadata = {
        "outcome": OUTCOMES.get(status, f"the verdict did not run (exit {status})"),
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
        "passed": status == 0, "severity": "ERROR",
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
    if argv[:1] == ["declared"] and len(argv) == 3:
        definitions = [_read(path) for path in sorted(pathlib.Path(argv[2]).glob("*.definition.json"))]
        found = declared(_read(argv[1]), definitions)
        if found:
            print(*found)
        return 0
    if argv[:1] == ["report"] and len(argv) == 5:
        table, sido, status, work = argv[1], argv[2], int(argv[3]), pathlib.Path(argv[4])
        line = report(table, sido, status, _read(work / "verdict.json"), _read(work / "checks.json"))
        print(json.dumps(line, ensure_ascii=False))
        return 0
    print(__doc__, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
