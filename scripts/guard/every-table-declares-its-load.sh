#!/usr/bin/env bash
# Bounded guard: every lakehouse table says what one load of it carries.
#
# What failing this prevents: the re-run guard compares the identities a batch carries against
# the ones the table records, and it read one column on every table. Measured 2026-09-01, the six
# live tables used three different things — an object key, a collection run, and nothing at all.
# Read as one kind, five of the six recorded no identity it could use, and re-running any of their
# loads would have appended 133,583,046 rows a second time (root ADR-0069).
#
# A table added without a declaration is the same hole reopened, and it opens silently: the load
# succeeds, the registry stays empty, and nothing says so until the table has been doubled.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
artifact="$repo_root/platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"

command -v python3 >/dev/null 2>&1 || {
  echo "FAIL every-table-declares-its-load: python3 is required" >&2
  exit 1
}
[ -f "$artifact" ] || {
  echo "FAIL every-table-declares-its-load: missing $artifact" >&2
  exit 1
}

python3 - "$artifact" <<'PY'
import json
import sys

KINDS = {"object", "run", "derived"}
artifact = json.loads(open(sys.argv[1], encoding="utf-8").read())
contracts = artifact.get("contracts")
if not isinstance(contracts, dict) or not contracts:
    print("FAIL every-table-declares-its-load: contracts object is missing or empty", file=sys.stderr)
    raise SystemExit(1)

problems = []
for name, spec in sorted(contracts.items()):
    declared = spec.get("load")
    if not isinstance(declared, dict):
        problems.append(f"{name}: declares no load unit")
        continue
    unit = declared.get("unit")
    if unit not in KINDS:
        problems.append(f"{name}: load unit {unit!r} is not one of {sorted(KINDS)}")
        continue
    if unit == "derived":
        continue
    column = declared.get("column")
    if not isinstance(column, str) or not column:
        problems.append(f"{name}: {unit} load declares no column for the guard to read")
        continue
    # 선언한 칸이 실제로 그 표에 있어야 한다. 없는 칸을 가리키면 적재는 그 자리에서 죽고,
    # 죽는 자리는 전국 실행이 이미 몇 시간 돈 뒤다.
    columns = {
        c.get("name"): c.get("required")
        for c in spec.get("columns", [])
        if isinstance(c, dict)
    }
    if column not in columns:
        problems.append(f"{name}: reads {column!r}, which the table does not have")
        continue
    # 그리고 그 칸은 필수여야 한다. 선택이면 적재기가 칸을 조용히 추가하고 기존 행을 전부
    # 비워 둔 채 성공하며, 안전장치는 셀 것이 없는 채로 통과한다. 2026-09-01 기준 아홉 계약
    # 모두 이 성질을 이미 만족한다 — 이 검사는 그것이 계속 참이게 한다 (root ADR-0069).
    if columns[column] is not True:
        problems.append(f"{name}: reads {column!r}, which the contract marks optional")

if problems:
    print("FAIL every-table-declares-its-load:", file=sys.stderr)
    for problem in problems:
        print(f"    {problem}", file=sys.stderr)
    print("    root ADR-0069", file=sys.stderr)
    raise SystemExit(1)

print(f"OK every-table-declares-its-load (tables={len(contracts)})")
PY
