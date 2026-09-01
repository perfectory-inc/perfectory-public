#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/every-table-declares-its-load.sh"
real="$(pwd -P)/platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL every-table-declares-its-load-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

fixture() {
  local name="$1" edit="$2"
  local root="$test_root/$name"
  mkdir -p "$root/platforms/foundation-platform/infra/lakehouse/contracts"
  python3 - "$real" "$root/platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json" "$edit" <<'PY'
import json, sys
source, target, edit = sys.argv[1], sys.argv[2], sys.argv[3]
doc = json.loads(open(source, encoding="utf-8").read())
table = "silver.parcel_boundaries"
if edit == "drop":
    doc["contracts"][table].pop("load", None)
elif edit == "unknown-kind":
    doc["contracts"][table]["load"]["unit"] = "whatever"
elif edit == "no-column":
    doc["contracts"][table]["load"].pop("column", None)
elif edit == "absent-column":
    doc["contracts"][table]["load"]["column"] = "a_column_that_is_not_there"
open(target, "w", encoding="utf-8").write(json.dumps(doc))
PY
  printf '%s' "$root"
}

expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL every-table-declares-its-load-self-test: accepted forbidden fixture ($2)" >&2
    exit 1
  fi
}

bash "$checker" "$(pwd -P)" >/dev/null || {
  echo "FAIL every-table-declares-its-load-self-test: rejected the real artifact" >&2
  exit 1
}

expect_rejected "$(fixture drop drop)" "선언이 없다"
expect_rejected "$(fixture kind unknown-kind)" "모르는 단위 이름"
expect_rejected "$(fixture nocol no-column)" "읽을 칸이 없다"
# 선언한 칸이 표에 없으면 적재는 그 자리에서 죽는다. 전국 실행이 몇 시간 돈 뒤에.
expect_rejected "$(fixture absent absent-column)" "없는 칸을 가리킨다"

echo "OK every-table-declares-its-load-self-test"
