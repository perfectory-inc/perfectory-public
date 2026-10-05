#!/usr/bin/env bash
# Proves runtime-secrets-contract refuses what it exists to refuse: the 2026-10-05 unit whose files
# lack the key its script requires, a hand-edited EnvironmentFile line, and a runbook that writes
# one by hand. Each is planted in a copy of the tree; the copy must pass before anything is planted.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/runtime-secrets-contract.sh"
name="runtime-secrets-contract-self-test"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
area="$fixture/platforms/foundation-platform"

fresh() {
  rm -rf "$fixture/platforms"
  mkdir -p "$area/scripts" "$area/infra" "$area/docs"
  cp -R "$root/platforms/foundation-platform/config" "$area/config"
  cp -R "$root/platforms/foundation-platform/infra/systemd" "$area/infra/systemd"
  cp -R "$root/platforms/foundation-platform/scripts/ops" "$root/platforms/foundation-platform/scripts/recovery" \
    "$root/platforms/foundation-platform/scripts/deploy" "$area/scripts/"
  cp -R "$root/platforms/foundation-platform/docs/runbooks" "$area/docs/runbooks"
}

expect_pass() {
  bash "$checker" "$fixture" >/dev/null 2>&1 || {
    echo "FAIL $name: $1 was refused" >&2
    bash "$checker" "$fixture" >&2 || true
    exit 1
  }
}

expect_fail() {
  local output
  if output="$(bash "$checker" "$fixture" 2>&1)"; then
    echo "FAIL $name: $1 was accepted" >&2
    exit 1
  fi
  grep -qF "$2" <<<"$output" || {
    echo "FAIL $name: $1 was refused for another reason:" >&2
    echo "$output" >&2
    exit 1
  }
}

fresh
expect_pass "the repository's own tree"

# 2026-10-05: the unit loses the reader file its script needs.
python3 - "$area/config/runtime-secrets.contract.json" <<'PY'
import json, sys
path = sys.argv[1]
contract = json.load(open(path, encoding="utf-8"))
unit = next(c for c in contract["consumers"] if c.get("unit") == "foundation-parcel-number-change.service")
unit["needs"] = {k: v for k, v in unit["needs"].items() if v != "lakehouse-reader"}
json.dump(contract, open(path, "w", encoding="utf-8"), indent=2)
PY
python3 "$area/scripts/deploy/runtime_secrets.py" --area "$area" render >/dev/null
expect_fail "a unit without the reader key its script requires" \
  "its ExecStart requires FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID"

fresh
sed -i 's|^EnvironmentFile=/etc/foundation-platform/recovery.env$|&\nEnvironmentFile=/etc/foundation-platform/parcel-publication.env|' \
  "$area/infra/systemd/foundation-parcel-number-change.service"
expect_fail "a hand-edited EnvironmentFile line" "foundation-parcel-number-change.service: EnvironmentFile lines"

fresh
printf 'sudo systemd-run -p EnvironmentFile=/etc/foundation-platform/recovery.env x\n' > "$area/docs/runbooks/planted.md"
expect_fail "a runbook that hand-writes an EnvironmentFile" "docs/runbooks/planted.md:1: hand-written EnvironmentFile"

echo "OK $name"
