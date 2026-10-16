#!/usr/bin/env bash
# Proves migration-approval-gates.sh refuses what it exists to refuse (root ADR-0259 §4). The replay
# is #465's own migration, copied from the tree: merged with no declaration it is refused, as its
# deploy was; declared without the day production got the setting it is refused; declared with it
# it passes. A stale entry and a gate in another area's migrations are refused too.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/migration-approval-gates.sh"
name="migration-approval-gates-self-test"
gate="platforms/foundation-platform/migrations/20261014021000_drop_the_catalog_parcel_building_and_unit_copies.sql"
setting="foundation.adr_0194_drop_approved"
fixture="$(mktemp -d)"
cleanup() {
  case "${fixture:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$fixture" ;;
    *) echo "FAIL $name: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT
mkdir -p "$fixture/tools/ci" "$fixture/$(dirname "$gate")" "$fixture/products/x/migrations"
printf 'CREATE TABLE ordinary (id int);\n' >"$fixture/products/x/migrations/20260101000000_plain.sql"

contract() { # contract <set_in_production or "">: one entry for the #465 gate, or none when no args
  if (($# == 0)); then
    printf '{"gates": []}\n' >"$fixture/tools/ci/migration-approval-gates.contract.json"
    return
  fi
  printf '{"gates": [{"migration": "%s", "setting": "%s", "value": "yes", "database": "foundation", "set_in_production": "%s"}]}\n' \
    "$gate" "$setting" "$1" >"$fixture/tools/ci/migration-approval-gates.contract.json"
}

expect() { # expect <label> pass|<text the refusal must hold>
  local label="$1" want="$2" output status=0
  output="$(bash "$checker" "$fixture" 2>&1)" || status=$?
  if [[ "$want" == pass ]]; then
    ((status == 0)) || { echo "FAIL $name: $label was refused:" >&2; echo "$output" >&2; exit 1; }
  elif ((status == 0)); then
    echo "FAIL $name: $label was accepted" >&2; exit 1
  elif ! grep -qF -- "$want" <<<"$output"; then
    echo "FAIL $name: $label was refused for another reason:" >&2; echo "$output" >&2; exit 1
  fi
}

bash "$checker" >/dev/null || { echo "FAIL $name: the repository was refused" >&2; bash "$checker" || true; exit 1; }

contract
expect "no migration and no entry" pass
cp "$gate" "$fixture/$gate"
expect "#465 merged with no declaration" "reads $setting in a DO block"
contract ""
expect "#465 declared without the day production got the setting" "has no set_in_production date"
contract "2026-10-14"
expect "#465 declared as set in production" pass
rm "$fixture/$gate"
expect "an entry whose migration is gone" "remove the entry"
contract
printf 'DO $$ BEGIN PERFORM current_setting(%s, true); END $$;\n' "'gz.approved'" \
  >"$fixture/products/x/migrations/20260102000000_gated.sql"
expect "a gate in another area" "reads gz.approved in a DO block"

echo "OK $name"
