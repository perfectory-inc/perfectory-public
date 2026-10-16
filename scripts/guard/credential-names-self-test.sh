#!/usr/bin/env bash
# Proves credential-names refuses what it exists to refuse: a retired name (the old catalog writer
# token, built in pieces so this file spells no retired name whole), a made-up
# name, and an R2_ spelling of a derived one. Each is planted in a copy of the contract and its
# checker; the copy must pass before anything is planted, and a derived name must stay accepted.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/credential-names.sh"
name="credential-names-self-test"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
area="$fixture/platforms/foundation-platform"
mkdir -p "$area/scripts"
cp -R "$root/platforms/foundation-platform/config" "$area/config"
cp -R "$root/platforms/foundation-platform/scripts/deploy" "$area/scripts/deploy"
mkdir -p "$area/infra" && cp -R "$root/platforms/foundation-platform/infra/systemd" "$area/infra/systemd"
git -C "$fixture" init -q
git -C "$fixture" add .

prefix="FOUNDATION_PLATFORM"
# The catalog writer token's name before root ADR-0247, in pieces so this file spells it nowhere whole.
retired="${prefix}_LAKEHOUSE_""CATALOG_TOKEN"

probe() {
  local label="$1" value="$2" should_fail="$3" output
  printf 'export %s=planted\n' "$value" >"$fixture/planted.sh"
  git -C "$fixture" add planted.sh
  if output="$(bash "$checker" "$fixture" 2>&1)"; then
    [[ "$should_fail" == no ]] || { echo "FAIL $name: $label was accepted" >&2; exit 1; }
  else
    [[ "$should_fail" == yes ]] || { echo "FAIL $name: $label was refused:" >&2; echo "$output" >&2; exit 1; }
    grep -qF "planted.sh:1: $value is not derived" <<<"$output" || {
      echo "FAIL $name: $label was refused for another reason:" >&2
      echo "$output" >&2
      exit 1
    }
  fi
  git -C "$fixture" rm -q --cached planted.sh
  rm -f "$fixture/planted.sh"
}

bash "$checker" "$fixture" >/dev/null 2>&1 || { echo "FAIL $name: the clean copy was refused" >&2; bash "$checker" "$fixture" >&2 || true; exit 1; }
probe "the retired catalog writer name" "$retired" yes
probe "a made-up credential name" "${prefix}_LAKEHOUSE_ADMIN_TOKEN" yes
probe "an R2_ spelling of a derived name" "${prefix}_R2_LAKEHOUSE_READER_ACCESS_KEY_ID" yes
probe "a derived name" "${prefix}_LAKEHOUSE_CATALOG_READER_TOKEN" no
# An Accepted ADR keeps the names of its day.
mkdir -p "$fixture/docs/adr"
printf '%s\n' "$retired" >"$fixture/docs/adr/0001-history.md"
git -C "$fixture" add docs/adr/0001-history.md
bash "$checker" "$fixture" >/dev/null 2>&1 || { echo "FAIL $name: an ADR's retired name was refused" >&2; exit 1; }

echo "OK $name"
