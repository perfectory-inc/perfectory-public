#!/usr/bin/env bash
# Proves migration-naming refuses a malformed name and a version two files share, within one
# area and across areas, and accepts distinct well-formed versions.
set -euo pipefail
guard="$(cd "$(dirname "$0")" && pwd)/migration-naming.sh"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
mkdir -p "$root/products/gongzzang/migrations" "$root/platforms/a/migrations" "$root/platforms/b/migrations"
touch "$root/platforms/a/migrations/20260101000000_first.sql" \
      "$root/platforms/b/migrations/20260101000001_second.sql" \
      "$root/products/gongzzang/migrations/20260101000002_third.sql"

expect() {
  local want=$1 label=$2
  if bash "$guard" "$root" >"$root/out" 2>&1; then got=pass; else got=fail; fi
  if [ "$got" != "$want" ]; then
    cat "$root/out" >&2
    echo "FAIL migration-naming-self-test: $label: expected $want, got $got" >&2
    exit 1
  fi
}

expect pass "distinct versions"
touch "$root/platforms/a/migrations/20260101000000_same_area_twin.sql"
expect fail "a version twice in one area"
rm "$root/platforms/a/migrations/20260101000000_same_area_twin.sql"
touch "$root/platforms/b/migrations/20260101000000_other_area_twin.sql"
expect fail "a version twice across areas"
rm "$root/platforms/b/migrations/20260101000000_other_area_twin.sql"
touch "$root/platforms/a/migrations/2026_bad-name.sql"
expect fail "a malformed name"
echo "OK migration-naming-self-test"
