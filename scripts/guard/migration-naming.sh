#!/usr/bin/env bash
# Prevents: sqlx version-ordering chaos across areas (observed 3 naming schemes), and two
# migrations with one version: sqlx keys a migration by the 14-digit number alone, so a second
# file with that number is refused or skipped (PR #636 and #633 both took 20261016120000).
# Convention: YYYYMMDDHHMMSS_<snake_case>.sql (docs/adr/0001 §7); the version is unique tree-wide.
set -euo pipefail
cd "${1:-$(dirname "$0")/../..}"
shopt -s nullglob
fail=0
declare -A seen=()
for f in products/gongzzang/migrations/*.sql platforms/*/migrations/*.sql; do
  base=$(basename "$f")
  if ! echo "$base" | grep -qE '^[0-9]{14}_[a-z0-9_]+\.sql$'; then
    echo "FAIL migration-naming: $f (want YYYYMMDDHHMMSS_snake_case.sql)" >&2; fail=1
    continue
  fi
  version=${base:0:14}
  if [ -n "${seen[$version]:-}" ]; then
    echo "FAIL migration-naming: version $version is used by both ${seen[$version]} and $f" >&2; fail=1
  else
    seen[$version]=$f
  fi
done
[ "$fail" -eq 0 ] && echo "OK migration-naming"
exit "$fail"
