#!/usr/bin/env bash
# Prevents: sqlx version-ordering chaos across areas (observed 3 naming schemes).
# Convention: YYYYMMDDHHMMSS_<snake_case>.sql (docs/adr/0001 §7).
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
for f in products/gongzzang/migrations/*.sql platforms/*/migrations/*.sql; do
  base=$(basename "$f")
  if ! echo "$base" | grep -qE '^[0-9]{14}_[a-z0-9_]+\.sql$'; then
    echo "FAIL migration-naming: $f (want YYYYMMDDHHMMSS_snake_case.sql)" >&2; fail=1
  fi
done
[ "$fail" -eq 0 ] && echo "OK migration-naming"
exit "$fail"
