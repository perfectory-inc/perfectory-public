#!/usr/bin/env bash
# A changed migration never does an expand-contract-unsafe thing unguarded (ADR-0299 계열 E, ADR-0300 P1).
#
# What failing this prevents: on 2026-10-13 #469 made a serving column required with no backfill; the
# deploy's migrate step refused it and the API stayed down until it was reverted. The Postgres class is
# an `ADD COLUMN ... NOT NULL` with no default and no backfill, and a lone `DROP`/`RENAME` of an object
# running code still reads. The rule lives in the Python checker; this wrapper finds Python and picks the
# migrations to check.
#
# Scope: only the migrations this change adds or edits (the pull request's base..head diff). A migration
# that already merged has already run against production and is append-only, so re-scanning the whole
# tree would only flag history. With no diff range (a push, the merge queue, a local pre-push hook) there
# is no new migration to judge, so the guard is a no-op — the pull request is where a new migration is
# caught, and the hook is advisory (root ADR-0005).
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
checker="$(cd "$(dirname "$0")" && pwd)/expand-contract-migrations.py"
py=python3
command -v python3 >/dev/null 2>&1 || py=python

base="${CHANGE_SCOPE_BASE:-}"
head="${CHANGE_SCOPE_HEAD:-}"
if [ -z "$base" ] || [ -z "$head" ]; then
  echo "OK expand-contract-migrations (no change range; a new migration is checked on its pull request)"
  exit 0
fi

changed=()
while IFS= read -r f; do
  case "$f" in
    */migrations/*.sql) changed+=("$f") ;;
  esac
done < <(git -C "$root" diff --name-only --diff-filter=d "$base" "$head" 2>/dev/null || true)

if [ "${#changed[@]}" -eq 0 ]; then
  echo "OK expand-contract-migrations (no migrations changed)"
  exit 0
fi

exec "$py" "$checker" "$root" "${changed[@]}"
