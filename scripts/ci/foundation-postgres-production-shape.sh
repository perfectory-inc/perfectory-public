#!/usr/bin/env bash
# The Postgres integration job's production-shape step (root ADR-0259 §5).
#
#   foundation-postgres-production-shape.sh <database url> <base commit>
#
# Only when the change touches Foundation's migrations or this gate (a change that does neither
# pays for its unit tests alone): each in its own new database on the job's server,
#   1. the self-test: onto the seeded rows a planted nullable column applies and a planted required
#      column fails, as it would on production's rows (and passes on an empty database), so a seed
#      that silently filled nothing cannot pass;
#   2. the change's new migrations, one at a time, onto main's schema holding production's rows.
set -euo pipefail

url="${1:?usage: foundation-postgres-production-shape.sh <database url> <base commit>}"
base="${2:-}"
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
gate="${root}/scripts/ci/foundation_postgres_production_shape.py"
python3 "${root}/scripts/ci/test_foundation_postgres_production_shape.py"

if [[ -z "${base}" || "${base}" =~ ^0+$ ]]; then
  echo "postgres-production-shape: no base commit (a manual run); the change cannot be told apart"
  base=HEAD
  touched=all
else
  git -C "${root}" cat-file -e "${base}^{commit}" 2>/dev/null \
    || git -C "${root}" fetch -q --no-tags --depth=1 origin "${base}"
  touched="$(git -C "${root}" diff --name-only "${base}" HEAD -- \
    platforms/foundation-platform/migrations \
    platforms/foundation-platform/config/postgres-production-shape.json \
    platforms/foundation-platform/config/postgres-seed-hints.json \
    scripts/ci/foundation_postgres_production_shape.py scripts/ci/foundation-postgres-production-shape.sh)"
fi
if [[ -z "${touched}" ]]; then
  echo "postgres-production-shape: no migration or seed change; nothing to apply onto rows"
  exit 0
fi

server="${url%/*}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
psql "${url}" -X -q -v ON_ERROR_STOP=1 -c "create database shape_self_test" -c "create database shape_change"

printf 'ALTER TABLE catalog.industrial_complex ADD COLUMN planted_note text;\n' >"${work}/planted_nullable.sql"
printf 'ALTER TABLE catalog.industrial_complex ADD COLUMN planted_required text NOT NULL;\n' >"${work}/planted_required.sql"
status=0
python3 "${gate}" --database-url "${server}/shape_self_test" --base HEAD \
  --try "${work}/planted_nullable.sql" --try "${work}/planted_required.sql" >"${work}/self-test.log" 2>&1 || status=$?
cat "${work}/self-test.log"
if ((status == 0)) || ! grep -q "planted_nullable.sql applied onto rows" "${work}/self-test.log" \
  || ! grep -q "planted_required.sql fails on tables that hold rows" "${work}/self-test.log"; then
  echo "postgres-production-shape: FAILED: the self-test's required column did not fail on the seeded rows" >&2
  exit 1
fi
echo "postgres-production-shape: self-test OK"

python3 "${gate}" --database-url "${server}/shape_change" --base "${base}"
