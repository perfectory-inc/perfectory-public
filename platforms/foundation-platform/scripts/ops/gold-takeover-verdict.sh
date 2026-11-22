#!/usr/bin/env bash
# one-off: until the last served Gold has moved to its _versions table
#
# The verdict a takeover PR carries (root ADR-0564 §6, ADR-0559 §5): are the current rows the
# shadow `gold.<x>_versions` built (DuckDB) the rows of the served `gold.<x>` build (Spark) whose
# input pins it read?
#
#   gold-takeover-verdict.sh <versions table> [시도]   e.g. gold.parcel_nearby_versions 41
#
# The rows compared are one 시도's (two digits; default 41, 경기도, the largest — pick the one that
# broke before when one did). The whole table is checked by aggregates only (checks.json: rows, unique
# keys, non-null counts per column), with no national copy; the switch's one full re-bake is the
# national comparison (root ADR-0559 §5, ADR-0564 §6-3). The publisher writes the 시도's two sides as
# Parquet with sides.json, and engine_equality.py compares them by key through the publisher's DuckDB
# (the release images have no Python duckdb). Exits 0 when equal, 1 when not or when a national check
# fails; verdict.json and checks.json in the work directory are the record.
set -euo pipefail
table="${1:?usage: gold-takeover-verdict.sh <gold.<x>_versions> [시도]}"
sido="${2:-41}"
[[ "${table}" =~ ^gold\.[a-z0-9_]+_versions$ ]] || {
  echo "gold-takeover-verdict: ${table} is not a versions table (gold.<x>_versions)" >&2
  exit 64
}
[[ "${sido}" =~ ^[0-9]{2}$ ]] || {
  echo "gold-takeover-verdict: ${sido} is not a two-digit 시도 code" >&2
  exit 64
}
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

work="/data/foundation-platform/gold-takeover/${table}/${sido}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${work}/spill"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TABLE="${table}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_SIDO="${sido}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_WORK_DIR="${work}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TEMP_DIRECTORY="${work}/spill"

"${PUBLISHER_BIN}" export-gold-takeover-sides | tail -n 1
# The input pins the report records are the ones sides.json carries.
exec python3 -I -B "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/engine_equality.py" \
  --sides "${work}/sides.json" --runner-command "${PUBLISHER_BIN} query-takeover-sides" \
  --job "${table}" --reference-engine spark --candidate-engine duckdb --report "${work}/verdict.json"
