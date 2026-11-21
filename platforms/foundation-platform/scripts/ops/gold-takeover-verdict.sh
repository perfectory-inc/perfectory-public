#!/usr/bin/env bash
# one-off: until the last served Gold has moved to its _versions table
#
# The verdict a takeover PR carries (root ADR-0564 §6, ADR-0559 §5): are the current rows the
# shadow `gold.<x>_versions` built (DuckDB) the rows of the served `gold.<x>` build (Spark) whose
# input pins it read?
#
#   gold-takeover-verdict.sh <versions table>      e.g. gold.parcel_nearby_versions
#
# The publisher writes both sides as Parquet with sides.json, and engine_equality.py compares them
# by key through the publisher's DuckDB (the release images have no Python duckdb). Exits 0 when
# equal, 1 when not; verdict.json in the work directory is the record.
set -euo pipefail
table="${1:?usage: gold-takeover-verdict.sh <gold.<x>_versions>}"
[[ "${table}" =~ ^gold\.[a-z0-9_]+_versions$ ]] || {
  echo "gold-takeover-verdict: ${table} is not a versions table (gold.<x>_versions)" >&2
  exit 64
}
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

work="/data/foundation-platform/gold-takeover/${table}/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${work}/spill"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TABLE="${table}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_WORK_DIR="${work}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TEMP_DIRECTORY="${work}/spill"

"${PUBLISHER_BIN}" export-gold-takeover-sides | tail -n 1
# The input pins the report records are the ones sides.json carries.
exec python3 -I -B "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/engine_equality.py" \
  --sides "${work}/sides.json" --runner-command "${PUBLISHER_BIN} query-takeover-sides" \
  --job "${table}" --reference-engine spark --candidate-engine duckdb --report "${work}/verdict.json"
