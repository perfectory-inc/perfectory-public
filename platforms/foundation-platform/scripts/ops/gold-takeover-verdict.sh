#!/usr/bin/env bash
# one-off: until the last served Gold has moved to its _versions table
#
# The verdict a takeover PR carries (root ADR-0564 §6, ADR-0559 §5): are the current rows the
# shadow `gold.<x>_versions` built (DuckDB) the rows of the served `gold.<x>` build (Spark) whose
# input pins it read?
#
#   gold-takeover-verdict.sh <versions table> [시도]   e.g. gold.parcel_nearby_versions 41
#
# The rows compared are one 시도's (two digits; default 41, 경기도, the largest; the lane's owner names
# another when a verdict or a switch's content check found a difference in it before). The whole table
# is checked by aggregates only, with no national copy (checks.json: rows, unique keys, the key hash
# sum, the row_digest sum, non-null counts per column; root ADR-0559 §5, ADR-0564 §6-3 revisions). The
# publisher writes the 시도's two sides as Parquet with sides.json, and engine_equality.py compares them
# by key through the publisher's DuckDB (the release images have no Python duckdb). Exits 0 when equal,
# 1 when not, 3 when a national check fails, any other code on an error of the run (an empty 시도 is
# one); verdict.json, which names the 시도 and carries the national checks, is the record.
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

status=0
"${PUBLISHER_BIN}" export-gold-takeover-sides >"${work}/export.out" 2>&1 || status=$?
tail -n 3 "${work}/export.out"
if [[ "${status}" -ne 0 ]]; then
  # A national result check that failed left its record; anything else is an error of the run.
  if [[ -s "${work}/checks.json" ]] && ! grep -q '"failed": \[\]' "${work}/checks.json"; then
    echo "gold-takeover-verdict: a national result check failed: ${work}/checks.json" >&2
    exit 3
  fi
  exit "${status}"
fi
# The input pins the report records are the ones sides.json carries.
verdict=0
python3 -I -B "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/engine_equality.py" \
  --sides "${work}/sides.json" --runner-command "${PUBLISHER_BIN} query-takeover-sides" \
  --job "${table}" --reference-engine spark --candidate-engine duckdb --report "${work}/verdict.json" || verdict=$?
# The record names its scope: the 시도 whose rows were compared and the national checks beside it.
python3 -I -B -c '
import json, sys
work = sys.argv[1]
report = json.load(open(work + "/verdict.json", encoding="utf-8"))
sides = json.load(open(work + "/sides.json", encoding="utf-8"))
report["scope"] = {"sido": sides["sido"], "sido_rows": sides["sido_rows"],
                   "rows_nationally": sides["rows_nationally"],
                   "national_checks": json.load(open(work + "/checks.json", encoding="utf-8"))}
with open(work + "/verdict.json", "w", encoding="utf-8") as out:
    json.dump(report, out, ensure_ascii=False, indent=2, sort_keys=True)
' "${work}"
exit "${verdict}"
