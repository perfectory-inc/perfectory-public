#!/usr/bin/env bash
# one-off: until the last served Gold has moved to its _versions table
#
# The verdict a takeover PR carries (root ADR-0564 §6, ADR-0559 §5): are the current rows the
# shadow `gold.<x>_versions` built (DuckDB) the rows of the served `gold.<x>` build (Spark) whose
# input pins it read?
#
#   gold-takeover-verdict.sh <versions table> <시도> <work directory>
#
# The shadow build's unit runs it after each shadow commit, in its own environment and pool, and
# reports the outcome as the asset check `takeover_equal` (gold-versions-build.sh,
# orchestration/dagster/takeover_checks.py); the 시도 is the one the check's declaration names.
#
# The rows compared are one 시도's (two digits; default 41, 경기도, the largest; the lane's owner names
# another when a verdict or a switch's content check found a difference in it before). The whole table
# is checked by aggregates DuckDB computes over the scan, with no row brought out (checks.json: rows,
# unique keys, the key hash sum, and of the columns that are not documents a hash sum and the non-null
# counts; root ADR-0559 §5, ADR-0564 §6-3 revisions, the last of 2026-11-24: a document's content is
# judged in the 시도 only, and the switch's full bake is its national check), and
# every candidate document of the 시도 is rendered by the lane's renderer and held to the schema
# registry (checks.json `documents`; a table no lane bakes is not, and the record says why). Both sides
# are compared as the values they say (foundation_publisher_core::json_values, the switch's reading:
# a null member absent, a number its value, an instant its time), row_digest (a hash of the bytes) not
# at all (ADR-0564 Revision 2026-11-22 (둘째)). The publisher writes the 시도's two sides a 시군구 a
# Parquet file (reference/, candidate/; one line a 시군구 in export.out) with sides.json, and
# engine_equality.py compares them by key through the publisher's DuckDB (the release images have no
# Python duckdb), one 시군구 at a time (sides.json `chunks`; the 시도 joined whole ran out of memory
# on the served parcel boundaries, 2026-11-23 17:38Z). Exits 0 when equal, 1 when not, 3 when a result check fails
# (a national aggregate or a document; the 시도's comparison is still made and recorded), any other
# code on an error of the run (an empty 시도 is one); verdict.json, which names the 시도 and carries the
# result checks, is the record.
set -euo pipefail
usage='usage: gold-takeover-verdict.sh <gold.<x>_versions> <시도> <work directory>'
table="${1:?${usage}}"
sido="${2:?${usage}}"
work="${3:?${usage}}"
# The sides of a Gold come from its build and its shadow commit; the unit prices' from the Spark join's
# newest batch and the same batch composed again at its pins (unit_official_price_versions_build).
case "${table}" in
  gold.*_versions) export_command=export-gold-takeover-sides ;;
  silver.unit_official_price_versions) export_command=export-unit-official-price-takeover-sides ;;
  *) export_command='' ;;
esac
[[ "${table}" =~ ^[a-z]+\.[a-z0-9_]+_versions$ && -n "${export_command}" ]] || {
  echo "gold-takeover-verdict: ${table} is not a versions table a verdict compares" >&2
  exit 64
}
[[ "${sido}" =~ ^[0-9]{2}$ ]] || {
  echo "gold-takeover-verdict: ${sido} is not a two-digit 시도 code" >&2
  exit 64
}
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# The caller's work directory (the build's run directory, on the SSD the unit's state lives on).
mkdir -p "${work}/spill"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TABLE="${table}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_SIDO="${sido}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_WORK_DIR="${work}"
export FOUNDATION_PLATFORM_GOLD_TAKEOVER_TEMP_DIRECTORY="${work}/spill"

status=0
"${PUBLISHER_BIN}" "${export_command}" >"${work}/export.out" 2>&1 || status=$?
tail -n 3 "${work}/export.out"
failed=0
if [[ "${status}" -ne 0 ]]; then
  # A result check that failed left its record and, past the national pass, the 시도's two sides:
  # they are still compared. Anything else is an error of the run.
  if python3 -I -B -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1], encoding="utf-8")).get("failed") else 1)' \
    "${work}/checks.json" 2>/dev/null; then
    echo "gold-takeover-verdict: a result check failed: ${work}/checks.json" >&2
    failed=1
  fi
  [[ "${failed}" -eq 1 && -f "${work}/sides.json" ]] || exit "$((failed == 1 ? 3 : status))"
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
[[ "${failed}" -eq 0 ]] || exit 3
exit "${verdict}"
