#!/usr/bin/env bash
# Whether each cadastral edition is all there (root ADR-0291 §2, ADR-0379): measure what the host
# holds and write the readiness report the operator console shows.
#
#   parcel-edition-readiness.sh            write ${OUT}
#
# Read-only on everything it measures, and each read is the smallest that answers its stage:
#   silver     silver."parcel_boundaries$partitions" through Trino: the table's partition metadata
#              (rows per edition, ADR-0316), no data file is read — under a second on 2026-11-04;
#   source     the newest provider inventory the daily edition collection kept
#              (vworld-parcel-edition-collect.sh), checked for re-uploads per edition;
#   rehearsal  the parcel tile bake's journal (parcel-tile-bake.sh), its rehearsal lines.
# What it cannot read is left out, and that stage is reported not ready: unmeasured is not ready.
# The report is replaced whole (it is a view of now, not a record); the record of what was served
# is the contract's promotions ledger.
set -euo pipefail

RELEASE_ROOT="${FOUNDATION_PLATFORM_RELEASE_ROOT:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
JOBS="${RELEASE_ROOT}/infra/lakehouse/spark/jobs"
OUT="${FOUNDATION_PARCEL_EDITION_READINESS_PATH:-/var/lib/foundation-platform/parcel-editions/readiness.json}"
EDITION_STATE="${FOUNDATION_VWORLD_PARCEL_EDITION_STATE_ROOT:-/var/lib/foundation-platform/vworld-parcel-edition}"
TILE_JOURNAL="${FOUNDATION_PARCEL_TILE_BAKE_STATE_ROOT:-/var/lib/foundation-parcel-tile-bake}/journal.log"
TRINO_CONTAINER="${FOUNDATION_PARCEL_EDITION_TRINO_CONTAINER:-foundation-platform-trino}"
PY=(python3 -E -s)

work="$(mktemp -d)"
trap 'rm -rf -- "${work}"' EXIT

partitions=()
if printf '%s\n' 'SELECT partition.source_snapshot_id, record_count FROM silver."parcel_boundaries$partitions"' |
  timeout 120 docker exec -i "${TRINO_CONTAINER}" trino --server localhost:8080 --catalog r2 --output-format CSV \
    --file /dev/stdin > "${work}/partitions.csv" 2> "${work}/trino.err"; then
  partitions=(--partitions "${work}/partitions.csv")
else
  echo "parcel-edition-readiness: silver not measured: $(tail -1 "${work}/trino.err")" >&2
fi

inventory=()
newest="$(ls -1d "${EDITION_STATE}"/runs/*/ 2>/dev/null | sort | tail -1 || true)"
if [ -n "${newest}" ] && [ -f "${newest}inventory.json" ]; then
  inventory=(--inventory "${newest}inventory.json")
else
  echo "parcel-edition-readiness: source not checked: no provider inventory under ${EDITION_STATE}/runs" >&2
fi

"${PY[@]}" "${JOBS}/parcel_edition_readiness.py" evidence "${partitions[@]}" "${inventory[@]}" \
  --journal "${TILE_JOURNAL}" --output "${work}/evidence.json"
mkdir -p "$(dirname "${OUT}")"
"${PY[@]}" "${JOBS}/parcel_edition_readiness.py" readiness --evidence "${work}/evidence.json" --output "${OUT}.tmp"
mv -f -- "${OUT}.tmp" "${OUT}"
"${PY[@]}" -c 'import json, sys
for row in json.load(open(sys.argv[1], encoding="utf-8"))["editions"]:
    print(row["edition"], "ready" if row["ready"] else "not ready: " + row["first_failing_stage"])' "${OUT}"
