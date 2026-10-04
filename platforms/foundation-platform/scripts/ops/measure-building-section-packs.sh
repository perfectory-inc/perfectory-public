#!/usr/bin/env bash
# Measures one full building section pack bake (root ADR-0147) without writing anything served.
#
#   measure-building-section-packs.sh <output-dir>
#
# An operator runs it once, before the cut-over gate, with the environment the by-PNU bake unit
# reads (its EnvironmentFile lines, infra/systemd/foundation-by-pnu-serving-bake.service), e.g.
#
#   sudo systemd-run --wait --collect --pipe -p User=foundation-platform -p MemoryMax=14G \
#     -p EnvironmentFile=/etc/foundation-platform/recovery.env \
#     -p EnvironmentFile=/etc/foundation-platform/source-sweep.env \
#     -p EnvironmentFile=/etc/foundation-platform/map-edit-fold.env \
#     /opt/foundation-platform/current/scripts/ops/measure-building-section-packs.sh \
#     /data/foundation-platform/by-pnu-bake/building-pack-measure
#
# What it guarantees:
# - packs go only under <output-dir> on the data disk (the local output driver); the R2 driver is
#   never set and the R2 writer keys are removed from the environment, so a write cannot reach the
#   bucket. The Gold scan and the approved-link read are reads.
# - the admitted current release's publisher runs it (admitted-writer-runtime.sh), shard by shard
#   along the building lane's remembered shard plan, each under the time tool for wall time and
#   peak resident memory.
# - it prints one summary JSON: counts, bytes, seconds, peak memory, and the R2 writes a real bake
#   would make. No environment value is printed.
set -euo pipefail

log() { printf '%s measure-building-section-packs: %s\n' "$(date -u +%FT%TZ)" "$*"; }
refuse() { log "refused: $1"; exit "${2:-64}"; }

OUT="${1:-}"
[[ -n "${OUT}" && "${OUT}" == /* ]] || refuse "usage: measure-building-section-packs.sh <absolute output dir>"
DATA_ROOT="${FOUNDATION_PACK_MEASURE_DATA_ROOT:-/data/}"
[[ "${OUT}" == "${DATA_ROOT}"* ]] || refuse "the output must be on the data disk (${DATA_ROOT}), never the root disk"
if [[ -e "${OUT}" && -n "$(ls -A "${OUT}")" ]]; then
  refuse "${OUT} is not empty; a measurement starts from nothing so its counts are its own"
fi
TIME_BIN="${FOUNDATION_PACK_MEASURE_TIME_BIN:-/usr/bin/time}"
[[ -x "${TIME_BIN}" ]] || refuse "${TIME_BIN} is not there; install GNU time" 69

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
ENV_PREFIX=FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING
STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}/building"

# No write can reach R2: the writer keys go, and the output is local.
unset FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
for name in $(compgen -e | grep "^${ENV_PREFIX}_" || true); do unset "${name}"; done
export "${ENV_PREFIX}_OUTPUT_STORAGE_DRIVER=local" "${ENV_PREFIX}_OUTPUT_ROOT=${OUT}/packs"
export "${ENV_PREFIX}_CONFIRM_PACK_EXPORT=true" "${ENV_PREFIX}_PACK_GENERATION=1"

# The building export reads the approved building links, from the same connection the bake uses.
if [[ -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(docker compose --project-directory "${RELEASE_ROOT}" \
    --env-file /dev/null -f "${RELEASE_ROOT}/docker-compose.yml" \
    config --format json --no-env-resolution 2>/dev/null \
    | python3 "${RELEASE_ROOT}/scripts/ops/runtime-database-url.py")"; then
    refuse "cannot resolve the runtime database connection the building export reads" 78
  fi
  export DATABASE_URL
fi

plan=()
if [[ -s "${STATE_ROOT}/shard-plan.txt" ]]; then
  mapfile -t plan < <(grep -E '[^[:space:]]' "${STATE_ROOT}/shard-plan.txt")
else
  plan=(1 2 3 4 5 6 7 8 9)
fi
for prefix in "${plan[@]}"; do
  [[ "${prefix}" =~ ^[0-9]{1,10}$ ]] || refuse "shard plan holds '${prefix}'" 65
done

mkdir -p "${OUT}/packs" "${OUT}/summaries" "${OUT}/times"
snapshot=""
for prefix in "${plan[@]}"; do
  log "shard ${prefix}"
  pinned=()
  [[ -z "${snapshot}" ]] || pinned=("${ENV_PREFIX}_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID=${snapshot}")
  if ! env "${pinned[@]}" "${ENV_PREFIX}_PNU_PREFIX=${prefix}" \
    "${ENV_PREFIX}_PACK_SUMMARY_PATH=${OUT}/summaries/shard-${prefix}.json" \
    "${TIME_BIN}" -f '%e %M' -o "${OUT}/times/shard-${prefix}" \
    "${PUBLISHER_BIN}" export-building-by-pnu-section-packs > "${OUT}/times/shard-${prefix}.log" 2>&1; then
    refuse "shard ${prefix} failed; see ${OUT}/times/shard-${prefix}.log" 70
  fi
  snapshot="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["gold_iceberg_snapshot_id"])' \
    "${OUT}/summaries/shard-${prefix}.json")"
done

python3 -I - "${OUT}" "${plan[@]}" <<'PY'
import json, pathlib, sys
out, plan = pathlib.Path(sys.argv[1]), sys.argv[2:]
summaries = [json.loads((out / "summaries" / f"shard-{p}.json").read_text()) for p in plan]
times = [(out / "times" / f"shard-{p}").read_text().split()[-2:] for p in plan]
snapshots = {s["gold_iceberg_snapshot_id"] for s in summaries}
sections = {}
for summary in summaries:
    for name, total in summary["totals"].items():
        into = sections.setdefault(name, {key: 0 for key in total})
        for key, value in total.items():
            into[key] = max(into[key], value) if key.startswith("largest_") else into[key] + value
packs = sum(s["packs"] for s in sections.values())
result = {
    "schema_version": "foundation-platform.building_section_pack_measurement.v1",
    "gold_iceberg_snapshots": sorted(snapshots),
    "shards": len(plan),
    "documents": sum(s["exported_row_count"] for s in summaries),
    "sections": sections,
    "packs": packs,
    "pack_bytes": sum(s["bytes"] for s in sections.values()),
    "head_bytes": sum(s["head_bytes"] for s in sections.values()),
    "bake_seconds": round(sum(float(t[0]) for t in times), 1),
    "slowest_shard_seconds": max(float(t[0]) for t in times),
    "peak_resident_bytes": max(int(t[1]) for t in times) * 1024,
    # One create-only PUT per pack; a publish adds the manifest and its history copy.
    "projected_r2_class_a_writes": packs + 2,
}
print(json.dumps(result, indent=2, sort_keys=True))
(out / "measurement.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
if len(snapshots) != 1:
    sys.exit("the shards read more than one Gold snapshot; measure again")
PY
