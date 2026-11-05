#!/usr/bin/env bash
# 규제 구역(용도지역지구) 지도 타일을 Silver 의 바뀐 판에서 굽는다 (root ADR-0373). 작업 zoning_tile_bake.
#
#   1. 넘김 — land_use_zone_area_tile_handoff.py 가 silver.land_use_zone_area 의 현재 행을 한 스냅숏에 고정해
#             굽기 넘김(v1)으로 쓴다. 그 스냅숏이 이미 타일에 나가 있으면 아무것도 읽지 않고 끝난다(unchanged).
#   2. 굽기 — bake-lakehouse-tiles unit=zoning. 처음이면 유닛이 이 굽기로 태어난다(root ADR-0374). 그 뒤로는
#             바뀐 구역이 닿는 타일만 패치로 낸다(root ADR-0323). 패치 한도를 넘으면 새 기본판을 굽되, 층
#             (feature id 앞 네 글자)마다 행이 그대로인 층의 archive 는 다시 쓰지 않는다(root ADR-0316).
#
# 모든 바이트는 tile-bake 계약의 work_disk.production_root(/data) 아래에 둔다. 실행 폴더는 증거로 남는다.
# 수집 대기 층(RAON 전용, 레인 계약 pending_regions)은 넘김 요약과 journal 에 적힌다.
# 마지막 줄은 `foundation-job-outcome changed|unchanged` 다(root ADR-0171).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"

work_root="$(job_state_root zoning_tile_bake)"
STATE_ROOT="${work_root}/zoning"
journal="${STATE_ROOT}/journal.log"
# 마지막으로 타일에 나간 Silver 스냅숏. 굽기가 승격까지 끝난 뒤에만 쓴다.
baked_file="${STATE_ROOT}/baked-snapshot"
mkdir -p "${STATE_ROOT}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
lakehouse_root="${work_root}/lakehouse"
work="${lakehouse_root}/zoning-tile-bake/${run_id}"
container_work="/workspace/target/lakehouse/zoning-tile-bake/${run_id}"
mkdir -p "${work}"
chmod 0777 "${lakehouse_root}" "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"

on_error() {
  job_journal "${journal}" "zoning tile bake FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

served=""
[[ -s "${baked_file}" ]] && served="$(cat "${baked_file}")"

# 1. 넘김. 이미 나간 스냅숏이면 3 으로 끝난다.
handoff_status=0
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${lakehouse_root}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run foundation-platform-compute \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[8,8]' --driver-memory 16g --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/land_use_zone_area_tile_handoff.py \
  --output "${container_work}/served.jsonl" --summary-output "${container_work}/served-summary.json" \
  --served-snapshot "${served}" >> "${run_log}" 2>&1 || handoff_status=$?
if ((handoff_status == 3)); then
  job_journal "${journal}" "nothing to do: the zoning tiles already show Silver snapshot ${served}"
  echo "foundation-job-outcome unchanged"; exit 0
fi
if ((handoff_status == 65)); then
  # 현재 행이 키마다 하나가 아니다: 굽지 않고, 어느 키인지 적은 보고서를 남긴 채 실패한다.
  job_journal "${journal}" "refused: $(grep -o 'land-use-zone-area-tile-handoff-refused.*' "${run_log}" | tail -1)" >&2
  job_run_log_tail "${run_log}"
  exit 65
fi
((handoff_status == 0)) || { on_error "${LINENO}"; exit "${handoff_status}"; }

read -r snapshot pending <<<"$(python3 -I - "${work}/served-summary.json" <<'PY'
import json, sys
summary = json.load(open(sys.argv[1], encoding="utf-8"))
pending = ",".join(f"{p['layer_code']}({p['name']})" for p in summary["pending_layers"]) or "none"
print(summary["canonical_iceberg_snapshot_id"], pending)
PY
)"

# 2. 굽기·승격. 규제 구역에는 편집 저장소가 없다: 넘김의 edits_through_change_seq 는 0 이다.
step=${SECONDS}
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT=zoning \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/served.jsonl" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/served-summary.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work_root}/bake" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="zoning-tile-bake-${run_id}-build" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="zoning-tile-bake-${run_id}-promote" \
  "${PUBLISHER_BIN}" bake-lakehouse-tiles >> "${run_log}" 2>&1
result="$(grep -oE 'lakehouse-tile-bake-(ok|patch-[a-z]+) .*' "${run_log}" | tail -1)"
printf '%s\n' "${snapshot}" > "${baked_file}.tmp"
mv "${baked_file}.tmp" "${baked_file}"
rm -f "${work}/served.jsonl"
job_journal "${journal}" "baked Silver snapshot ${snapshot} in $((SECONDS - step))s pending_layers=${pending}: ${result}"
echo "foundation-job-outcome changed"
