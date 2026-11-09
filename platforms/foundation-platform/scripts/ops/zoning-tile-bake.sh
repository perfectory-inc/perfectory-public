#!/usr/bin/env bash
# 규제 구역(용도지역지구) 지도 타일을 Silver 의 바뀐 판에서 굽는다 (root ADR-0373). 작업 zoning_tile_bake.
#
#   1. 넘김 — land_use_zone_area_tile_handoff.py 가 silver.land_use_zone_area 의 한 스냅숏에 고정한다. 그 스냅숏이
#             이미 타일에 나가 있으면 아무것도 읽지 않고 끝난다(unchanged). 아니면 먼저 타일 색인
#             gold.land_use_zone_area_by_tile 을 Silver 에 덧붙은 행만 읽어 맞추고, 서빙 중인 타일 이후 그 색인에
#             덧붙은 줄로 바뀐 구역과 그 구역이 닿는 z10 타일을 찾아, 그 타일의 현재 구역(이웃)과 바뀐 id 목록만
#             넘긴다(부분 넘김, root ADR-0375 §2).
#   2. 굽기 — bake-lakehouse-tiles unit=zoning. 처음이면 유닛이 이 굽기로 태어난다(root ADR-0374). 그 뒤로는
#             옛 지문 색인에 바뀐 id 목록을 합쳐 바뀐 구역이 닿는 타일만 패치로 낸다(root ADR-0323).
#   3. 부분 넘김은 기본판이 되지 않는다. 굽기가 패치를 못 하면(옛 색인 없음, 패치 한도 초과 등)
#      lakehouse-tile-bake-partial-declined 를 남기고 65 로 끝난다. 그러면 넘김을 --full 로 다시 해 현재 구역
#      전부를 넘기고(선언된 전량 예외) 다시 굽는다. 새 기본판은 층(feature id 앞 네 글자)마다 행이 그대로인
#      층의 archive 를 다시 쓰지 않는다(root ADR-0316).
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
# 마지막으로 타일에 나간 Silver 스냅숏과 그때의 타일 색인 스냅숏. 굽기가 승격까지 끝난 뒤에만 쓴다.
baked_file="${STATE_ROOT}/baked-snapshot"
baked_by_tile_file="${STATE_ROOT}/baked-by-tile-snapshot"
# 그때 굽기를 정한 tile-bake 계약(이미지·줌별 인자)의 sha256. 계약이 바뀌면 Silver 가 그대로여도 전부 다시
# 넘겨 굽는다: 코드가 바꾼 타일도 데이터처럼 따라간다(W42, 2026-11-09 저배율 일반화가 Silver 변경 전까지 안 나갈 뻔).
baked_contract_file="${STATE_ROOT}/baked-bake-contract"
contract_digest="$(sha256sum "$(dirname "${BASH_SOURCE[0]}")/../../config/tile-bake-containers.contract.json" | cut -d' ' -f1)"
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
served_by_tile=""
[[ -s "${baked_by_tile_file}" ]] && served_by_tile="$(cat "${baked_by_tile_file}")"
if [[ -n "${served}" && "$(cat "${baked_contract_file}" 2>/dev/null)" != "${contract_digest}" ]]; then
  # 넘김이 unchanged 로 끝나지 않게 서빙 스냅숏을 비운다: 넘김은 전량이 되고 새 기본판이 된다(선언된 전량 예외).
  job_journal "${journal}" "the tile-bake contract changed since Silver snapshot ${served} was baked: handing every zone"
  served=""
fi

# 넘김 한 번: $1 은 파일 이름 앞부분, 나머지는 넘김 인자. 이미 나간 스냅숏이면 3 으로 끝난다.
handoff() {
  local name="$1"; shift
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${lakehouse_root}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[8,8]' --driver-memory 16g --jars "${SPARK_RELEASE_JARS}" \
    /workspace/infra/lakehouse/spark/jobs/land_use_zone_area_tile_handoff.py \
    --output "${container_work}/${name}.jsonl" --summary-output "${container_work}/${name}-summary.json" \
    --served-snapshot "${served}" --served-by-tile-snapshot "${served_by_tile}" "$@" >> "${run_log}" 2>&1
}

# 요약에서 스냅숏·수집 대기 층·범위·타일 색인 스냅숏을 읽는다.
summary_fields() {
  python3 -I - "$1" <<'PY'
import json, sys
summary = json.load(open(sys.argv[1], encoding="utf-8"))
pending = ",".join(f"{p['layer_code']}({p['name']})" for p in summary["pending_layers"]) or "none"
print(summary["canonical_iceberg_snapshot_id"], pending, summary["handoff_scope"], summary["by_tile_iceberg_snapshot_id"])
PY
}

# 굽기·승격 한 번. 규제 구역에는 편집 저장소가 없다: 넘김의 edits_through_change_seq 는 0 이다.
bake() {
  local name="$1"
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT=zoning \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/${name}.jsonl" \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/${name}-summary.json" \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work_root}/bake" \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="zoning-tile-bake-${run_id}-${name}-build" \
  FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="zoning-tile-bake-${run_id}-${name}-promote" \
    "${PUBLISHER_BIN}" bake-lakehouse-tiles >> "${run_log}" 2>&1
}

# 1. 넘김.
handoff_status=0
handoff served || handoff_status=$?
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
read -r snapshot pending scope by_tile <<<"$(summary_fields "${work}/served-summary.json")"
read_line="$(grep -oE 'land-use-zone-area-tile-handoff-read .*' "${run_log}" | tail -1)"

# 2. 굽기. 3. 부분 넘김을 굽기가 받지 않으면(65, partial-declined) 전량으로 다시 넘기고 굽는다.
step=${SECONDS}
name=served
bake_status=0
bake "${name}" || bake_status=$?
if ((bake_status == 65)) && [[ "${scope}" == partial ]] && grep -q 'lakehouse-tile-bake-partial-declined' "${run_log}"; then
  declined="$(grep -oE 'lakehouse-tile-bake-partial-declined .*' "${run_log}" | tail -1)"
  job_journal "${journal}" "partial handoff of Silver snapshot ${snapshot} declined, handing every zone: ${declined}"
  rm -f "${work}/served.jsonl"
  name=full
  handoff "${name}" --full
  read -r snapshot pending scope by_tile <<<"$(summary_fields "${work}/${name}-summary.json")"
  read_line="$(grep -oE 'land-use-zone-area-tile-handoff-read .*' "${run_log}" | tail -1)"
  bake_status=0
  bake "${name}" || bake_status=$?
fi
((bake_status == 0)) || { on_error "${LINENO}"; exit "${bake_status}"; }
result="$(grep -oE 'lakehouse-tile-bake-(ok|patch-[a-z]+) .*' "${run_log}" | tail -1)"
printf '%s\n' "${snapshot}" > "${baked_file}.tmp"
printf '%s\n' "${by_tile}" > "${baked_by_tile_file}.tmp"
printf '%s\n' "${contract_digest}" > "${baked_contract_file}.tmp"
mv "${baked_by_tile_file}.tmp" "${baked_by_tile_file}"
mv "${baked_contract_file}.tmp" "${baked_contract_file}"
mv "${baked_file}.tmp" "${baked_file}"
rm -f "${work}/${name}.jsonl"
job_journal "${journal}" "baked Silver snapshot ${snapshot} (${scope} handoff, by-tile ${by_tile}) in $((SECONDS - step))s pending_layers=${pending} ${read_line}: ${result}"
echo "foundation-job-outcome changed"
