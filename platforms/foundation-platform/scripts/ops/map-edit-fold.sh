#!/usr/bin/env bash
# 관리자 폴리곤 편집을 타일에 접는다 (root ADR-0112 §7·§9).
#
# systemd 타이머가 매시간 이 스크립트를 돌린다. 편집 저장소에 접히지 않은 편집이 없으면
# 굽지 않는다 — 같은 원천을 매시간 다시 구우면 타일 주소만 바뀌어 캐시가 버려진다.
# 편집이 있으면: 편집 내보내기 → 원장·Gold 서빙본(Spark) → 굽기·승격·접기.
# `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 은 편집이 없어도 굽는다(새 Silver 원천을 반영할 때).
#
# 감시: 오버레이가 응답하지 않거나, 접히지 않은 편집이 너무 오래 남았거나, 어느 단계든
# 실패하면 슬랙 #alerts 가 안다. 아무 일도 없던 시간에도 journal 에 한 줄을 남긴다 —
# "할 일 없음"과 "확인 안 함"은 구별되어야 한다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 유닛은 첫 인자다 — systemd 템플릿 `foundation-map-edit-fold@<unit>.service` 가 `%i` 로 넘긴다.
UNIT="${1:-${FOUNDATION_MAP_EDIT_FOLD_UNIT:-complex}}"
STATE_ROOT="${FOUNDATION_MAP_EDIT_FOLD_STATE_ROOT:-/var/lib/foundation-platform/map-edit-fold}"
# 전용 폴더다. 공용 /var/lib/foundation-platform/lakehouse 는 Spark(uid 185) 소유라 서비스 계정이 쓸 수 없고,
# 그 소유를 바꾸면 다른 적재 작업이 깨진다(2026-09-29 첫 설치에서 실제로 그랬다).
source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"
LAKEHOUSE_STATE_ROOT="${FOUNDATION_MAP_EDIT_FOLD_LAKEHOUSE_STATE_ROOT:-$(job_state_root "map_edit_fold_${UNIT}")/lakehouse}"

# 유닛마다 Silver 좌표계와 서빙본을 만드는 Spark 작업이 다르다. 좌표계의 정본은 각 서빙본
# 계약의 geometry_srid 관문이다 — 여기 값이 어긋나면 Spark 작업이 편집 내보내기를 거부한다.
case "${UNIT}" in
  complex)
    HANDOFF_SRID=5186
    SERVED_JOB=industrial_complex_boundary_served_gold.py
    ;;
  admin)
    HANDOFF_SRID=4326
    SERVED_JOB=administrative_boundary_served_gold.py
    ;;
  parcels)
    # 필지는 전국 4천만 행이라 굽기 유닛의 상태·잠금·작업 디스크에서 접는다(parcel-tile-bake.sh fold).
    echo "map-edit-fold: parcels 는 foundation-parcel-tile-bake@fold 가 접는다" >&2
    exit 64
    ;;
  *)
    echo "map-edit-fold: 모르는 유닛 ${UNIT}" >&2
    exit 64
    ;;
esac

: "${FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL:?map-edit.env must provide the edit store URL}"
: "${FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN:?map-edit.env must provide the writer token}"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — daily-source-sweep.sh 와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_log="${STATE_ROOT}/last-run.log"
: > "${run_log}"

on_error() {
  local line="$1"
  job_journal "${journal}" "fold unit=${UNIT} FAILED at line ${line}" >&2
  tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

# 1·2. 오버레이가 살아 있는가, 접히지 않은 편집이 몇 건이고 얼마나 오래됐는가(알림 포함).
source "$(dirname "${BASH_SOURCE[0]}")/map-edit-pending.sh"
map_edit_pending "${UNIT}"
if [ "${pending}" = "0" ] && [ "${FOUNDATION_MAP_EDIT_FOLD_FORCE:-0}" != "1" ]; then
  job_journal "${journal}" "fold unit=${UNIT} pending=0 overlay=${overlay_status} skipped"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

# 3. 한 번의 접기 = 한 작업 폴더. 모든 산출물은 증거로 남고 덮어쓰지 않는다.
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/map-edit-fold/${UNIT}/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.

FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_UNIT="${UNIT}" \
FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_SRID="${HANDOFF_SRID}" \
FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_OUTPUT="${work}/edits.jsonl" \
  "${PUBLISHER_BIN}" export-map-edit-handoff >> "${run_log}" 2>&1

container_work="/workspace/target/lakehouse/map-edit-fold/${UNIT}/${run_id}"
# 한 번 쓰고 끝나는 초기화 컨테이너가 이름을 붙잡고 있으면 compose run 이 이름 충돌로 죽는다.
lakehouse_run_log "${run_log}"
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run foundation-platform-compute \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[4,8]' --driver-memory 4g \
  --jars "${SPARK_RELEASE_JARS}" \
  "/workspace/infra/lakehouse/spark/jobs/${SERVED_JOB}" \
  --edits-input "${container_work}/edits.jsonl" \
  --output "${container_work}/served.jsonl" \
  --summary-output "${container_work}/served-summary.json" \
  --allow-non-smoke-write >> "${run_log}" 2>&1

# Root ADR-0621 §5: edits this build left out because the source changed under them.
newly_stale="$(python3 -c 'import json,sys; print(int(json.load(open(sys.argv[1])).get("edits_newly_stale", 0)))' \
  "${work}/served-summary.json")"
if [ "${newly_stale}" -gt 0 ]; then
  notify_slack "🟠 지도 편집 ${newly_stale}건이 원천이 바뀌어 ${UNIT} 타일에 적용되지 않는다 — 손님은 공식 폴리곤을 본다. 목록 gold.map_edit_stale, 다시 그려야 한다"
fi

FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT="${UNIT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/served.jsonl" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/served-summary.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work}/bake" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="map-edit-fold-${UNIT}-${run_id}-build" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="map-edit-fold-${UNIT}-${run_id}-promote" \
  "${PUBLISHER_BIN}" bake-lakehouse-tiles >> "${run_log}" 2>&1

result="$(grep -o 'lakehouse-tile-bake-ok.*' "${run_log}" | tail -1)"
job_journal "${journal}" "fold unit=${UNIT} pending=${pending} ${result}"
notify_slack "🗺️ 지도 편집 ${pending}건을 ${UNIT} 타일에 접었다 — ${result}"
# The run's last line (root ADR-0171, ADR-0260): the edits are folded into the served tiles.
printf 'foundation-job-outcome changed\n'
