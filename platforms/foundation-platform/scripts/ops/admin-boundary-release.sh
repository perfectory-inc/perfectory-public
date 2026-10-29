#!/usr/bin/env bash
# 읍면동 경계의 새 회차를 Silver·발행 ID·타일까지 올린다 (root ADR-0236). 예약 작업 admin_boundary_release.
#
#   1. 받기   — Bronze 의 경계 ZIP 전부(매일 수집이 받는다, 몇십 MB)를 읽기 키로 받는다(convert.sh download).
#   2. 회차   — ZIP 안 레이어 이름에서 가장 새 회차를 고른다(rounds.py). 없으면 할 일 없음.
#   3. 확인   — silver.administrative_boundaries 가 그 회차를 이미 가졌으면 끝(unchanged). Spark 한 번.
#   4. 변환   — 그 회차의 ZIP 만 GeoJSON 으로(convert.sh convert).
#   5. 병합   — merge.py. 공식 코드 목록의 시도 하나라도 파일이 없으면 기다린다(unchanged, 다음 날 다시).
#   6. 적재   — administrative_boundaries_handoff_to_silver.py: 회차를 덧붙이고, 코드가 바뀐 동은 옛 id 를 잇는다.
#   7. 발행 ID — place_id_release_to_gold.py --unit admin.
#   8. 타일   — map-edit-fold.sh admin 을 FORCE 로: 새 Silver 위에 편집을 다시 접어 굽는다.
#
# 모든 Spark 단계는 이 작업의 spark 풀 자리(3)에서 돈다 — 매시 접기(2 자리)와 겹치지 않는다. 그래서 compose `spark`
# (20g)를 쓴다: 병합·적재는 전국 경계를 드라이버의 파이썬으로 모으고, spark-small(2.5g)에서는 2026-10-15 OOM(137)이었다.
# 마지막 줄은 `foundation-job-outcome changed|unchanged` 다(root ADR-0171).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 접기 단위의 상태 폴더 안에 둔다: 8 단계가 그 단위를 그대로 부르고, 릴리스가 이미 만들어 둔 곳이다.
STATE_ROOT="${FOUNDATION_ADMIN_BOUNDARY_RELEASE_STATE_ROOT:-/var/lib/foundation-platform/map-edit-fold/admin-boundary-release}"
source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"
LAKEHOUSE_STATE_ROOT="${FOUNDATION_ADMIN_BOUNDARY_RELEASE_LAKEHOUSE_STATE_ROOT:-$(job_state_root admin_boundary_release)/lakehouse}"
SNAPSHOT_PREFIX="vworldkr__boundary_emd-30603-"
# 쓰기 키는 이 작업의 읽기 단계에 필요 없다: 받기는 읽기 키 한 쌍으로만 한다(root ADR-0152).
unset FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/admin-boundary-release/${run_id}"
container_work="/workspace/target/lakehouse/admin-boundary-release/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"

on_error() {
  job_journal "${journal}" "admin boundary release FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

spark() {
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[4,8]' --driver-memory 12g \
    --jars "${SPARK_RELEASE_JARS}" "/workspace/$1" "${@:2}" >> "${run_log}" 2>&1
}

# 받은 ZIP 과 변환본은 몇십 MB 씩이라 남기지 않는다; 요약과 로그만 증거로 남는다.
drop_inputs() { rm -rf "${work}/zips" "${work}/out" "${work}/official-administrative-boundary.geojson"; }

finish() {
  job_journal "${journal}" "admin boundary release $1 run=${run_id} $2"
  printf 'admin-boundary-release-outcome outcome=%s %s\n' "$1" "$2"
  printf 'foundation-job-outcome %s\n' "$1"
}

convert() { ADMIN_BOUNDARY_WORKDIR="${work}" "${RELEASE_ROOT}/scripts/tiles/admin-boundary/convert.sh" "$@" >> "${run_log}" 2>&1; }

# 1·2. 받고 가장 새 회차를 고른다.
convert download
picked="$(python3 -I "${RELEASE_ROOT}/scripts/tiles/admin-boundary/rounds.py" "${work}/zips")"
printf '%s\n' "${picked}" > "${work}/round.json"
round="$(python3 -I -c 'import json, sys; print(json.loads(sys.argv[1])["round"] or "")' "${picked}")"
if [[ -z "${round}" ]]; then
  drop_inputs
  finish unchanged "reason=no_round"
  exit 0
fi
snapshot="${SNAPSHOT_PREFIX}${round}"

# 3. 이미 가진 회차인가.
spark infra/lakehouse/spark/jobs/administrative_boundaries_handoff_to_silver.py --probe-only \
  --source-snapshot-id "${snapshot}"
if grep -q "^administrative-boundaries-silver-probe snapshot=${snapshot} held=true$" "${run_log}"; then
  drop_inputs
  finish unchanged "reason=already_loaded round=${round}"
  exit 0
fi
grep -q "^administrative-boundaries-silver-probe snapshot=${snapshot} held=false$" "${run_log}"

# 4. 그 회차의 ZIP 만 변환한다. Bronze 키가 계보다(source_record_id).
mapfile -t zips < <(python3 -I -c 'import json, sys; print("\n".join(json.loads(sys.argv[1])["files"].values()))' "${picked}")
convert convert "${zips[@]}"
records="$(printf 'bronze/source=vworldkr__boundary_emd/%s\n' "${zips[@]}" | paste -sd, -)"

# 5. 병합. 시도가 빠진 회차는 실패가 아니라 기다림이다(제공자가 시도 파일을 나눠 올린다).
merge_rc=0
spark scripts/tiles/admin-boundary/merge.py --round "${round}" --out-dir "${container_work}/out" || merge_rc=$?
if ((merge_rc != 0)); then
  if grep -q "^round ${round} has no file for sido" "${run_log}"; then
    drop_inputs
    finish unchanged "reason=incomplete_round round=${round}"
    exit 0
  fi
  on_error "merge"
  exit "${merge_rc}"
fi

# 6. Silver. 코드가 바뀐 동의 무게는 지도가 쓰는 필지 판으로 잰다.
parcel_snapshot="$(python3 -I "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/vworld_parcel_editions.py" snapshot-id \
  --edition "$(python3 -I "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/vworld_parcel_editions.py" served)")"
spark infra/lakehouse/spark/jobs/administrative_boundaries_handoff_to_silver.py \
  --input "${container_work}/official-administrative-boundary.geojson" \
  --source-snapshot-id "${snapshot}" --source-record-id "${records}" \
  --legal-dong-change-table reference.legal_dong_code_change --predecessor-parcel-snapshot-id "${parcel_snapshot}" \
  --summary-output "${container_work}/silver-summary.json" --allow-non-smoke-write

# 7. 발행 ID.
spark infra/lakehouse/spark/jobs/place_id_release_to_gold.py --unit admin --release-id "admin-${run_id}" \
  --summary-output "${container_work}/place-id-summary.json" --allow-non-smoke-write
drop_inputs

# 8. 타일: 편집이 없어도 새 Silver 로 굽는다.
FOUNDATION_MAP_EDIT_FOLD_FORCE=1 "${RELEASE_ROOT}/scripts/ops/map-edit-fold.sh" admin >> "${run_log}" 2>&1

rows="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["row_count"])' "${work}/silver-summary.json")"
finish changed "round=${round} snapshot=${snapshot} rows=${rows}"
