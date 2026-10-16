#!/usr/bin/env bash
# 필지 ID 원장의 첫 등록을 시도마다 이어 간다 (root ADR-0243). 예약 작업 parcel_registry.
#
#   1. 확인 — 지도가 쓰는 필지 판의 시도 중 silver.parcel_registry 에 아무것도 없는 곳을 작은 것부터 묻는다
#             (parcel_registry_to_silver.py --probe-missing). 없으면 할 일 없음(unchanged).
#   2. 등록 — 그 시도마다 bootstrap 한 번(시도 하나 = Spark 한 번). 시도마다 걸린 시간을 journal 에 남긴다.
#             시작한 지 START_BUDGET 분이 지나면 새 시도를 시작하지 않는다: 유닛의 시간 상한 안에서 끝나고, 남은
#             시도는 다음 실행이 잇는다(이미 등록된 시도는 1 이 고르지 않는다).
#
# 판이 바뀐 뒤의 이어 붙이기(advance)는 계보 표(silver.parcel_lineage, lineage_stewardship 0b)를 읽는 별도 변경이고, 이 작업이 하지 않는다.
# 마지막 줄은 `foundation-job-outcome changed|unchanged` 다(root ADR-0171).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_PARCEL_REGISTRY_STATE_ROOT:-/var/lib/foundation-platform/lineage-stewardship/parcel-registry}"
LAKEHOUSE_STATE_ROOT="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-/var/lib/foundation-platform/lineage-stewardship/lakehouse}"
# 가장 큰 시도(약 600만 필지)도 이 뒤에 시작해서 유닛 상한(150 분) 안에 끝나야 한다.
START_BUDGET_MINUTES="${FOUNDATION_PARCEL_REGISTRY_START_BUDGET_MINUTES:-100}"
DRIVER_MEMORY="${FOUNDATION_PARCEL_REGISTRY_DRIVER_MEMORY:-16g}"

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/parcel-registry/${run_id}"
container_work="/workspace/target/lakehouse/parcel-registry/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"
started="${SECONDS}"

on_error() {
  job_journal "${journal}" "parcel registry FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

spark() {
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p foundation-platform-compute --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[4]' --driver-memory "${DRIVER_MEMORY}" \
    --jars "${SPARK_RELEASE_JARS}" "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >> "${run_log}" 2>&1
}

finish() {
  job_journal "${journal}" "parcel registry $1 run=${run_id} $2"
  printf 'parcel-registry-outcome outcome=%s %s\n' "$1" "$2"
  printf 'foundation-job-outcome %s\n' "$1"
}

editions="${RELEASE_ROOT}/infra/lakehouse/spark/jobs/vworld_parcel_editions.py"
edition="$(python3 -I "${editions}" served)"
to_date="$(python3 -I "${editions}" valid-from --edition "${edition}")"
to_date="${to_date%%T*}"

# 1. 등록되지 않은 시도.
spark parcel_registry_to_silver.py --probe-missing
missing="$(grep -a '^parcel-registry-probe ' "${run_log}" | tail -1 | cut -d' ' -f2-)"
[[ -n "${missing}" ]]
# One line per region and none when nothing is missing (a lone empty line would be a region named "").
mapfile -t regions < <(python3 -I -c 'import json, sys; sys.stdout.write("".join(f"{s} {n}\n" for s, n in json.loads(sys.argv[1])["missing"]))' "${missing}")
if ((${#regions[@]} == 0)); then
  finish unchanged "reason=all_registered edition=${edition}"
  exit 0
fi

# 2. 시도마다 bootstrap. 끝낸 시도와 남은 시도를 센다.
done_regions=()
for entry in "${regions[@]}"; do
  read -r sido parcels <<<"${entry}"
  if (((SECONDS - started) / 60 >= START_BUDGET_MINUTES)); then
    break
  fi
  region_started="${SECONDS}"
  spark parcel_registry_to_silver.py --to-sido "${sido}" --to-date "${to_date}" --bootstrap --allow-non-smoke-write \
    --summary-output "${container_work}/summary-${sido}.json"
  job_journal "${journal}" "parcel registry region=${sido} parcels=${parcels} seconds=$((SECONDS - region_started)) run=${run_id}"
  done_regions+=("${sido}")
done
remaining=$((${#regions[@]} - ${#done_regions[@]}))
if ((${#done_regions[@]} == 0)); then
  finish unchanged "reason=budget_spent edition=${edition} remaining=${remaining}"
  exit 0
fi
finish changed "edition=${edition} registered=$(IFS=,; echo "${done_regions[*]}") remaining=${remaining}"
