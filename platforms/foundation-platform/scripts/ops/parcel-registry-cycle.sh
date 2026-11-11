#!/usr/bin/env bash
# 필지 ID 원장의 첫 등록을 시도마다 이어 가고, 그 ID 를 지역마다 발행한다 (root ADR-0243, ADR-0481). 예약 작업 parcel_registry.
#
#   1. 확인 — 지도가 쓰는 필지 판의 시도 중 silver.parcel_registry 에 아무것도 없는 곳을 작은 것부터 묻는다
#             (parcel_registry_to_silver.py --probe-missing). 없으면 할 일 없음(unchanged).
#   2. 등록 — 그 시도마다 bootstrap 한 번(시도 하나 = Spark 한 번). 시도마다 걸린 시간을 journal 에 남긴다.
#             시작한 지 START_BUDGET 분이 지나면 새 시도를 시작하지 않는다: 유닛의 시간 상한 안에서 끝나고, 남은
#             시도는 다음 실행이 잇는다(이미 등록된 시도는 1 이 고르지 않는다).
#   2b. 후보 — 판정(parcel-edition-readiness.sh)이 다음 서빙판으로 꼽은 후보 판(서빙판보다 새, source·silver 통과)의
#             등록부 한 걸음을 덧붙인다(root ADR-0490 §4a). 아직 그 판의 행이 없는 지역만, 지역마다 그곳에 등록된 가장
#             새 판에서 계보(silver.parcel_lineage)를 따라 advance 한다. 계보가 잇는 시도(29·46 → 12)는 한 지역이다.
#             두 판이 같이 가진 필지는 아무것도 쓰지 않는다. 후보의 행은 판 열(to_snapshot_id)로 갈리므로 서빙판을 읽는
#             쪽(place-id 발행, ADR-0288)은 승격 전까지 그대로이고, 후보의 대조 관문은 후보까지 접어 읽는다.
#   3. 발행 — 원장이나 법정동 코드 변경표가 지난 발행 뒤에 바뀐 지역만 묻고(place_id_release_to_gold.py --unit parcel
#             --probe-stale), 작은 것부터 그 지역 하나씩 gold.place_id_registry·bridge·changelog 로 낸다(unit=parcel).
#             같은 예산 안에서만 시작한다. 바뀐 것이 없으면 아무 지역도 읽지 않는다.
#
# 계보 표는 lineage_stewardship 이 짓는다. 후보 판의 계보가 아직 없으면 그 지역은 기다린다(2b 가 고르지 않는다).
# 마지막 줄은 `foundation-job-outcome changed|unchanged` 다(root ADR-0171).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"
# Both roots under the job's state_paths in jobs.v1.json (root ADR-0266); the overrides are the tests'.
STATE_ROOT="${FOUNDATION_PARCEL_REGISTRY_STATE_ROOT:-$(job_state_root parcel_registry)/parcel-registry}"
# Its own name, not FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT: map-edit-fold.env (which the unit loads for
# the catalog) sets that one to the fold's folder, which this unit may not write; the first supervised
# run failed on it (2026-10-16, "Read-only file system").
LAKEHOUSE_STATE_ROOT="${FOUNDATION_PARCEL_REGISTRY_LAKEHOUSE_ROOT:-$(job_state_root parcel_registry)/lakehouse}"
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
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[4,8]' --driver-memory "${DRIVER_MEMORY}" \
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

# 2b. 후보 판의 등록부 한 걸음(root ADR-0490 §4a). 후보가 없으면 묻지도 않는다.
READINESS="${FOUNDATION_PARCEL_EDITION_READINESS_PATH:-/var/lib/foundation-platform/parcel-editions/readiness.json}"
candidate="$(python3 -I "${editions}" candidate --report "${READINESS}")"
advanced=()
advance_regions=()
if [[ "${candidate}" != "-" ]] && (((SECONDS - started) / 60 < START_BUDGET_MINUTES)); then
  candidate_snapshot="$(python3 -I "${editions}" snapshot-id --edition "${candidate}")"
  candidate_date="$(python3 -I "${editions}" valid-from --edition "${candidate}")"
  candidate_date="${candidate_date%%T*}"
  spark parcel_registry_to_silver.py --to-snapshot-id "${candidate_snapshot}" --allow-non-served-edition --probe-advance
  waiting="$(grep -a '^parcel-registry-advance-probe ' "${run_log}" | tail -1 | cut -d' ' -f2-)"
  [[ -n "${waiting}" ]]
  mapfile -t advance_regions < <(python3 -I -c 'import json, sys; sys.stdout.write("".join(f"{t} {r} {s} {n}\n" for t, r, s, n in json.loads(sys.argv[1])["waiting"]))' "${waiting}")
  for entry in "${advance_regions[@]}"; do
    read -r to_sido from_sido from_snapshot parcels <<<"${entry}"
    if (((SECONDS - started) / 60 >= START_BUDGET_MINUTES)); then
      break
    fi
    region_started="${SECONDS}"
    spark parcel_registry_to_silver.py --to-snapshot-id "${candidate_snapshot}" --allow-non-served-edition       --from-snapshot-id "${from_snapshot}" --to-sido "${to_sido}" --from-sido "${from_sido}" --to-date "${candidate_date}"       --allow-non-smoke-write --summary-output "${container_work}/advance-${to_sido//,/-}.json"
    job_journal "${journal}" "parcel registry advance edition=${candidate} region=${to_sido} from=${from_snapshot}:${from_sido} parcels=${parcels} seconds=$((SECONDS - region_started)) run=${run_id}"
    advanced+=("${to_sido}")
  done
fi
unadvanced=$((${#advance_regions[@]} - ${#advanced[@]}))

# 3. 바뀐 지역의 place_id 발행. 등록이 예산을 다 썼으면 다음 실행이 잇는다.
released=()
stale_regions=()
probed=0
if (((SECONDS - started) / 60 < START_BUDGET_MINUTES)); then
  probed=1
  spark place_id_release_to_gold.py --unit parcel --probe-stale
  stale="$(grep -a '^place-id-parcel-probe ' "${run_log}" | tail -1 | cut -d' ' -f2-)"
  [[ -n "${stale}" ]]
  mapfile -t stale_regions < <(python3 -I -c 'import json, sys; sys.stdout.write("".join(f"{s} {n}\n" for s, n in json.loads(sys.argv[1])["stale"]))' "${stale}")
  for entry in "${stale_regions[@]}"; do
    read -r sido ids <<<"${entry}"
    if (((SECONDS - started) / 60 >= START_BUDGET_MINUTES)); then
      break
    fi
    region_started="${SECONDS}"
    spark place_id_release_to_gold.py --unit parcel --sido "${sido}" --release-id "parcel-${run_id}" --allow-non-smoke-write \
      --summary-output "${container_work}/place-id-${sido}.json"
    job_journal "${journal}" "parcel place-id release region=${sido} ids=${ids} seconds=$((SECONDS - region_started)) run=${run_id}"
    released+=("${sido}")
  done
fi
unreleased=$((${#stale_regions[@]} - ${#released[@]}))

detail="edition=${edition} registered=$(IFS=,; echo "${done_regions[*]}") remaining=${remaining} candidate=${candidate} advanced=$(IFS=/; echo "${advanced[*]}") unadvanced=${unadvanced} released=$(IFS=,; echo "${released[*]}") unreleased=${unreleased}"
if ((${#done_regions[@]} == 0 && ${#advanced[@]} == 0 && ${#released[@]} == 0)); then
  if ((remaining > 0 || unadvanced > 0 || unreleased > 0 || probed == 0)); then
    finish unchanged "reason=budget_spent ${detail}"
  else
    finish unchanged "reason=all_registered_and_released ${detail}"
  fi
  exit 0
fi
finish changed "${detail}"
