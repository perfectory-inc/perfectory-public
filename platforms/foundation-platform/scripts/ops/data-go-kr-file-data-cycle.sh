#!/usr/bin/env bash
# data.go.kr 파일 데이터 — 기타 공공 일괄 파일 수집과 Silver 적재 (root ADR-0401). 등록된 작업
# data_go_kr_file_data(spark 3칸, 가중치 1)가 하루 한 번 돌린다.
#
#   1. 수집 — collect-data-go-kr-file-data 가 계약(data-go-kr-file-data.contract.json)의 파일 가운데 등록공장을 뺀
#             전부를 맡는다(등록공장은 작업 registered_factory). 파일마다 포털의 메타데이터를 한 번 묻고, 마지막으로
#             받은 것과 다른 파일(atchFileId)일 때만 받아 그 원천의 Bronze(datagokr__<dataset_slug>)에 남긴다. 파일마다
#             따로 커밋하므로 한 파일이 실패해도 다른 파일은 받고, run 은 끝에 실패한다.
#   2. 계획 — plan-silver-load-objects 가 표마다 R2 만 보고 쌓을 판을 정한다: 그 원천의 Bronze 파일 가운데 표의 커밋
#             요약이 아직 기록하지 않은 것, 계약의 범위(silver_load) 안에서 오래된 것부터 하나.
#   3. 적재 — data_go_kr_file_data.py load 가 판마다 머리줄·인코딩·행 수 한계를 검사하고, 행 키로 표의 현재 행과
#             비교해 새 키·바뀐 키와 사라진 키의 삭제 표지만 쌓는다(ADR-0318). 쌓을 판이 없어도 돈다: 법정동 코드
#             변경표가 표의 마지막 연결 뒤에 바뀌었으면 바뀐 코드 아래의 행만 다시 잇는다(ADR-0329 §3).
#
# run 당 상한은 파일 자체다(2026-11-04 실측): 8개가 모두 새 판이면 약 365 MB(상가(상권) zip 349 MB 가 59초), 간격을 넣어
# 3분 안팎. 적재는 표마다 한 판. 받은 사본은 성공하든 실패하든 지운다. 실패하면 0 이 아닌 값으로 끝나고 Airflow 실패
# 알림이 간다(ADR-0122). 적재가 실패한 판은 표에 기록이 없어 다음 실행의 계획이 다시 받는다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152).
required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "data.go.kr file data: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_DATA_GO_KR_FILE_DATA_STATE_ROOT:-/var/lib/foundation-platform/data-go-kr-file-data}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-file-data.contract.json"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
lake="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector" "${lake}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "data.go.kr file data FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect" "${lake}"' EXIT

# 1. 수집 — 한 파일의 실패가 다른 파일의 적재를 막지 않는다: 수집기가 실패해도 받은 파일의 manifest 는 남으니
#    계획·적재까지 하고, 끝에서 실패로 끝난다.
datasets="$(python3 -I -c '
import json, sys
files = json.load(open(sys.argv[1], encoding="utf-8"))["files"]
print(",".join(slug for slug in files if slug != "registered_factory"))
' "${CONTRACT}")"
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${COLLECT}_DATASETS=${datasets}" "${PUBLISHER_BIN}" collect-data-go-kr-file-data >> "${run_log}" 2>&1 || collect_status=$?
collect_status="${collect_status:-0}"
[ -f "${work}/collect/manifest.json" ] || exit "${collect_status}" # 설정 거부: 아무것도 받지 않았다
landed="$(python3 -I -c 'import json, sys; print(" ".join(f["dataset_slug"] for f in json.load(open(sys.argv[1], encoding="utf-8"))["files"]))' "${work}/collect/manifest.json")"

# 2. 계획: 표마다, R2 의 Bronze 와 표의 커밋 기록만 본다.
mapfile -t loads < <(python3 -I "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/data_go_kr_file_data.py" tables)
owed_total=0
plan_args=()
for load in "${loads[@]}"; do
  read -r table slug <<<"${load}"
  name="${table#silver.}"
  read -r window_days max_objects < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))["files"][sys.argv[2]]["silver_load"]
print(c["window_days"], c["max_objects"])
' "${CONTRACT}" "${name}")
  env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="${table}" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${slug}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${lake}/plans/${name}" \
    "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
  owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${lake}/plans/${name}/plan.json")"
  owed_total=$((owed_total + owed))
  plan_args+=(--plan-dir "/workspace/target/lakehouse/runs/${run_id}/plans/${name}")
done
chmod -R a+rwX "${lake}" # Spark 컨테이너는 uid 185 로 읽고, 판의 UTF-8 사본을 그 옆에 쓴다.

# 3. 적재와 코드 변경 따라가기
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run foundation-platform-compute \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[4,8]' --driver-memory 8g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/data_go_kr_file_data.py load \
  "${plan_args[@]}" --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^data-go-kr-file-data-summary-json ' "${run_log}" | tail -1)"
appended="$(python3 -I -c '
import json, sys
s = json.loads(sys.argv[1].split(" ", 1)[1])
print(sum(e["appended"] for e in s["editions"]) + sum(f["appended"] for f in s["follows"]))
' "${summary}")"
# A file no load recorded for longer than the window means loads have been failing: it is loaded now,
# and the run fails so the gap raises an alert (the plan names it `late`).
late="$(python3 -I -c '
import json, sys
print(sum(len(json.load(open(p, encoding="utf-8")).get("selection", {}).get("late", [])) for p in sys.argv[1:]))
' "${lake}"/plans/*/plan.json)"
if [ "${collect_status}" != 0 ]; then
  failed="$(python3 -I -c 'import json, sys; print("; ".join(json.load(open(sys.argv[1], encoding="utf-8"))["failed"]))' "${work}/collect/manifest.json")"
  job_journal "${journal}" "data.go.kr file data COLLECT FAILED run=${run_id}: ${failed}; the files that landed were loaded ${summary}" >&2
  echo "data.go.kr file data: collection failed for ${failed}" >&2
  exit 1
fi
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "data.go.kr file data LATE run=${run_id}: loaded ${late} file(s) older than the window that no load had recorded ${summary}" >&2
  echo "data.go.kr file data: ${late} edition(s) waited past the window unloaded; they are loaded now, and this run fails so the gap is seen" >&2
  exit 1
fi
job_journal "${journal}" "data.go.kr file data ok run=${run_id} landed=[${landed}] owed=${owed_total} appended=${appended} ${summary}"
if [ -n "${landed}" ] || [ "${owed_total}" != 0 ] || [ "${appended}" != 0 ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
