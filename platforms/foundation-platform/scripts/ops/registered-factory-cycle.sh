#!/usr/bin/env bash
# 한국산업단지공단 전국등록공장현황 (data.go.kr 15105482) — 수집과 Silver 적재 (root ADR-0280 §4). 등록된 작업
# registered_factory(spark 3칸, 가중치 1)가 하루 한 번 돌린다. 공급자는 해마다 한 번쯤 새 판을 올린다.
#
#   1. 수집 — collect-data-go-kr-registered-factory 가 포털의 메타데이터를 한 번 묻고, 마지막으로 받은 것과 다른
#             파일일 때만 zip 을 받아 Bronze 에 남긴다(data-go-kr-registered-factory.contract.json).
#   2. 계획 — plan-silver-load-objects 가 R2 만 보고 쌓을 판을 정한다: 이 원천의 Bronze zip 가운데
#             silver.registered_factory 의 커밋 요약이 아직 기록하지 않은 것, 계약의 범위(silver_load) 안에서
#             오래된 것부터 하나. 이 실행의 폴더로 받는다. 없으면 unchanged 로 끝난다(Spark 를 띄우지 않는다).
#   3. 적재 — data_go_kr_registered_factory.py load 가 멤버(이름·CP949·머리글·행 수 한계)를 검사하고, 주소로
#             PNU 를 찾아(지번 → 도로명, address_pnu.py) silver.registered_factory 에 판 전체를 한 번 쌓는다.
#             요약 줄(registered-factory-summary-json)에 단계별 건수와 일치율이 있다.
#
# 대기열은 없다. 적재가 실패하면 그 zip 은 표에 기록이 없어 다음 실행의 계획이 다시 받는다. 받은 사본은 성공하든
# 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

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
  echo "registered-factory: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_REGISTERED_FACTORY_STATE_ROOT:-/var/lib/foundation-platform/registered-factory}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_REGISTERED_FACTORY"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
plan_dir="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}/plan"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "registered-factory FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect" "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"' EXIT

# 1. 수집
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-data-go-kr-registered-factory >> "${run_log}" 2>&1

# 2. 계획: R2 의 Bronze 와 표의 커밋 기록만 본다.
read -r window_days max_objects slug < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"], "datagokr__" + c["dataset_slug"])
' "${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-registered-factory.contract.json")
env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE=silver.registered_factory FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${slug}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${plan_dir}" \
  "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${plan_dir}/plan.json")"
if [ "${owed}" = 0 ]; then
  job_journal "${journal}" "registered-factory unchanged run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
chmod -R a+rX "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}" # Spark 컨테이너는 uid 185 로 읽는다.

# 3. 적재
docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[4,8]' --driver-memory 8g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/data_go_kr_registered_factory.py load \
  --plan-dir "/workspace/target/lakehouse/runs/${run_id}/plan" --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^registered-factory-summary-json ' "${run_log}" | tail -1)"
# A zip no load recorded for longer than the window means loads have been failing: it is loaded now,
# and the run fails so the gap raises an alert (the plan names it `late`).
late="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8")).get("selection", {}).get("late", [])))' "${plan_dir}/plan.json")"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "registered-factory LATE run=${run_id}: loaded ${late} zip(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "registered-factory: ${late} edition(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen" >&2
  exit 1
fi
job_journal "${journal}" "registered-factory loaded run=${run_id} ${summary}"
printf 'foundation-job-outcome changed\n'
