#!/usr/bin/env bash
# 한국산업단지공단 전국등록공장현황 (data.go.kr 15105482) — 수집과 Silver 적재 (root ADR-0280 §4). 등록된 작업
# registered_factory(spark 3칸, 가중치 1)가 하루 한 번 돌린다. 공급자는 해마다 한 번쯤 새 판을 올린다.
#
#   1. 수집 — collect-data-go-kr-registered-factory 가 포털의 메타데이터를 한 번 묻고, 마지막으로 받은 것과 다른
#             파일일 때만 zip 을 받아 Bronze 에 남긴다(data-go-kr-registered-factory.contract.json). 같으면
#             unchanged 로 여기서 끝난다(Spark 를 띄우지 않는다).
#   2. 넘김 — 새 판을 lakehouse/pending/<실행>/ 으로 옮긴다.
#   3. 적재 — data_go_kr_registered_factory.py load 가 멤버(이름·CP949·머리글·행 수 한계)를 검사하고, 주소로
#             PNU 를 찾아(지번 → 도로명, address_pnu.py) silver.registered_factory 에 판 전체를 한 번 쌓는다.
#             요약 줄(registered-factory-summary-json)에 단계별 건수와 일치율이 있다.
#
# 실패하면 넘김이 pending/ 에 남아 다음 실행이 적재만 다시 한다(이미 쌓은 객체는 append_batch_once 가 건너뛴다).
set -Eeuo pipefail
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
mkdir -p "${work}/collect" "${STATE_ROOT}/collector" "${LAKEHOUSE_STATE_ROOT}/pending" "${LAKEHOUSE_STATE_ROOT}/loaded"
chmod 0777 "${LAKEHOUSE_STATE_ROOT}" "${LAKEHOUSE_STATE_ROOT}/loaded" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"
trap 'job_journal "${journal}" "registered-factory FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 1. 수집
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-data-go-kr-registered-factory >> "${run_log}" 2>&1

# 2. 넘김: 새 판이 있을 때만.
if grep -q '"written": true' "${work}/collect/manifest.json"; then
  mv "${work}/collect" "${LAKEHOUSE_STATE_ROOT}/pending/${run_id}"
  chmod -R a+rwX "${LAKEHOUSE_STATE_ROOT}/pending/${run_id}"
fi
if ! compgen -G "${LAKEHOUSE_STATE_ROOT}/pending/*/manifest.json" >/dev/null; then
  job_journal "${journal}" "registered-factory unchanged run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

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
  --state-dir /workspace/target/lakehouse --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^registered-factory-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "registered-factory loaded run=${run_id} ${summary}"
printf 'foundation-job-outcome changed\n'
