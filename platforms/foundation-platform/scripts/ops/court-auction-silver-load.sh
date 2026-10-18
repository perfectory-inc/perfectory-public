#!/usr/bin/env bash
# 법원경매정보의 Silver 적재 절반 (root ADR-0280, ADR-0281). 등록된 작업 court_auction_silver(spark 3칸, 가중치 1)가
# 매일 06:30 KST, 수집이 끝난 뒤에 돌린다.
#
# 수집 작업(court-auction-collect.sh)이 쓴 적재 대기 넘김마다, 오래된 것부터 courtauction_go_kr.py load 가 주소로
# PNU 를 찾아(address_pnu.py) silver.court_auction_item 에 내용이 바뀐 물건과(완전한 물건 검색일 때만) 빠진 물건의
# 삭제 표지를, silver.court_auction_sale_result 에 바뀐 행을 쌓고 넘김을 loaded/ 로 옮긴다. 대기 넘김이 없으면
# Spark 를 띄우지 않는다. 실패하면 넘김이 pending/ 에 남아 다음 실행이 다시 쌓는다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_COURT_AUCTION_STATE_ROOT:-/var/lib/foundation-platform/court-auction}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${LAKEHOUSE_STATE_ROOT}/pending" "${LAKEHOUSE_STATE_ROOT}/loaded" "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
chmod 0777 "${LAKEHOUSE_STATE_ROOT}/loaded" "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}/run.log"
trap 'job_journal "${journal}" "court-auction silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR

if ! compgen -G "${LAKEHOUSE_STATE_ROOT}/pending/*/manifest.json" >/dev/null; then
  job_journal "${journal}" "court-auction silver load: nothing pending run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[4,8]' --driver-memory 8g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/courtauction_go_kr.py load \
  --state-dir /workspace/target/lakehouse --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^court-auction-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "court-auction silver load ok run=${run_id} ${summary}"
printf 'foundation-job-outcome changed\n'
