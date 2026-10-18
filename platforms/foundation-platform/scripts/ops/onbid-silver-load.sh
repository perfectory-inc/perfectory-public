#!/usr/bin/env bash
# 차세대 온비드 공매의 Silver 적재 절반 (root ADR-0280). 등록된 작업 onbid_public_sale_silver(spark 1칸)가
# 매시간 수집 뒤에 돌린다. 한 번에 수만 행이라 작은 Spark(spark-small, 계약된 메모리 상한)로 돈다.
#
# 수집 작업(onbid-collect.sh)이 쓴 적재 대기 넘김(lakehouse/pending/<실행>/)마다, 오래된 것부터
# data_go_kr_onbid.py load 가 silver.onbid_real_estate_item 에 내용이 바뀐 회차와(전수가 완전할 때만) 빠진
# 회차의 삭제 표지를, silver.onbid_item_bid_result 에 입찰결과 답을 쌓고 넘김을 loaded/ 로 옮긴다
# (root ADR-0275). 대기 넘김이 없으면 Spark 를 띄우지 않고 unchanged 로 끝난다. 실패하면 넘김이 pending/ 에
# 남아 다음 실행이 다시 쌓는다: 이미 쌓은 객체는 append_batch_once 가 건너뛴다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_ONBID_STATE_ROOT:-/var/lib/foundation-platform/onbid}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${LAKEHOUSE_STATE_ROOT}/pending" "${LAKEHOUSE_STATE_ROOT}/loaded" "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
chmod 0777 "${LAKEHOUSE_STATE_ROOT}/loaded" "${LAKEHOUSE_STATE_ROOT}/runs/${run_id}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}/run.log"
trap 'job_journal "${journal}" "onbid silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR

if ! compgen -G "${LAKEHOUSE_STATE_ROOT}/pending/*/manifest.json" >/dev/null; then
  job_journal "${journal}" "onbid silver load: nothing pending run=${run_id}"
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
  spark-small spark-submit --master 'local[2]' --driver-memory 2g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/data_go_kr_onbid.py load \
  --state-dir /workspace/target/lakehouse --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^data-go-kr-onbid-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "onbid silver load ok run=${run_id} ${summary}"
printf 'foundation-job-outcome changed\n'
