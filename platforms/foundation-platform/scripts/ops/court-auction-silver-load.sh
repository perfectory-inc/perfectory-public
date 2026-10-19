#!/usr/bin/env bash
# 법원경매정보의 Silver 적재 절반 (root ADR-0280, ADR-0281). 등록된 작업 court_auction_silver(spark 3칸, 가중치 1)가
# 매일 06:30 KST, 수집이 끝난 뒤에 돌린다.
#
# 쌓을 것은 R2 만 보고 정한다. plan-silver-load-objects 가 검색마다 원천의 Bronze 접두어를 나열하고, 그 표의 커밋
# 요약이 이미 기록한 객체를 빼고, 남은 것을 오래된 것부터 계약의 범위(courtauction-go-kr.contract.json silver_load)
# 안에서 이 실행의 폴더로 받는다. courtauction_go_kr.py load 가 객체마다 커밋 하나로, 주소로 PNU 를 찾아
# (address_pnu.py) silver.court_auction_item 에 내용이 바뀐 물건과(물건 검색을 끝까지 읽은 밤에만) 빠진 물건의
# 삭제 표지를, silver.court_auction_sale_result 에 바뀐 행을 쌓는다. 쌓을 것이 없으면 Spark 를 띄우지 않는다.
#
# 대기열이 없으니 옮길 것도 없다. 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다.
# 받은 사본은 성공하든 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_COURT_AUCTION_STATE_ROOT:-/var/lib/foundation-platform/court-auction}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/courtauction-go-kr.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "court-auction silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/items" "${work}/results"' EXIT

read -r window_days max_objects item_slug result_slug < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"],
      "courtauctiongokr__" + c["item_search"]["dataset_slug"], "courtauctiongokr__" + c["sale_result_search"]["dataset_slug"])
' "${CONTRACT}")

plan() { # <table> <source slug> <dir>
  env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="$1" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="$2" \
    FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="$3" \
    "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
  python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "$3/plan.json"
}
owed_items="$(plan silver.court_auction_item "${item_slug}" "${work}/items")"
owed_results="$(plan silver.court_auction_sale_result "${result_slug}" "${work}/results")"
if [ "${owed_items}" = 0 ] && [ "${owed_results}" = 0 ]; then
  job_journal "${journal}" "court-auction silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
chmod -R a+rX "${work}" # Spark 컨테이너는 uid 185 로 읽는다.

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
  --plan-dir "/workspace/target/lakehouse/runs/${run_id}/items" \
  --plan-dir "/workspace/target/lakehouse/runs/${run_id}/results" \
  --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^court-auction-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "court-auction silver load ok run=${run_id} owed=${owed_items}+${owed_results} ${summary}"
printf 'foundation-job-outcome changed\n'
