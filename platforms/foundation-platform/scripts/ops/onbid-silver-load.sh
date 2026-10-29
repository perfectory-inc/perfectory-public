#!/usr/bin/env bash
# 차세대 온비드 공매의 Silver 적재 절반 (root ADR-0280). 등록된 작업 onbid_public_auction_silver(spark 1칸)가
# 매시간 수집 뒤에 돌린다. 한 번에 수만 행이라 작은 Spark(spark-small, 계약된 메모리 상한)로 돈다.
#
# 쌓을 것은 R2 만 보고 정한다. plan-silver-load-objects 가 표마다 원천의 Bronze 접두어를 나열하고, 그 표의
# 커밋 요약(foundation.ingest-batch-objects)이 이미 기록한 객체를 빼고, 남은 것을 오래된 것부터 계약의
# 범위(data-go-kr-onbid.contract.json silver_load: 최근 window_days 안, 표마다 max_objects 개) 안에서 이
# 실행의 폴더로 받는다. data_go_kr_onbid.py load 가 객체마다 커밋 하나로 silver.onbid_real_estate_item 에
# 내용이 바뀐 회차와(전수가 완전할 때만) 빠진 회차의 삭제 표지를, silver.onbid_item_bid_result 에 입찰결과
# 답을, silver.onbid_real_estate_item_photo 에 물건별 사진·감정평가서 해시 색인(root ADR-0290 §7)을
# 쌓는다(root ADR-0275). 쌓을 것이 없으면 Spark 를 띄우지 않고 unchanged 로 끝난다.
#
# 대기열이 없으니 옮길 것도 없다. 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다.
# 받은 사본은 성공하든 실패하든 지운다. 계획이 늦은 객체(window_days 보다 오래 기록 없이 남은 것)를 내놓으면
# 쌓은 뒤 실패로 끝난다: 적재가 그만큼 실패해 왔다는 뜻이니 조용히 넘어가면 안 된다(알림이 간다).
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_ONBID_STATE_ROOT:-/var/lib/foundation-platform/onbid}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-onbid.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "onbid silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/plans"' EXIT

read -r window_days max_objects < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"])
' "${CONTRACT}")
# Each table and the source it loads (root ADR-0340 §6): the item detail feeds both the media index
# and the detail table, each recording its own loads.
mapfile -t loads < <(python3 -I "${RELEASE_ROOT}/infra/lakehouse/spark/jobs/data_go_kr_onbid.py" tables)

plan() { # <table> <source slug> <dir>
  env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="$1" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="$2" \
    FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="$3" \
    "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
  python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "$3/plan.json"
}
owed_total=0
plan_args=()
for load in "${loads[@]}"; do
  read -r table slug <<<"${load}"
  dir="${work}/plans/${table#silver.}"
  owed="$(plan "${table}" "${slug}" "${dir}")"
  owed_total=$((owed_total + owed))
  plan_args+=(--plan-dir "/workspace/target/lakehouse/runs/${run_id}/plans/${table#silver.}")
done
if [ "${owed_total}" = 0 ]; then
  job_journal "${journal}" "onbid silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
chmod -R a+rX "${work}" # Spark 컨테이너는 uid 185 로 읽는다.

FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run foundation-platform-compute \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[2,8]' --driver-memory 2g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/data_go_kr_onbid.py load \
  "${plan_args[@]}" \
  --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^data-go-kr-onbid-summary-json ' "${run_log}" | tail -1)"
late="$(python3 -I -c '
import json, sys
print(sum(len(json.load(open(p, encoding="utf-8"))["selection"].get("late", [])) for p in sys.argv[1:]))
' "${work}"/plans/*/plan.json)"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "onbid silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "onbid silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen (run.log names them)" >&2
  exit 1
fi
job_journal "${journal}" "onbid silver load ok run=${run_id} owed=${owed_total} ${summary}"
printf 'foundation-job-outcome changed\n'
