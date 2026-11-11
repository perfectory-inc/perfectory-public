#!/usr/bin/env bash
# 산업단지 고시의 Silver 적재 절반 (root ADR-0484). 등록된 작업 industrial_complex_notice_silver(spark 1칸, 가중치 10)가
# 수집 실행이 Bronze 를 바꿨다고 알리면, 그리고 일정마다 돌린다.
#
# 쌓을 것은 R2 만 보고 정한다. plan-silver-load-objects 가 고시 상세 원천의 Bronze 접두어를 나열하고, 읽은 기록
# 표(silver.industrial_complex_notice_detail)의 커밋 요약이 이미 기록한 객체를 빼고, 남은 것을 오래된 것부터 계약의
# 범위(industryland-or-kr.contract.json silver_load) 안에서 이 실행의 폴더로 받는다. industryland_or_kr.py load 가
# 객체마다 커밋 하나로, 먼저 저장소에 담긴 첨부를 silver.industrial_complex_notice_attachment 에, 그다음 읽은 기록을
# silver.industrial_complex_notice_detail 에 쌓는다. 쌓을 것이 없으면 Spark 를 띄우지 않는다.
#
# 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다. 받은 사본은 성공하든 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_NOTICE_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-notice}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/industryland-or-kr.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial-complex notice silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/details"' EXIT

read -r window_days max_objects detail_slug < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"],
      "industrylandorkr__" + c["notice_detail"]["dataset_slug"])
' "${CONTRACT}")

env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE=silver.industrial_complex_notice_detail \
  FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${detail_slug}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${work}/details" \
  "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${work}/details/plan.json")"
if [ "${owed}" = 0 ]; then
  job_journal "${journal}" "industrial-complex notice silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
chmod -R a+rX "${work}" # Spark 컨테이너는 uid 185 로 읽는다.

FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run foundation-platform-compute \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[2,8]' --driver-memory 1g \
  --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/industryland_or_kr.py load \
  --plan-dir "/workspace/target/lakehouse/runs/${run_id}/details" \
  --allow-non-smoke-write >> "${run_log}" 2>&1

summary="$(grep '^industrial-complex-notice-summary-json ' "${run_log}" | tail -1)"
# An object no load recorded for longer than the window means loads have been failing: it is loaded
# now, and the run fails so the gap raises an alert (the plan names it `late`).
late="$(python3 -I -c '
import json, sys
print(len(json.load(open(sys.argv[1], encoding="utf-8")).get("selection", {}).get("late", [])))
' "${work}/details/plan.json")"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "industrial-complex notice silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "industrial-complex notice silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen" >&2
  exit 1
fi
job_journal "${journal}" "industrial-complex notice silver load ok run=${run_id} owed=${owed} ${summary}"
printf 'foundation-job-outcome changed\n'
