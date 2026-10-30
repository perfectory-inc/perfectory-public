#!/usr/bin/env bash
# 국토교통부 실거래가의 Silver 적재 절반 (root ADR-0348, ADR-0349). 등록된 작업 real_transaction_silver(spark 1칸)가
# 매시간 수집 뒤에 돌린다. 작은 Spark(spark-small, 계약된 메모리 상한)로 돈다.
#
#   1. 시군구 목록 — 수집기가 묻는 시군구(lawd-set.json)가 없거나 계약의 lawd_set_refresh_hours 보다 낡았으면
#      reference.legal_dong_code_snapshot 의 가장 새 스냅숏에서 다시 쓴다(data_go_kr_rtms.py lawd-set).
#   2. 적재 — 쌓을 것은 R2 만 보고 정한다(plan-silver-load-objects: 12 원천의 Bronze 중 silver.real_transaction
#      의 커밋이 아직 기록하지 않은 것, 계약의 window_days·max_objects 안). data_go_kr_rtms.py load 가 객체마다
#      커밋 하나로, 객체의 (데이터셋, 시군구, 달) 묶음마다 새 이름·바뀐 행과 빠진 이름의 삭제 표지를 쌓는다. 계약해제는
#      같은 이름의 새 판이다. 법정동·PNU 는 정본 해석기로 정한다(root ADR-0288).
#
# 쌓을 것도, 목록을 새로 쓸 일도 없으면 Spark 를 띄우지 않고 unchanged 로 끝난다. 받은 사본은 성공하든 실패하든
# 지운다. 늦은 객체(window_days 보다 오래 기록 없이 남은 것)나 연결률이 계약의 하한 아래인 객체는 싣고 실패로
# 끝난다: 조용히 넘어가면 안 된다(알림이 간다).
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_REAL_TRANSACTION_STATE_ROOT:-/var/lib/foundation-platform/real-transaction}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-rtms.contract.json"
JOB="${RELEASE_ROOT}/infra/lakehouse/spark/jobs/data_go_kr_rtms.py"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "real-transaction silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/plans"' EXIT

read -r window_days max_objects refresh_hours < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))["silver_load"]
print(c["window_days"], c["max_objects"], c["lawd_set_refresh_hours"])
' "${CONTRACT}")
read -r table slugs < <(python3 -I "${JOB}" tables)

spark() { # <job arguments...>: one spark-small run of data_go_kr_rtms.py
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark-small spark-submit --master 'local[2,8]' --driver-memory 2g \
    --jars "${SPARK_RELEASE_JARS}" \
    /workspace/infra/lakehouse/spark/jobs/data_go_kr_rtms.py "$@" >> "${run_log}" 2>&1
}

# 1. 시군구 목록 (root ADR-0348 §2). The Spark container writes under the lakehouse state root it mounts.
lawd_set="${STATE_ROOT}/lawd-set.json"
if [ ! -f "${lawd_set}" ] || [ -n "$(find "${lawd_set}" -mmin "+$((refresh_hours * 60))" -print)" ]; then
  chmod a+rwx "${work}"
  spark lawd-set --output "/workspace/target/lakehouse/runs/${run_id}/lawd-set.json"
  install -m 0644 "${work}/lawd-set.json" "${lawd_set}"
  job_journal "${journal}" "real-transaction lawd set written run=${run_id} $(grep '^data-go-kr-rtms-lawd-set ' "${run_log}" | tail -1)"
fi

# 2. 적재
plan_dir="${work}/plans/real_transaction"
env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="${table}" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${slugs}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${plan_dir}" \
  "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${plan_dir}/plan.json")"
if [ "${owed}" = 0 ]; then
  job_journal "${journal}" "real-transaction silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
chmod -R a+rX "${work}" # Spark 컨테이너는 uid 185 로 읽는다.
spark load --plan-dir "/workspace/target/lakehouse/runs/${run_id}/plans/real_transaction" --allow-non-smoke-write

summary="$(grep '^data-go-kr-rtms-summary-json ' "${run_log}" | tail -1)"
late="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["selection"].get("late", [])))' "${plan_dir}/plan.json")"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "real-transaction silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "real-transaction silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen (run.log names them)" >&2
  exit 1
fi
job_journal "${journal}" "real-transaction silver load ok run=${run_id} owed=${owed} ${summary}"
printf 'foundation-job-outcome changed\n'
