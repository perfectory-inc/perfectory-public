#!/usr/bin/env bash
# 산업용지 분양공고의 Silver 적재 절반 (root ADR-0603). 등록된 작업 industrial_land_sale_notice_silver 가 수집 실행이
# Bronze 를 바꿨다고 알리면, 그리고 일정마다 돌린다. Spark 는 쓰지 않는다.
#
# 쌓을 것은 R2 만 보고 정한다. plan-silver-load-objects 가 네 원천(공장설립온라인·ILIS·한국산업단지공단·LH)의 상세 Bronze 접두어를
# 나열하고, 읽은 기록 표(silver.industrial_land_sale_notice_fetch_attempt)의 커밋 요약이 이미 기록한 객체를 빼고, 남은 것을
# 오래된 것부터 계약(industrial-land-sale-notice.contract.json silver_load)의 범위 안에서 이 실행의 폴더로 받는다.
# 발행기의 load-industrial-land-sale-notice-silver 가 객체마다(발행기 안의 DuckDB 가 행을 만들고 발행기가 Iceberg 추가
# 커밋으로 붙인다, root ADR-0559·ADR-0560) 먼저 저장소에 담긴 첨부 가운데 새것·바뀐 것을
# silver.industrial_land_sale_notice_attachment(미디어 대조기가 읽는 색인)에, 그다음 읽은 기록을
# silver.industrial_land_sale_notice_fetch_attempt 에 쌓는다. 행이 없는 객체도 기록한다. 쌓을 것이 없으면 아무것도 띄우지 않는다.
#
# 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다. 받은 사본은 성공하든 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# The plan reads the sources' Bronze objects from the ledger (root ADR-0626 §4, amending root ADR-0280):
# recovery.env holds the password, not the address, so it is built as the collectors build it.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_LAND_SALE_NOTICE_STATE_ROOT:-/var/lib/foundation-platform/industrial-land-sale-notice}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/industrial-land-sale-notice.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial land sale notice silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/details"' EXIT

# The detail sources: <provider without dots>__<detail_dataset_slug> of every source, as the load
# itself derives them (industrial_land_sale_notice_stage::detail_source_slugs).
settings="$(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
slugs = [s["provider"].replace(".", "") + "__" + s["detail_dataset_slug"] for s in c["sources"].values()]
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"], ",".join(slugs))
' "${CONTRACT}")"
read -r window_days max_objects detail_slugs <<< "${settings}"

env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE=silver.industrial_land_sale_notice_fetch_attempt \
  FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${detail_slugs}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${work}/details" \
  "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${work}/details/plan.json")"
if [ "${owed}" = 0 ]; then
  job_journal "${journal}" "industrial land sale notice silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
lakehouse_run_log "${run_log}"
FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE_PLAN_DIR="${work}/details" \
  "${PUBLISHER_BIN}" load-industrial-land-sale-notice-silver >> "${run_log}" 2>&1

summary="$(grep '^industrial-land-sale-notice-summary-json ' "${run_log}" | tail -1)"
# An object no load recorded for longer than the window means loads have been failing: it is loaded
# now, and the run fails so the gap raises an alert (the plan names it `late`).
late="$(python3 -I -c '
import json, sys
print(len(json.load(open(sys.argv[1], encoding="utf-8")).get("selection", {}).get("late", [])))
' "${work}/details/plan.json")"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "industrial land sale notice silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "industrial land sale notice silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen" >&2
  exit 1
fi
job_journal "${journal}" "industrial land sale notice silver load ok run=${run_id} owed=${owed} ${summary}"
printf 'foundation-job-outcome changed\n'
