#!/usr/bin/env bash
# 중개사 가입 확인 명부 적재 (root ADR-0550 §2·§3). 두 중개업 Silver 레인이 바꾸면, 그리고 매일 한 번
# 대체 실행으로 돈다: 지난 적재 뒤 덧붙은 사무소만 읽어 Postgres 명부의 그 사무소 행을 물리고 새 현재 행을
# 넣는다. 바뀐 것이 없으면 쓰지 않고 `unchanged` 다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_REAL_ESTATE_BROKER_LOOKUP_STATE_ROOT:-/var/lib/foundation-platform/real-estate-broker-lookup}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run_log="${STATE_ROOT}/runs/${run_id}.log"
mkdir -p "${STATE_ROOT}/runs"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — lineage-stewardship-cycle.sh 와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

on_error() {
  job_journal "${journal}" "broker-lookup FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

"${PUBLISHER_BIN}" load-real-estate-broker-lookup >> "${run_log}" 2>&1
result="$(grep -E '^real-estate-broker-lookup-load ' "${run_log}" | tail -n 1)"
job_journal "${journal}" "${result} run=${run_id}"
case "${result}" in
  # The job's last line, the one every scheduled job ends with (root ADR-0171).
  'real-estate-broker-lookup-load changed '*) echo 'foundation-job-outcome changed' ;;
  'real-estate-broker-lookup-load unchanged '*) echo 'foundation-job-outcome unchanged' ;;
  *) echo "broker-lookup: the load printed no outcome" >&2; exit 1 ;;
esac
