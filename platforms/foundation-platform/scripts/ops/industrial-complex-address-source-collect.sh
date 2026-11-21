#!/usr/bin/env bash
# 산업입지정보시스템(ILIS) 산업단지 목록·고시 목록·상세를 Bronze 로 받는다 — 산업단지 주소 해석의 근거 (root ADR-0033).
# 등록된 작업 industrial_complex_address_source_collect(default_pool)가 작업 등록부의 일정에 돌린다. Spark 는 쓰지 않는다.
#
# collect-industrial-complex-address-source 가 목록(il/danji/list.do)과 고시 목록(il/danref/list.do)을 한 번씩 받고,
# Bronze 의 가장 새 산업단지 프로필 통합본에 있는데 목록에 없는 단지만 상세(il/danji/det/info.do)로 하나씩 묻는다.
# 요청 사이 2 초, 요청 수는 첫 요청 전에 상한과 대조한다. 받은 답이 지난번과 같으면 Bronze 커밋이 새 객체를 만들지 않는다.
#
# 적재로 넘기는 것은 없다. 새 객체가 Silver 적재(silver_industrial_complexes)를 깨운다.
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
  echo "industrial-complex address source collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_ADDRESS_SOURCE_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-address-source}"
COLLECT="FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_ADDRESS_SOURCE"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial-complex address source collect FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR

env "${COLLECT}_LIVE_WRITE=1" "${COLLECT}_SUMMARY_PATH=${work}/summary.json" \
  "${PUBLISHER_BIN}" collect-industrial-complex-address-source >> "${run_log}" 2>&1
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
job_journal "${journal}" "industrial-complex address source collect ok run=${run_id} ${outcome#foundation-job-outcome }"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
