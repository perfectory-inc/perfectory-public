#!/usr/bin/env bash
# 산업입지정보시스템(ILIS) 산업단지 고시와 그 첨부를 받는다 — 수집 절반 (root ADR-0484). 등록된 작업
# industrial_complex_notice_collect(default_pool)가 작업 등록부의 일정에 돌린다. Spark 는 쓰지 않는다.
#
# collect-industrial-complex-notice-attachments 가 고시 목록을 한 번에 받고, Silver 읽은 기록이 끝났다고 하지 않는
# 고시를 새 것부터 계약(industryland-or-kr.contract.json notice_detail)의 상한·시간 예산 안에서 묻는다. 첨부는 형식에
# 상관없이 공용 미디어 저장소에 내용 해시로 한 번만 담고, 상세는 commit_every 건마다 Bronze 객체 하나로 커밋한다.
# 상세를 하나도 못 읽은 실행만 실패로 끝난다.
#
# 적재로 넘기는 것은 없다. 적재(industrial_complex_notice_silver)는 R2 의 Bronze 와 Silver 표의 커밋 기록만 보고
# 쌓을 객체를 정한다(plan-silver-load-objects).
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
  echo "industrial-complex notice collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_NOTICE_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-notice}"
COLLECT="FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_NOTICE"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial-complex notice collect FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect"' EXIT

if ! env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-industrial-complex-notice-attachments >> "${run_log}" 2>&1; then
  # What any run committed is in Bronze; the load finds it there.
  job_journal "${journal}" "industrial-complex notice collect STOPPED run=${run_id}: $(tail -1 "${run_log}")" >&2
  job_run_log_tail "${run_log}"
  exit 1
fi
summary="$(grep '^industrial-complex-notice-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "industrial-complex notice collect ok run=${run_id} ${summary#industrial-complex-notice-summary-json }"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
