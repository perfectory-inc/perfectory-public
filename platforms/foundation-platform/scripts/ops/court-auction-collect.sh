#!/usr/bin/env bash
# 법원경매정보 목록을 courtauction.go.kr 에서 받는다 — 수집 절반 (root ADR-0280, ADR-0281). 등록된 작업
# court_auction_collect(default_pool)가 매일 01:05 KST 에 돌린다. Spark 는 쓰지 않는다.
#
# collect-courtauction-go-kr 이 계약(courtauction-go-kr.contract.json)의 예의를 지킨다: 신원을 밝힌 User-Agent
# (연락처 FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT), 요청 사이 최소 3초, 연결 하나, 새벽(01–06 KST)에만, 실행당 요청
# 상한, robots.txt 먼저. 403·429·503·Retry-After·HTML 안내 화면·ipcheck=false 중 하나라도 보면 그 자리에서 멈추고,
# 받은 것까지 Bronze 에 남기고, collector/refused.json 을 쓰고 실패로 끝난다. 그 파일이 있는 동안 다음 실행은 시작하지
# 않는다: 사람이 원인을 확인하고 지운다(런북 court-auction.md). 우회하는 코드는 없다.
#
# 받은 객체가 있으면 실행 폴더를 적재 대기 넘김(lakehouse/pending/<실행>/)으로 옮긴다. 적재는 court_auction_silver
# (court-auction-silver-load.sh)가 한다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

required_env=(
  FOUNDATION_PLATFORM_COURT_AUCTION_CONTACT
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
  echo "court-auction collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_COURT_AUCTION_STATE_ROOT:-/var/lib/foundation-platform/court-auction}"
COLLECT="FOUNDATION_PLATFORM_COURT_AUCTION"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector" "${STATE_ROOT}/lakehouse/pending"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "court-auction collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

stage() {
  if [ -f "${work}/collect/manifest.json" ] && grep -q '"written": true' "${work}/collect/manifest.json"; then
    mv "${work}/collect" "${STATE_ROOT}/lakehouse/pending/${run_id}"
    chmod -R a+rwX "${STATE_ROOT}/lakehouse/pending/${run_id}" # Spark 컨테이너는 uid 185 로 옮긴다.
    return 0
  fi
  return 1
}

if ! env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-courtauction-go-kr >> "${run_log}" 2>&1; then
  # A refusal still landed what was collected before it; hand that to the load, then fail.
  stage || true
  job_journal "${journal}" "court-auction collect STOPPED run=${run_id}: $(tail -1 "${run_log}")" >&2
  job_run_log_tail "${run_log}"
  exit 1
fi
if stage; then
  job_journal "${journal}" "court-auction collect ok run=${run_id} staged"
  printf 'foundation-job-outcome changed\n'
else
  job_journal "${journal}" "court-auction collect ok run=${run_id} nothing landed"
  printf 'foundation-job-outcome unchanged\n'
fi
