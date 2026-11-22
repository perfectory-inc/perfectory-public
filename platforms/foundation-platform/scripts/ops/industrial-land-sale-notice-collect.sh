#!/usr/bin/env bash
# 산업용지 분양·처분·공급 공고와 그 첨부를 받는다 — 수집 절반 (root ADR-0603). 인자 하나가 원천이다:
# ilis(산업입지정보시스템 분양공고), kicox(한국산업단지공단 임대/분양 게시판), lh(LH 토지 공고, data.go.kr).
# 등록된 작업 industrial_land_sale_notice_<원천>_collect(default_pool)가 작업 등록부의 일정에
# foundation-industrial-land-sale-notice-collect@<원천>.service 로 돌린다. Spark 는 쓰지 않는다.
#
# collect-industrial-land-sale-notices 가 원천의 목록을 새 것부터 쪽마다 읽고, Silver 읽은 기록이 같은 목록 지문으로
# 끝났다고 하지 않는 공고를 계약(industrial-land-sale-notice.contract.json)의 상한·시간 예산 안에서 묻는다. 첨부는
# 형식에 상관없이 공용 미디어 저장소에 내용 해시로 한 번만 담고, 상세는 commit_every 건마다 Bronze 객체 하나로
# 커밋한다. 상세를 하나도 못 읽은 실행만 실패로 끝난다. LH 는 서비스키(data-go-kr 그룹)가 없거나 게이트웨이가
# 활용신청 전이라고 거부하면 아무것도 쓰기 전에 78 로 끝난다(사장 결정을 기다린다).
#
# 적재로 넘기는 것은 없다. 적재(industrial_land_sale_notice_silver)는 R2 의 Bronze 와 Silver 표의 커밋 기록만 보고
# 쌓을 객체를 정한다(plan-silver-load-objects).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

source_name="${1:-}"
case "${source_name}" in
  ilis | kicox | lh) ;;
  *)
    echo "industrial land sale notice collect: usage: $0 ilis|kicox|lh" >&2
    exit 64 # EX_USAGE
    ;;
esac

required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
)
if [ "${source_name}" = lh ]; then
  required_env+=(DATA_GO_KR_SERVICE_KEY)
fi
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "industrial land sale notice collect (${source_name}): refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_LAND_SALE_NOTICE_STATE_ROOT:-/var/lib/foundation-platform/industrial-land-sale-notice}"
COLLECT="FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${source_name}-${run_id}"
mkdir -p "${work}/collect"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial land sale notice collect (${source_name}) FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect"' EXIT

if ! env "${COLLECT}_SOURCE=${source_name}" "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-industrial-land-sale-notices >> "${run_log}" 2>&1; then
  # The provider refused the lane itself (LH before the owner's usage application): nothing was written.
  if refused="$(grep '^industrial-land-sale-notice-refused ' "${run_log}" | tail -1)" && [ -n "${refused}" ]; then
    job_journal "${journal}" "industrial land sale notice collect (${source_name}) REFUSED run=${run_id}: ${refused#industrial-land-sale-notice-refused }" >&2
    exit 78 # EX_CONFIG
  fi
  # What any run committed is in Bronze; the load finds it there.
  job_journal "${journal}" "industrial land sale notice collect (${source_name}) STOPPED run=${run_id}: $(tail -1 "${run_log}")" >&2
  job_run_log_tail "${run_log}"
  exit 1
fi
summary="$(grep '^industrial-land-sale-notice-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "industrial land sale notice collect (${source_name}) ok run=${run_id} ${summary#industrial-land-sale-notice-summary-json }"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
