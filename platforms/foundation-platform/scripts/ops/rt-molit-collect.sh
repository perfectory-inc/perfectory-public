#!/usr/bin/env bash
# 국토교통부 실거래가 12종을 rt.molit.go.kr 조건별 자료제공(벌크 CSV)에서 받는다 — 수집 절반 (root ADR-0348,
# 사장 결정 2026-10-30). 등록된 작업 rt_molit_real_transaction_collect(ADR-0122, default_pool)가 새벽 조용한
# 시간(KST 01–06시)에 매시간 돌린다. Spark 는 쓰지 않는다.
#
#   수집 — collect-rt-molit-real-transaction 이 빚진 (데이터셋, 계약 달) 전국 CSV 를 정해진 순서로(최근 0–2달
#          매일 → 3–12달 매주 → 13–24달 매달 → 처음 받는 이력, 새 달부터, 공장창고·토지·상업업무용 먼저) 한 세션에서
#          하나씩, 5초 간격으로 받는다. run 당 18건, KST 하루 90건(제공자 한도 100건) 안
#          (rt-molit-real-transaction.contract.json manners, root ADR-0281 의 예절). 바이트가 그대로인 달은 다시
#          쓰지 않고, 바뀐 달은 내용 주소 키(export--sha256-<hex>.csv)로 옆에 쌓는다.
#
# 거절(HTTP 403/429/503, JSON·CSV 자리의 HTML)은 그 run 을 끝내고 refused.json 을 남긴다. 그 파일이 있는 동안
# 모든 run 이 요청 없이 실패한다: 사람이 읽고 지워야 다시 묻는다(root ADR-0281 §3). 제공자의 하루 한도 답은
# 다음 KST 날까지 기다린다. 실패하면 0 이 아닌 값으로 끝나고 Airflow 실패 알림이 슬랙에 간다(ADR-0122).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152).
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
  echo "rt.molit collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_REAL_TRANSACTION_STATE_ROOT:-/var/lib/foundation-platform/real-transaction}"
COLLECT="FOUNDATION_PLATFORM_RT_MOLIT_COLLECT"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "rt.molit collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-rt-molit-real-transaction >> "${run_log}" 2>&1

summary="$(grep '^rt-molit-collect-summary-json ' "${run_log}" | tail -1 || true)"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
job_journal "${journal}" "rt.molit collect ok run=${run_id} ${outcome#foundation-job-outcome } ${summary#rt-molit-collect-summary-json }"
rm -rf "${work}/collect"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
