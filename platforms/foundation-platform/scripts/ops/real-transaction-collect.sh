#!/usr/bin/env bash
# 국토교통부 실거래가 12종을 data.go.kr OpenAPI 에서 받는다 — 수집 절반 (root ADR-0348). 등록된 작업
# real_transaction_collect(ADR-0122, default_pool)가 매시간 돌린다. Spark 는 쓰지 않는다.
#
#   수집 — collect-data-go-kr-rtms 가 이 run 이 빚진 (데이터셋, 시군구, 계약 달) 묶음을 정해진 순서로(최근
#          0–2달 → 목록에서 빠진 코드의 마지막 질의 → 3–12달 매주 → 13–24달 매달 → 처음 받는 이력, 공장창고·
#          토지·상업업무용 먼저) 한 번에 하나씩 묻고, 묶음은 totalCount 까지 다 받았을 때만 남긴다. 500 호출마다
#          받은 묶음을 데이터셋 하나에 gzip JSON Lines Bronze 객체 하나로 커밋하고 상태를 저장한다(as it goes).
#          run 하나는 계약의 per_run_call_limit(3,000)·run_time_budget_seconds(30분) 안에서 멈춘다
#          (data-go-kr-rtms.contract.json, root ADR-0300 H). 429 는 그 run 을 거기서 끝내고 다음 run 이 잇는다.
#
# 시군구 목록(lawd-set.json)은 Silver 적재가 reference.legal_dong_code_snapshot 에서 쓴다. 아직 없으면
# 아무것도 묻지 않고 unchanged 로 끝난다. 사흘 넘게 낡았으면 실패한다: 낡은 목록은 새 시군구를 말없이 놓친다.
#
# 키(DATA_GO_KR_SERVICE_KEY)는 /etc/foundation-platform/data-go-kr.env 에 있다(runtime-secrets 그룹
# data-go-kr). 없으면 아무것도 하기 전에 78 로 끝난다. 실패하면 0 이 아닌 값으로 끝나고 Airflow 실패 알림이
# 슬랙에 간다(ADR-0122).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152).
required_env=(
  DATA_GO_KR_SERVICE_KEY
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
  echo "real-transaction collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_REAL_TRANSACTION_STATE_ROOT:-/var/lib/foundation-platform/real-transaction}"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_RTMS"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "real-transaction collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" \
  "${COLLECT}_LAWD_SET=${STATE_ROOT}/lawd-set.json" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-data-go-kr-rtms >> "${run_log}" 2>&1

summary="$(grep '^data-go-kr-rtms-summary-json ' "${run_log}" | tail -1 || true)"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
job_journal "${journal}" "real-transaction collect ok run=${run_id} ${outcome#foundation-job-outcome } ${summary#data-go-kr-rtms-summary-json }"
# The objects are in R2 (the Bronze ledger records them); the run folder is not needed again.
rm -rf "${work}/collect"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
