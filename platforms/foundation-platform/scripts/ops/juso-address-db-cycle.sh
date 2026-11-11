#!/usr/bin/env bash
# 주소DB(도로명주소 ↔ 지번) — 로그인·키 없이 제공처의 공개 내려받기 화면 API 로 받아 Bronze 에 쌓는다(루트
# ADR-0495). 등록된 작업 juso_address_db 가 하루 한 번 돌린다.
#
#   수집 — collect-juso-address-db 가 계약(juso-address-db.contract.json)대로, 기초가 없으면 가장 새 월전체 하나를
#          기초로 받고(jusogokr__address_db_monthly_full), 커서(다음에 받을 날)부터 어제(KST)까지 날마다의 일변동
#          파일을 오래된 것부터 받는다(jusogokr__address_db_daily_change). 파일마다 따로 커밋하고, 커밋된 뒤에만
#          커서를 옮긴다. run 당 상한은 계약 per_run_cap(일변동 파일 수, 바이트 수)이다.
#
# 실패하면 0 이 아닌 값으로 끝나 유닛의 OnFailure 알림이 간다(루트 ADR-0445). 받은 것까지는 남고 다음 run 이
# 커서부터 잇는다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_JUSO_ADDRESS_DB_STATE_ROOT:-/var/lib/foundation-platform/juso-address-db}"
COLLECT="FOUNDATION_PLATFORM_JUSO_ADDRESS_DB"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"

# 1. 부작용 전에 전부 확인한다(루트 ADR-0152).
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
  echo "juso address db: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "juso address db FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 2. 수집. 실패는 수집기가 0 이 아닌 값으로 끝내고, 받은 것까지는 manifest 에 남는다.
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-juso-address-db >> "${run_log}" 2>&1 || collect_status=$?
collect_status="${collect_status:-0}"
[ -f "${work}/collect/manifest.json" ] || exit "${collect_status}" # 설정 거부: 아무것도 묻지 않았다
summary="$(python3 -I -c '
import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({"requests": m.get("requests"), "bytes": m.get("bytes"), "files": len(m["files"]),
                  "base_month": m.get("base_month"), "next_day": m.get("next_day"), "stop": m.get("stop"),
                  "failed": m.get("failed")}, ensure_ascii=False))
' "${work}/collect/manifest.json")"
if [ "${collect_status}" != 0 ]; then
  job_journal "${journal}" "juso address db COLLECT FAILED run=${run_id}: ${summary}" >&2
  echo "juso address db: collection failed: ${summary}" >&2
  exit 1
fi
files="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["files"]))' "${work}/collect/manifest.json")"
job_journal "${journal}" "juso address db ok run=${run_id} ${summary}"
if [ "${files}" != 0 ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
