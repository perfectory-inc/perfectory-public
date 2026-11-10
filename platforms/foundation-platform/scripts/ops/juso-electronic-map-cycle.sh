#!/usr/bin/env bash
# 주소정보 전자지도 변동분 — 공식 데이터 연계(update.juso.go.kr/updateInfo.do)로 매일 받아 Bronze 에 쌓는다(루트
# ADR-0475). 등록된 작업 juso_electronic_map 이 하루 한 번 돌린다.
#
#   수집 — collect-juso-electronic-map 이 계약(juso-electronic-map.contract.json)의 요청 코드(cntc_cd)마다 자기
#          커서(다음에 물을 날)부터 어제(KST)까지 최대 10일 창을 오래된 것부터 묻는다. 창마다 받은 파일(100바이트
#          머리 + zip)을 그 코드가 맡는 종류마다 Bronze(jusogokr__<kind>)에 그대로 남기고, 창의 파일이 다 커밋된
#          뒤에만 커서를 옮긴다. run 당 상한은 코드마다 창 수, run 전체 바이트 수다(계약 backfill).
#
# 승인키(runtime-secrets 묶음 juso, FOUNDATION_PLATFORM_JUSO_APP_KEY)가 없으면 아무것도 묻지 않고 그렇다고 적고
# `waiting` 으로 끝난다. 키가 놓인 뒤 첫 일정의 run 이 측정 run 이다(루트 ADR-0457). 제공처가 키나 요청 코드를
# 거부하면 그 자리에서 모든 질문을 멈추고 실패로 끝나 OnFailure 알림이 간다; 다음 일정의 run 이 스스로 다시
# 묻는다(사람이 지울 파일이 없다).
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_JUSO_ELECTRONIC_MAP_STATE_ROOT:-/var/lib/foundation-platform/juso-electronic-map}"
COLLECT="FOUNDATION_PLATFORM_JUSO_ELECTRONIC_MAP"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"

# 0. 키가 없으면 부작용 없이 기다린다: 쓰기 키도, 데이터베이스도, 제공처도 묻지 않는다.
if [ -z "${FOUNDATION_PLATFORM_JUSO_APP_KEY:-}" ]; then
  mkdir -p "${STATE_ROOT}"
  job_journal "${journal}" "juso electronic map waiting run=${run_id}: no approval key (runtime-secrets group juso)"
  printf 'foundation-job-outcome waiting for the juso approval key (runtime-secrets group juso)
'
  exit 0
fi

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
  echo "juso electronic map: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "juso electronic map FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 2. 수집. 거부·실패는 수집기가 0 이 아닌 값으로 끝내고, 받은 것까지는 manifest 에 남는다.
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-juso-electronic-map >> "${run_log}" 2>&1 || collect_status=$?
collect_status="${collect_status:-0}"
[ -f "${work}/collect/manifest.json" ] || exit "${collect_status}" # 설정 거부: 아무것도 묻지 않았다
summary="$(python3 -I -c '
import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({"requests": m.get("requests"), "bytes": m.get("bytes"), "files": len(m["files"]),
                  "stops": m.get("stops"), "refused": m.get("refused"), "failed": m.get("failed")}, ensure_ascii=False))
' "${work}/collect/manifest.json")"
if [ "${collect_status}" != 0 ]; then
  job_journal "${journal}" "juso electronic map COLLECT FAILED run=${run_id}: ${summary}" >&2
  echo "juso electronic map: collection failed or was refused: ${summary}" >&2
  exit 1
fi
files="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["files"]))' "${work}/collect/manifest.json")"
job_journal "${journal}" "juso electronic map ok run=${run_id} ${summary}"
if [ "${files}" != 0 ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
