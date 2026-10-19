#!/usr/bin/env bash
# 차세대 온비드 공매를 data.go.kr 에서 받는다 — 수집 절반 (root ADR-0280). 등록된 작업
# onbid_public_sale_collect(ADR-0122, default_pool)가 매시간 돌린다. Spark 는 쓰지 않는다.
#
#   1. 수집 — collect-data-go-kr-onbid 가 공고·공고 입찰결과(매시간), 물건 전수(3시간마다), 목록에서 빠진
#             회차의 입찰결과(오래된 것부터)를 API 마다 하루 상한 안에서 받아, API 하나에 gzip JSON Lines
#             Bronze 객체 하나로 남긴다(data-go-kr-onbid.contract.json). 상한을 다 쓴 API 는 다음 날을
#             기다린다. 키·파라미터 문제(00·03 이 아닌 답)는 실패다.
#   2. 넘김 — 물건이나 입찰결과 객체가 생겼으면 실행 폴더를 적재 대기 넘김(lakehouse/pending/<실행>/)으로
#             옮긴다. 적재는 onbid_public_sale_silver(onbid-silver-load.sh)가 한다.
#
# 키(DATA_GO_KR_SERVICE_KEY)는 /etc/foundation-platform/data-go-kr.env 에 있다(runtime-secrets 그룹
# data-go-kr). 없으면 아무것도 하기 전에 78 로 끝난다. 어느 단계든 실패하면 0 이 아닌 값으로 끝나고,
# Airflow 실패 알림이 슬랙에 간다(ADR-0122).
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
  echo "onbid collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_ONBID_STATE_ROOT:-/var/lib/foundation-platform/onbid}"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_ONBID"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector" "${STATE_ROOT}/lakehouse/pending"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "onbid collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 1. 수집
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-data-go-kr-onbid >> "${run_log}" 2>&1

# 2. 넘김: 적재할 객체(물건·입찰결과)가 있을 때만.
loadable="$(python3 -I -c '
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
print(sum(1 for o in manifest["objects"] if o["written"] and o["role"] in ("item_list", "item_bid_result")))
' "${work}/collect/manifest.json")"
if [ "${loadable}" != 0 ]; then
  mv "${work}/collect" "${STATE_ROOT}/lakehouse/pending/${run_id}"
  chmod -R a+rwX "${STATE_ROOT}/lakehouse/pending/${run_id}" # Spark 컨테이너는 uid 185 로 옮긴다.
  job_journal "${journal}" "onbid collect ok run=${run_id} staged"
  printf 'foundation-job-outcome changed\n'
else
  job_journal "${journal}" "onbid collect ok run=${run_id} nothing to load"
  printf 'foundation-job-outcome unchanged\n'
fi
