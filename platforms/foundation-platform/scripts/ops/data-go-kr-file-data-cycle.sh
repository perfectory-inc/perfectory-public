#!/usr/bin/env bash
# data.go.kr 파일 데이터 — 기타 공공 일괄 파일 수집 (root ADR-0401). 등록된 작업 data_go_kr_file_data(default_pool)가
# 하루 한 번 돌린다. Spark 는 쓰지 않는다.
#
#   수집 — collect-data-go-kr-file-data 가 계약(data-go-kr-file-data.contract.json)의 파일 가운데 등록공장을 뺀 전부를
#          맡는다(등록공장은 작업 registered_factory 가 받는다). 파일마다 포털의 메타데이터를 한 번 묻고, 마지막으로 받은
#          것과 다른 파일(atchFileId)일 때만 받아 그 원천의 Bronze(datagokr__<dataset_slug>)에 남긴다. 파일마다 따로
#          커밋하므로 한 파일이 실패해도 다른 파일은 받고, run 은 끝에 실패한다.
#
# run 당 상한은 파일 자체다(2026-11-04 실측): 8개가 모두 새 판이면 약 365 MB(상가(상권) zip 349 MB 가 59초), 간격을 넣어
# 3분 안팎. 받은 사본은 성공하든 실패하든 지운다. 실패하면 0 이 아닌 값으로 끝나고 Airflow 실패 알림이 간다(ADR-0122).
set -Eeuo pipefail
shopt -s inherit_errexit
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
  echo "data.go.kr file data: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_DATA_GO_KR_FILE_DATA_STATE_ROOT:-/var/lib/foundation-platform/data-go-kr-file-data}"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "data.go.kr file data FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect"' EXIT

datasets="$(python3 -I -c '
import json, sys
files = json.load(open(sys.argv[1], encoding="utf-8"))["files"]
print(",".join(slug for slug in files if slug != "registered_factory"))
' "${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-file-data.contract.json")"

env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${COLLECT}_DATASETS=${datasets}" "${PUBLISHER_BIN}" collect-data-go-kr-file-data >> "${run_log}" 2>&1

outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
landed="$(python3 -I -c 'import json, sys; print(" ".join(f["dataset_slug"] for f in json.load(open(sys.argv[1], encoding="utf-8"))["files"]))' "${work}/collect/manifest.json")"
job_journal "${journal}" "data.go.kr file data ok run=${run_id} ${outcome#foundation-job-outcome } landed=[${landed}]"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
