#!/usr/bin/env bash
# data.go.kr 파일 하나를 자기 작업으로 받는다 — 수집만 (root ADR-0604). 인자 하나가 계약
# (data-go-kr-file-data.contract.json)의 dataset_slug 다(유닛 인스턴스 이름에는 _ 가 못 들어가 - 로 쓴다). 그 파일은
# 자기 작업(own_job)을 적고 Silver 칸(silver_load)이 없어야 한다(있으면 그 작업의 cycle 이 받고 쌓는다). 등록된 작업
# pension_workplace_collect(국민연금 가입 사업장)와 employment_insurance_workplace_collect(고용·산재보험 가입 사업장)가
# 작업 등록부의 일정에 foundation-data-go-kr-file-collect@<dataset-slug>.service 로 돌린다. Spark 는 쓰지 않는다.
#
# collect-data-go-kr-file-data 가 포털의 메타데이터를 한 번 묻고, 마지막으로 받은 것과 다른 파일(atchFileId)일 때만
# 받아 그 원천의 Bronze(datagokr__<dataset_slug>)에 받은 그대로 한 번 쓴다. 파일이 past_editions 를 적으면 데이터셋
# 쪽의 주기성 과거 데이터 목록에서 아직 묻지 않은 판을 오래된 것부터 max_per_run 개까지 같은 방법으로 받고, 묻는 대로
# 상태(<dataset_slug>/past.json)에 적는다: 전체 이력은 여러 실행에 나눠 한 번만 받고, 그 뒤 실행은 메타데이터와 목록
# 두 요청이다. 받은 사본은 성공하든 실패하든 지운다. 실패하면 0 이 아닌 값으로 끝나고 실패 알림이 간다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

dataset="${1:-}"
dataset="${dataset//-/_}"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/data-go-kr-file-data.contract.json"
if ! python3 -I -c '
import json, sys
files = json.load(open(sys.argv[1], encoding="utf-8"))["files"]
f = files.get(sys.argv[2])
sys.exit(0 if f and "own_job" in f and "silver_load" not in f else 1)
' "${CONTRACT}" "${dataset}"; then
  echo "data.go.kr file collect: usage: $0 <dataset_slug of a file that names its own_job and has no silver_load>" >&2
  exit 64 # EX_USAGE
fi

# 부작용 전에 전부 확인한다(루트 ADR-0152).
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
  echo "data.go.kr file collect (${dataset}): refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_DATA_GO_KR_FILE_COLLECT_STATE_ROOT:-/var/lib/foundation-platform/data-go-kr-file-collect}"
COLLECT="FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${dataset}-${run_id}"
mkdir -p "${work}/collect" "${STATE_ROOT}/collector"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "data.go.kr file collect (${dataset}) FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/collect"' EXIT

env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_STATE_DIR=${STATE_ROOT}/collector" "${COLLECT}_LIVE_WRITE=1" \
  "${COLLECT}_DATASETS=${dataset}" "${PUBLISHER_BIN}" collect-data-go-kr-file-data >> "${run_log}" 2>&1
summary="$(python3 -I -c '
import json, sys
files = json.load(open(sys.argv[1], encoding="utf-8"))["files"]
past = sum(1 for f in files if "past_edition" in f)
print("landed=%d past=%d bytes=%d" % (len(files), past, sum(f["size_bytes"] for f in files)))
' "${work}/collect/manifest.json")"
job_journal "${journal}" "data.go.kr file collect (${dataset}) ok run=${run_id} ${summary}"
outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
printf '%s\n' "${outcome:-foundation-job-outcome unchanged}"
