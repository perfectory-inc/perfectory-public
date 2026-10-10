#!/usr/bin/env bash
# VWorld 필지고유번호변동연혁(MK/30527)을 받는다 — 수집 절반 (root ADR-0144 §4, ADR-0145). 등록된
# 작업(ADR-0122, default_pool)이 하루 한 번 돌린다. Spark 는 쓰지 않는다.
#
#   1. 확인 — 제공자 목록을 받아(plan-vworld-dataset-collection → inventory-vworld-dataset-files, 이 데이터셋
#             하나만) 시도 파일마다 제공자 갱신일을 마지막으로 넘긴 것과 비교한다(changed-files). 바뀐 파일이
#             없으면 그렇게 남기고 0 으로 끝난다. 제공자는 같은 파일 번호에 새 판을 올리므로 번호가 아니라
#             갱신일로 가린다.
#   2. 수집 — 바뀐 파일만 Bronze 에 받는다(ingest-vworld-dataset-files). 파일 번호가 같아도 내용이 바뀌었으니
#             이미 받은 객체로 건너뛰지 않게 다시 받는다(FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1). 키는
#             내용 해시로 짓는다(<파일>--sha256-<해시>.zip, 루트 ADR-0152): 버킷은 같은 키를 덮어쓰지 않으므로
#             번호만으로 지은 키는 새 판도 재실행도 받지 못한다. 같은 바이트를 다시 받으면 제 객체를 찾고
#             아무것도 쓰지 않는다.
#   3. 검사와 넘김 — 받은 객체를 Bronze 에서 읽기 키로 되읽어(내려받은 바이트가 아니라 Bronze 가 가진 바이트)
#             계약(vworld-parcel-number-change-history.contract.json)의 형식·격리 비율·줄어듦 한계로 검사하고,
#             통과하면 적재 대기 넘김(pending/<넘김>/)을 쓴다(stage-handoff). 하나라도 실패하면 넘김도 상태도
#             남기지 않아 다음 실행이 다시 받는다.
#
# 적재는 lineage_stewardship 단위가 법정동 짝 맞추기 앞에서 한다(legal-dong-code-load.sh): 넘김을
# silver.parcel_number_change_history 에 쌓고, 짝 맞추기가 그 기간의 공식 짝을 읽는다. 어느 단계든 실패하면
# 0 이 아닌 값으로 끝나고, Airflow 실패 알림이 슬랙에 간다(ADR-0122).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152). 2026-10-05 첫 실행은 읽기 키가 없다는 것을 Bronze 에 20개를 쓴
#    뒤에야 알았다. 이 목록이 단위의 환경 파일에 다 있는지는 저장소 검사가 계약으로 본다
#    (config/runtime-secrets.contract.json, scripts/deploy/runtime_secrets.py check; 루트 ADR-0153).
required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "parcel-number-change collect: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi
[[ "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}" == https://* ]] || {
  echo "parcel-number-change collect: refused: FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT is not an https URL" >&2
  exit 78
}
command -v docker >/dev/null || { echo "parcel-number-change collect: refused: docker is not on PATH" >&2; exit 78; }

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 다른 등록 작업과 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT:-/var/lib/foundation-platform/parcel-number-change}"
JOBS="${RELEASE_ROOT}/infra/lakehouse/spark/jobs"
CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
# -E -s, not -I: the job imports its sibling modules; -E and -s still shut out PYTHONPATH and the user site.
PY=(python3 -E -s)
journal="${STATE_ROOT}/journal.log"
mkdir -p "${STATE_ROOT}"

history() { "${PY[@]}" "${JOBS}/vworld_parcel_number_change_history.py" "$@"; }

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/downloads"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "parcel-number-change collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 1. 확인. 계획은 이 데이터셋 하나만 담은 목록에서 만든다 — 목록 전체를 받으면 다른 데이터셋 수백 파일의
#    목록까지 매일 긁는다.
history endpoint-catalog --catalog "${CATALOG}" --output "${work}/catalog.json" --summary-output "${work}/inventory-summary.csv"
export FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH="${work}/catalog.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH="${work}/inventory-summary.csv"
export FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH="${work}/plan.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${work}/inventory.json"
"${PUBLISHER_BIN}" plan-vworld-dataset-collection > "${run_log}" 2>&1
"${PUBLISHER_BIN}" inventory-vworld-dataset-files >> "${run_log}" 2>&1
changed="$(history changed-files --inventory "${work}/inventory.json" --state-dir "${STATE_ROOT}" --output "${work}/inventory-changed.json")"
if [ "${changed}" = 0 ]; then
  job_journal "${journal}" "parcel-number-change unchanged run=${run_id}"
  echo '{"status": "unchanged", "files": 0}'
  exit 0
fi

# 2. 수집. 바뀐 파일만 담은 목록으로 ingest 를 부른다.
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${work}/inventory-changed.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="${work}/ingest-evidence.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1
export FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed
# 내용 해시를 재는 동안 본문을 받아 두는 곳(ADR-0168). 이 데이터셋의 파일은 작아(전부 8MB 안팎) 단위의 상태
# 디렉터리 안에 둔다; 실행 디렉터리와 함께 남지 않게 수집기가 파일마다 지운다.
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR="${work}/spool"
"${PUBLISHER_BIN}" ingest-vworld-dataset-files >> "${run_log}" 2>&1

# 3. 되읽기·검사·넘김. 읽기만 하므로 읽기 키를 쓴다(0 에서 확인했다). 내용 해시가 아닌 키는 landed-objects 와
#    stage-handoff 가 거부한다.
# The pinned reference tools/technology-versions.contract.json lists.
AWS_IMAGE="amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
while read -r file_key object_key; do
  AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID}" \
  AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY}" \
  docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION=auto \
    -v "${work}/downloads:/w" "${AWS_IMAGE}" s3 cp --only-show-errors \
    --endpoint-url "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}" \
    "s3://${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}/${object_key}" "/w/${file_key}.zip" >> "${run_log}" 2>&1
done < <(history landed-objects --evidence "${work}/ingest-evidence.json")
result="$(history stage-handoff --inventory "${work}/inventory-changed.json" --evidence "${work}/ingest-evidence.json" \
  --download-dir "${work}/downloads" --state-dir "${STATE_ROOT}" | tee -a "${run_log}")"
rm -rf "${work}/downloads" # the handoff holds its own copy
job_journal "${journal}" "parcel-number-change collect ok run=${run_id} ${result}"
