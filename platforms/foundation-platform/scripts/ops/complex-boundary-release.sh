#!/usr/bin/env bash
# 산업단지 경계의 새 원천 판을 Silver 와 타일까지 올린다 (root ADR-0240). 예약 작업 complex_boundary_release.
#
#   1. 고르기 — Bronze 의 산단 경계 ZIP(매일 수집이 받는다) 중 가장 늦게 올라온 것 하나.
#   2. 받기   — 그 ZIP 을 읽기 키로 받는다(몇 MB).
#   3. 변환   — 발행기 export-industrial-complex-boundary-silver-handoff 가 shapefile 을 Silver 행으로 바꾼다.
#   4. 적재   — industrial_complex_boundaries_handoff_to_silver.py. 적재 기록(source_record_id)에 그 ZIP 이 이미
#               있으면 아무것도 쓰지 않는다(unchanged). 새 판은 앞 판 옆에 덧붙고, 읽는 쪽은 가장 새 판을 쓴다.
#   5. 타일   — 바뀌었으면 map-edit-fold.sh complex 를 FORCE 로: 새 Silver 위에 편집을 다시 접어 굽는다.
#
# spark 풀 3 자리라 매시 접기(2 자리)와 겹치지 않는다. 마지막 줄은 `foundation-job-outcome changed|unchanged`
# 다(root ADR-0171).
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 접기 단위의 상태 폴더 안에 둔다: 5 단계가 그 단위를 그대로 부르고, 릴리스가 이미 만들어 둔 곳이다.
STATE_ROOT="${FOUNDATION_COMPLEX_BOUNDARY_RELEASE_STATE_ROOT:-/var/lib/foundation-platform/map-edit-fold/complex-boundary-release}"
source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"
LAKEHOUSE_STATE_ROOT="${FOUNDATION_COMPLEX_BOUNDARY_RELEASE_LAKEHOUSE_STATE_ROOT:-$(job_state_root complex_boundary_release)/lakehouse}"
SOURCE_SLUG=vworldkr__sandan_boundary
# 쓰기 키는 이 작업에 필요 없다: 받기는 읽기 키 한 쌍으로만 한다(root ADR-0152).
unset FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
for name in FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
    FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY; do
  [[ -n "${!name:-}" ]] || { echo "complex-boundary-release: ${name} is not set" >&2; exit 78; }
done

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/complex-boundary-release/${run_id}"
container_work="/workspace/target/lakehouse/complex-boundary-release/${run_id}"
mkdir -p "${work}/bronze/source=${SOURCE_SLUG}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"

on_error() {
  job_journal "${journal}" "complex boundary release FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

finish() {
  job_journal "${journal}" "complex boundary release $1 run=${run_id} $2"
  printf 'complex-boundary-release-outcome outcome=%s %s\n' "$1" "$2"
  printf 'foundation-job-outcome %s\n' "$1"
}

aws_read() {
  docker run --rm -v "${work}":/w \
    -e AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY}" \
    amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73 \
    "$@" --endpoint-url "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}"
}

# 1. 가장 늦게 올라온 ZIP. 키가 판을 말하지 않으므로(손 시절 키, 내용 주소 키) 올라온 시각으로 고른다.
aws_read s3api list-objects-v2 --bucket "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}" \
  --prefix "bronze/source=${SOURCE_SLUG}/" --output json > "${work}/listing.json" 2>> "${run_log}"
key="$(python3 -I - "${work}/listing.json" <<'PY'
import json, re, sys
text = open(sys.argv[1], encoding="utf-8").read().strip()
objects = [o for o in (json.loads(text).get("Contents") or [] if text else []) if o["Key"].endswith(".zip")]
newest = max(objects, key=lambda o: (o["LastModified"], o["Key"]), default=None)
if newest and not re.fullmatch(r"bronze/source=[a-z0-9_]+/[A-Za-z0-9._-]+\.zip", newest["Key"]):
    sys.exit(f"refusing an object key outside the Bronze layout: {newest['Key']}")
print(newest["Key"] if newest else "")
PY
)"
if [[ -z "${key}" ]]; then
  finish unchanged "reason=no_object"
  exit 0
fi
object="${key##*/}"

# 2·3. 받고 Silver 행으로 바꾼다. 판 id 는 발행기가 객체 이름에서 정한다(<원천>-<객체>).
aws_read s3 cp "s3://${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}/${key}" "/w/bronze/source=${SOURCE_SLUG}/${object}" \
  --only-show-errors >> "${run_log}" 2>&1
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF_BRONZE_ROOT="${work}" \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF_SOURCE_SLUG="${SOURCE_SLUG}" \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF_SOURCE_OBJECT="${object}" \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF_OUTPUT_PATH="${work}/handoff.jsonl" \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF_SUMMARY_PATH="${work}/export-summary.json" \
  "${PUBLISHER_BIN}" export-industrial-complex-boundary-silver-handoff >> "${run_log}" 2>&1
rm -rf "${work}/bronze"

# 4. 적재. 같은 ZIP 의 두 번째 적재는 적재 기록이 막는다(append_batch_once).
docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[4,8]' --driver-memory 4g --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/industrial_complex_boundaries_handoff_to_silver.py \
  --input "${container_work}/handoff.jsonl" --write-mode iceberg --iceberg-catalog-name lakehouse \
  --summary-output "${container_work}/silver-summary.json" >> "${run_log}" 2>&1
rm -f "${work}/handoff.jsonl"
if grep -q "^silver-industrial-complex-boundaries-iceberg-already-ingested" "${run_log}"; then
  finish unchanged "reason=already_loaded object=${key}"
  exit 0
fi
grep -q "^silver-industrial-complex-boundaries-iceberg-write-ok" "${run_log}"

# 5. 타일: 편집이 없어도 새 Silver 로 굽는다.
FOUNDATION_MAP_EDIT_FOLD_FORCE=1 "${RELEASE_ROOT}/scripts/ops/map-edit-fold.sh" complex >> "${run_log}" 2>&1
finish changed "object=${key}"
