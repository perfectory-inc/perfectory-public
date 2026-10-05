#!/usr/bin/env bash
# VWorld 연속지적도(MK/30563)의 새 판을 받는다 (root ADR-0148). 등록된 작업(ADR-0122, default_pool)이
# 매일 돌린다. Spark 는 쓰지 않는다: spark 풀은 더 받을 자리가 없다(ADR-0138 의 굶주림 한도).
#
#   1. 확인 — 제공자 목록을 받아(plan-vworld-dataset-collection → inventory-vworld-dataset-files, 필지
#             데이터셋 하나만) 가장 새 기준월을 원천 계약의 판과 비교한다(vworld_parcel_editions.py
#             provider-edition). 계약이 이미 가진 판이면 그렇게 남기고 0 으로 끝난다. 목록은 페이지
#             몇 장이다.
#   2. 수집 — 새 판이면 그 판에서 바로 내려받는 파일 전부를 Bronze 에 받는다(ingest-vworld-dataset-files;
#             어느 벌을 싣는지는 측정이 정한다). 제공자 내려받기 도구(RAON)로만 주는 파일은 빼고 받는다:
#             2026-10 기준 가장 큰 시도 파일 여섯이고, 시군구 파일은 없다. 제공자는 판마다 같은 파일 번호를 다시
#             쓰므로, 이미 받은 판인지는 번호가 아니라 제공자 갱신일로 가린다(holds_listed_release):
#             다시 돌면 받은 파일은 건너뛴다.
#   3. 측정 — 받은 ZIP 마다 중앙 디렉터리를 범위 요청으로 읽어(vworld_parcel_edition_members.py, GDAL
#             이미지) 계약의 판 항목을 만든다(propose). 계약과 합쳐 검사를 통과한 것만 남긴다:
#             proposed/<판>.json.
#   4. 끝 — 판을 계약에 넣는 것은 사람이 PR 로 한다(원천 계약은 저장소의 정본이다). 그때까지 이
#           작업은 3 으로 끝나고 "new edition waiting for contract entry" 를 슬랙에 직접 보낸다(재시도 없음).
#           결함이 아니라 "계약에 넣을 판이 있다"는 뜻이다. Silver 적재는 계약이 그 판을 가진 릴리스에서 런북의 명령으로 한다.
#
# 어느 단계든 실패하면 0 이 아닌 값(3 이 아닌)으로 끝난다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152): 측정의 읽기 키가 없다는 것을 판 전부를 Bronze 에 받은 뒤에
#    알면 안 된다. 이 목록이 단위의 환경 파일에 다 있는지는 저장소 검사가 계약으로 본다
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
  echo "vworld-parcel-edition: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

STATE_ROOT="${FOUNDATION_VWORLD_PARCEL_EDITION_STATE_ROOT:-/var/lib/foundation-platform/vworld-parcel-edition}"
JOBS="${RELEASE_ROOT}/infra/lakehouse/spark/jobs"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/vworld-parcel-source-objects.json"
CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
PY=(python3 -E -s)
AWAITING_CONTRACT=3
journal="${STATE_ROOT}/journal.log"
mkdir -p "${STATE_ROOT}/proposed"

SLACK_TOKEN_FILE="${FOUNDATION_VWORLD_PARCEL_EDITION_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_VWORLD_PARCEL_EDITION_SLACK_CHANNEL:-#alerts}"

editions() { "${PY[@]}" "${JOBS}/vworld_parcel_editions.py" "$@" --contract "${CONTRACT}"; }

# A notice that cannot be sent is logged, not fatal: the exit code still says what happened.
notify_slack() {
  local token payload response
  token="$(tr -d '\r\n' < "${SLACK_TOKEN_FILE}")" || { echo "no Slack token; notice not sent" >&2; return 0; }
  payload="$("${PY[@]}" -c 'import json, sys; print(json.dumps({"channel": sys.argv[1], "text": sys.argv[2]}, ensure_ascii=False))' \
    "${SLACK_CHANNEL}" "$1")"
  response="$(curl -sS --max-time 30 -H "Authorization: Bearer ${token}" -H "Content-Type: application/json; charset=utf-8" \
    -d "${payload}" https://slack.com/api/chat.postMessage 2>&1)" || true
  "${PY[@]}" -c 'import json, sys; sys.exit(0 if json.loads(sys.argv[1]).get("ok") is True else 1)' "${response}" 2>/dev/null ||
    echo "slack refused the notice" >&2
}

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 다른 등록 작업과 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  : "${FOUNDATION_ADMIN_PASSWORD:?recovery.env must provide FOUNDATION_ADMIN_PASSWORD}"
  DATABASE_URL="$(python3 - <<PY
import os, urllib.parse
q = lambda s: urllib.parse.quote(s, safe=str())
port = os.environ.get("FOUNDATION_DB_PORT", "15434")
print("postgres://foundation_admin:" + q(os.environ["FOUNDATION_ADMIN_PASSWORD"])
      + "@127.0.0.1:" + port + "/foundation")
PY
)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'printf "%s vworld-parcel-edition FAILED at line %s run=%s\n" "$(date -u +%FT%TZ)" "${LINENO}" "${run_id}" >> "${journal}"; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true' ERR

# 1. 확인. 계획은 필지 데이터셋 하나만 담은 목록에서 만든다 — 목록 전체를 받으면 다른 데이터셋 수백
#    파일의 목록까지 매일 긁는다.
editions endpoint-catalog --catalog "${CATALOG}" --output "${work}/catalog.json" \
  --summary-output "${work}/inventory-summary.csv"
export FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH="${work}/catalog.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH="${work}/inventory-summary.csv"
export FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH="${work}/plan.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${work}/inventory.json"
"${PUBLISHER_BIN}" plan-vworld-dataset-collection > "${run_log}" 2>&1
"${PUBLISHER_BIN}" inventory-vworld-dataset-files >> "${run_log}" 2>&1
read -r edition held < <(editions provider-edition --inventory "${work}/inventory.json")
if [ "${held}" = held ]; then
  printf '%s vworld-parcel-edition provider=%s held run=%s\n' "$(date -u +%FT%TZ)" "${edition}" "${run_id}" >> "${journal}"
  # 제공자가 같은 기준월로 파일을 다시 올렸으면 계약은 앞 업로드를 말한다. 막지 않고 요약에 경고로 남긴다.
  reuploads="$(editions held-reuploads --edition "${edition}" --inventory "${work}/inventory.json")"
  if [ -n "${reuploads}" ]; then
    printf '%s vworld-parcel-edition WARNING provider re-uploaded %s file(s) of held edition %s after its extraction: %s run=%s\n' \
      "$(date -u +%FT%TZ)" "$(printf '%s\n' "${reuploads}" | wc -l)" "${edition}" "$(printf '%s' "${reuploads}" | tr '\n' ',')" \
      "${run_id}" | tee -a "${journal}" >&2
  fi
  exit 0
fi

# 2. 수집. 이 판의 파일만 담은 목록으로 ingest 를 부른다.
files="$(editions select-inventory --edition "${edition}" --inventory "${work}/inventory.json" \
  --output "${work}/inventory-${edition}.json")"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${work}/inventory-${edition}.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="${work}/ingest-evidence.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1
"${PUBLISHER_BIN}" ingest-vworld-dataset-files >> "${run_log}" 2>&1
"${PY[@]}" - "${work}/ingest-evidence.json" "${files}" > "${work}/keys.txt" <<'PY'
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))
items = [item for item in evidence.get("files", []) if item.get("status") in ("succeeded", "skipped_existing")]
if len(items) != int(sys.argv[2]) or any(not item.get("object_key") for item in items):
    sys.exit(f"the ingest holds {len(items)} of {sys.argv[2]} files of the edition; nothing is proposed")
print("\n".join(sorted(item["object_key"] for item in items)))
PY

# 3. 측정. 자격증명은 이 작업의 환경에 있고 컨테이너에 이름으로만 넘긴다(값을 찍지 않는다).
# The pinned reference tools/technology-versions.contract.json lists (container-images-match-the-contract
# keeps it equal to that list; scripts/catalog/sync-container-images.py rewrites it on a digest change).
GDAL_IMAGE="ghcr.io/osgeo/gdal:ubuntu-small-3.10.2@sha256:a2af3ef63be13b35790ce7a508ff395c409ef7a0b8ddc5ab9685dd4518af9779"
# The measurement only reads (ranged GETs), so it gets the reader key: a container given the
# writer key could overwrite the Bronze objects it measures.
AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID:?the lakehouse reader key is required}" \
AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY:?the lakehouse reader key is required}" \
AWS_S3_ENDPOINT="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT#https://}" \
B="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}" \
  docker run --rm -i --memory 1g -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_S3_ENDPOINT -e B \
    -e AWS_VIRTUAL_HOSTING=FALSE -e AWS_REGION=auto -v "${JOBS}:/jobs:ro" "${GDAL_IMAGE}" \
    python3 /jobs/vworld_parcel_edition_members.py < "${work}/keys.txt" > "${work}/members.jsonl" 2>> "${run_log}"
editions propose --edition "${edition}" --measured "${work}/members.jsonl" \
  --output "${work}/proposed-${edition}.json" >> "${run_log}"
cp "${work}/proposed-${edition}.json" "${STATE_ROOT}/proposed/${edition}.json.tmp"
mv -f "${STATE_ROOT}/proposed/${edition}.json.tmp" "${STATE_ROOT}/proposed/${edition}.json"

# 4. 끝. 계약에 넣을 판이 있다. 알림은 이 작업이 직접 그 뜻으로 보낸다: Airflow 의 실패 알림은 "실패"라고만
#    말한다. 작업은 재시도하지 않는다(jobs.v1.json retries 0) — 다시 돌아도 같은 판을 받고 같은 제안을 낸다.
message="🟡 new edition waiting for contract entry: 연속지적도 ${edition} 판을 받아 계약 항목을 제안했다(${files} 파일). ${STATE_ROOT}/proposed/${edition}.json 을 vworld-parcel-source-objects.json 에 PR 로 넣는다 — 런북 vworld-parcel-editions.md. 결함이 아니다."
printf '%s vworld-parcel-edition provider=%s new: collected %s files, proposed %s run=%s\n' \
  "$(date -u +%FT%TZ)" "${edition}" "${files}" "${STATE_ROOT}/proposed/${edition}.json" "${run_id}" >> "${journal}"
notify_slack "${message}"
echo "new edition waiting for contract entry: edition ${edition}; add ${STATE_ROOT}/proposed/${edition}.json to vworld-parcel-source-objects.json (runbook vworld-parcel-editions.md)" >&2
exit "${AWAITING_CONTRACT}"
