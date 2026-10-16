#!/usr/bin/env bash
# VWorld 연속지적도(MK/30563)의 새 판을 받는다 (root ADR-0148). 등록된 작업(ADR-0122, default_pool)이
# 매일 돌린다. Spark 는 쓰지 않는다: spark 풀은 더 받을 자리가 없다(ADR-0138 의 굶주림 한도).
#
#   1. 확인 — 제공자 목록을 받아(plan-vworld-dataset-collection → inventory-vworld-dataset-files, 필지
#             데이터셋 하나만) 가장 새 기준월을 Bronze 장부가 가진 판과 비교한다(발행기 vworld-parcel-editions
#             → vworld_parcel_editions.py provider-edition; root ADR-0224). 장부가 이미 가진 판이면 그렇게
#             남기고 0 으로 끝난다. 목록은 페이지 몇 장이다.
#   2. 수집 — 새 판이면 그 판에서 바로 내려받는 파일 전부를 Bronze 에 받는다(ingest-vworld-dataset-files;
#             어느 벌을 싣는지는 측정이 정한다). 제공자 내려받기 도구(RAON)로만 주는 파일은 빼고 받는다:
#             2026-10 기준 가장 큰 시도 파일 여섯이고, 시군구 파일은 없다. 제공자는 판마다 같은 파일 번호를 다시
#             쓰므로, 이미 받은 판인지는 번호가 아니라 제공자 갱신일로 가린다(holds_listed_release):
#             다시 돌면 받은 파일은 건너뛴다.
#   3. 측정 — 받은 ZIP 의 안 이름을 장부 옆에 쌓는다(bronze-object-members.sh vworldkr__parcel, 범위 읽기;
#             root ADR-0169 §1). 그 뒤 장부가 그 판을 완전한 판으로 읽어야 한다: 앞 판보다 덜 덮으면
#             (계약의 new_edition_bounds) 반쯤 받은 목록이고 실패로 끝난다.
#   4. 끝 — 판은 장부에 있다. 계약에 넣는 PR 은 없다(root ADR-0224). Silver 적재는 spark 풀에 자리가 없어
#           런북의 명령으로 사람이 한다. 그때까지 이 작업은 3 으로 끝나고 "new edition waiting for Silver
#           load" 를 슬랙에 직접 보낸다(재시도 없음). 결함이 아니라 "적재할 판이 있다"는 뜻이다.
#
# 어느 단계든 실패하면 0 이 아닌 값(3 이 아닌)으로 끝난다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152): 측정의 읽기 키가 없다는 것을 판 전부를 Bronze 에 받은 뒤에
#    알면 안 된다. 이 목록이 단위의 환경 파일에 다 있는지는 저장소 검사가 계약으로 본다
#    (config/runtime-secrets.contract.json, scripts/deploy/runtime_secrets.py check; 루트 ADR-0153).
required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY
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
MEMBERS="$(dirname "${BASH_SOURCE[0]}")/bronze-object-members.sh"
CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
PY=(python3 -E -s)
AWAITING_LOAD=3
journal="${STATE_ROOT}/journal.log"
mkdir -p "${STATE_ROOT}"

SLACK_TOKEN_FILE="${FOUNDATION_VWORLD_PARCEL_EDITION_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_VWORLD_PARCEL_EDITION_SLACK_CHANNEL:-#alerts}"

editions() { "${PY[@]}" "${JOBS}/vworld_parcel_editions.py" "$@" --contract "${CONTRACT}" --editions "${work}/editions.json"; }
# 판은 장부에서 읽는다(root ADR-0224). 아직 안 잰 객체가 있으면 판을 정할 수 없으므로 먼저 잰다: 이미 잰 객체는
# 다시 재지 않으니 매일 돌아도 몇 초다. 측정은 읽기 키만 쓴다(그 스크립트가 쓰기 키를 지운다).
ledger_editions() {
  "${MEMBERS}" vworldkr__parcel >> "${run_log}" 2>&1
  VWORLD_PARCEL_SOURCE_CONTRACT="${CONTRACT}" "${PUBLISHER_BIN}" vworld-parcel-editions > "${work}/editions.json" 2>> "${run_log}"
}

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
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "vworld-parcel-edition FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# 1. 확인. 계획은 필지 데이터셋 하나만 담은 목록에서 만든다 — 목록 전체를 받으면 다른 데이터셋 수백
#    파일의 목록까지 매일 긁는다.
ledger_editions
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
  job_journal "${journal}" "vworld-parcel-edition provider=${edition} held run=${run_id}"
  # 제공자가 같은 기준월로 파일을 다시 올렸으면 장부의 판은 앞 업로드다. 막지 않고 요약에 경고로 남긴다.
  reuploads="$(editions held-reuploads --edition "${edition}" --inventory "${work}/inventory.json")"
  if [ -n "${reuploads}" ]; then
    printf '%s vworld-parcel-edition WARNING provider re-uploaded %s file(s) of held edition %s after its extraction: %s run=%s\n' \
      "$(date -u +%FT%TZ)" "$(printf '%s\n' "${reuploads}" | wc -l)" "${edition}" "$(printf '%s' "${reuploads}" | tr '\n' ',')" \
      "${run_id}" | tee -a "${journal}" >&2
  fi
  # Silver did not move: a run that does not say so counts as changed and starts what reads it (ADR-0171).
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

# 2. 수집. 이 판의 파일만 담은 목록으로 ingest 를 부른다.
files="$(editions select-inventory --edition "${edition}" --inventory "${work}/inventory.json" \
  --output "${work}/inventory-${edition}.json")"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${work}/inventory-${edition}.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="${work}/ingest-evidence.json"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1
# 판마다 같은 파일 번호를 다시 쓰므로 키는 내용이다(root ADR-0152): 번호 키는 앞 판이 남긴 객체와 부딪히거나(R2 502)
# 그 옛 객체를 "이미 있음"으로 읽는다(2026-10-14). 본문은 해시를 재는 동안 실행 폴더의 spool 에 두고 파일마다 지운다.
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR="${work}/spool"
"${PUBLISHER_BIN}" ingest-vworld-dataset-files >> "${run_log}" 2>&1
"${PY[@]}" - "${work}/ingest-evidence.json" "${files}" > "${work}/keys.txt" <<'PY'
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))
items = [item for item in evidence.get("files", []) if item.get("status") in ("succeeded", "skipped_existing")]
if len(items) != int(sys.argv[2]) or any(not item.get("object_key") for item in items):
    sys.exit(f"the ingest holds {len(items)} of {sys.argv[2]} files of the edition; the edition is not complete")
print("\n".join(sorted(item["object_key"] for item in items)))
PY

# 3. 측정. 받은 ZIP 의 안 이름이 장부 옆에 쌓이면 장부가 판을 안다. 반쯤 받은 판은 장부가 건너뛰므로
#    (skipped) 여기서 판이 없으면 실패다.
ledger_editions
if ! editions source-keys --edition "${edition}" > "${work}/source-keys.txt" 2>> "${run_log}"; then
  echo "the ledger does not read edition ${edition} as complete after collecting it; see ${run_log}" >&2
  false
fi

# 4. 끝. 적재할 판이 있다. 알림은 이 작업이 직접 그 뜻으로 보낸다: Airflow 의 실패 알림은 "실패"라고만
#    말한다. 작업은 재시도하지 않는다(jobs.v1.json retries 0) — 다시 돌아도 같은 판을 다시 확인할 뿐이다.
message="🟡 new edition waiting for Silver load: 연속지적도 ${edition} 판을 받아 장부가 읽었다(${files} 파일). 런북 vworld-parcel-editions.md 의 변환·적재를 돌린다. 결함이 아니다."
job_journal "${journal}" \
  "vworld-parcel-edition provider=${edition} new: collected ${files} files, read from the ledger run=${run_id}"
notify_slack "${message}"
echo "new edition waiting for Silver load: edition ${edition} (runbook vworld-parcel-editions.md)" >&2
# Not a fault: the unit counts this status as success (SuccessExitStatus), so systemd's failure notice and
# the scheduler's red run stay for real failures; the notice above is the one this run sends. Silver is
# loaded later by the runbook's manual step, so nothing downstream starts from this run.
printf 'foundation-job-outcome unchanged\n'
exit "${AWAITING_LOAD}"
