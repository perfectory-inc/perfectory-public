#!/usr/bin/env bash
# 필지 계보 스튜어드 순환 (root ADR-0115 §9·§11). systemd 타이머가 하루 한 번 돌린다.
#
#   0a. 법정동 — 수집 작업이 넘긴 법정동 코드 변경을 먼저 적재하고 짝을 맞춘다(legal-dong-code-load.sh,
#               root ADR-0143). 넘김이 없으면 그렇게 남기고 지나간다.
#
#   0b. 도출   — 원천 계약의 필지 판 이웃 쌍마다, 시도 묶음마다 계보를 한 번 도출해 쌓는다
#               (parcel_lineage_scheduled.py, root ADR-0113 §1). 쌓인 묶음은 건너뛰고 예산을 넘기면 다음 회차가 잇는다.
#
#   1. 접기   — 서 있는 스튜어드 결정을 silver.parcel_lineage 에 steward 행으로 쌓는다
#               (export-lineage-steward-fold → Spark → record-lineage-steward-folds).
#   2. 목록   — 계보에서 검토 목록을 다시 만든다(gold.lineage_review_queue + 넘김 파일).
#               접기를 먼저 해야 방금 결정한 필지가 목록에서 빠진다.
#   3. 적재   — 넘김 파일을 스튜어드 DB 에 통째로 바꿔 넣는다(load-lineage-review-items).
#   4. 요약   — 슬랙에 대기 건수·지역·접힌 결정·다시 열린 건을 보낸다.
#
# "할 일 없음"과 "확인 안 함"은 구별되어야 한다: 계보 표가 아직 없으면 그렇다고 알리고, 어느 단계든
# 실패하면 슬랙 #alerts 가 안다. 모든 산출물은 실행마다 새 작업 폴더에 남는다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_LINEAGE_STEWARDSHIP_STATE_ROOT:-/var/lib/foundation-platform/lineage-stewardship}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
SLACK_TOKEN_FILE="${FOUNDATION_LINEAGE_STEWARDSHIP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_LINEAGE_STEWARDSHIP_SLACK_CHANNEL:-#alerts}"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — map-edit-fold.sh 와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
container_work="/workspace/target/lakehouse/runs/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
journal="${STATE_ROOT}/journal.log"
run_log="${work}/run.log"

notify_slack() {
  local token payload
  token="$(tr -d '\r\n' < "${SLACK_TOKEN_FILE}")" || return 0
  payload="$(python3 - "${SLACK_CHANNEL}" "$1" <<'PY'
import json, sys
print(json.dumps({"channel": sys.argv[1], "text": sys.argv[2]}, ensure_ascii=False))
PY
)"
  curl -sS --max-time 30 -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "${payload}" https://slack.com/api/chat.postMessage >/dev/null 2>&1 || true
}

on_error() {
  job_journal "${journal}" "stewardship FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

spark() {
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p foundation-platform-compute --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[4,8]' --driver-memory 4g \
    --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >> "${run_log}" 2>&1
}

# The release's publisher, for the sourced legal-dong load (root ADR-0263); output goes to the run log.
publisher() {
  "${PUBLISHER_BIN}" "$@"
}

# One line per refused lineage group, naming the 동 codes the code change table does not pair (Slack).
lineage_refusal_text() {
  PYTHONIOENCODING=utf-8 python3 - "$1" <<'PY'
import json, sys
summary = json.load(open(sys.argv[1], encoding="utf-8"))
lines = ["⛔ 필지 계보 도출 거부 — 코드 변경 표에 짝이 없는 법정동 코드:"]
for pair in summary.get("pairs", []):
    for group in pair.get("refused", []):
        codes = ", ".join(group.get("unpaired_codes") or []) or "(코드 없음: " + group.get("reason", "")[:200] + ")"
        lines.append(f"• {pair['from']}→{pair['to']} 시도 {group['from_sido']}→{group['to_sido']}: {codes}")
lines.append("공식 변동연혁(30527)이 반영되면 다음 회차가 잇는다; 그 전엔 스튜어드 목록(ADR-0144 §3)으로만 정한다.")
print("\n".join(lines))
PY
}

json_field() { python3 -c "import json,sys; print(json.load(open(sys.argv[1]))$2)" "$1"; }

# 0a. 법정동 코드 변경 (root ADR-0143). 계보가 그 소비자이므로 계보 앞에서, 이 단위의 spark 자리로 적재하고
# 짝을 맞춘다. 실패하면 여기서 멈춘다 — 낡은 크로스워크로 계보에 들어가지 않는다.
source "$(dirname "${BASH_SOURCE[0]}")/legal-dong-code-load.sh"
load_legal_dong_code_handoffs

# 0b. 계보 도출 (root ADR-0113 §1). 원천 계약이 가진 필지 판의 이웃 쌍마다, 시도 묶음마다 한 번 도출해
# silver.parcel_lineage 에 쌓는다. 이미 쌓은 묶음은 건너뛰고, 예산이 지나면 새 묶음을 시작하지 않는다 —
# 남은 묶음은 다음 회차가 잇는다. 예산은 단위 상한(2h) 안에서 뒤 단계 몫을 남긴다.
spark parcel_lineage_scheduled.py --allow-non-smoke-write \
  --deadline-minutes "${FOUNDATION_LINEAGE_DERIVE_DEADLINE_MINUTES:-60}" --work-dir "${container_work}" \
  --summary-output "${container_work}/derive-summary.json"
derive_refused="$(json_field "${work}/derive-summary.json" '["refused"]')"
job_journal "${journal}" "lineage derive run=${run_id} derived=$(json_field "${work}/derive-summary.json" '["derived"]') deferred=$(json_field "${work}/derive-summary.json" '["deferred"]') refused=${derive_refused} waiting=$(json_field "${work}/derive-summary.json" '.get("waiting", [])')"
# A refused group did not stop the others or the steps below; the unit still fails at the end.
grep -a '^parcel-lineage-refused ' "${run_log}" >&2 || true

# 0. 계보 표가 아직 없으면(9월 필지 적재 전) 할 일이 없다 — 조용히 넘기지 않고 그렇다고 남긴다.
# `|| probe_rc=$?`, not `set +e`: the ERR trap fires even with errexit off, so "no lineage yet"
# (exit 3) was logged and posted to Slack as a failure on every run (found 2026-10-01). A command
# on the left of `||` does not trip the trap.
probe_rc=0
spark lineage_review_queue_to_gold.py --probe-only --summary-output "${container_work}/probe.json" \
  || probe_rc=$?
if [ "${probe_rc}" = 3 ]; then
  job_journal "${journal}" "stewardship waiting: silver.parcel_lineage does not exist yet"
  notify_slack "⏳ 필지 계보 검토: 계보 표가 아직 없음 — 필지 스냅숏 적재 뒤 자동 시작"
  printf 'foundation-job-outcome waiting silver.parcel_lineage does not exist yet\n'
  exit 0
elif [ "${probe_rc}" != 0 ]; then
  on_error "probe"
  exit "${probe_rc}"
fi

# 1. 접기
FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_OUTPUT="${work}/fold.json" \
  "${PUBLISHER_BIN}" export-lineage-steward-fold >> "${run_log}" 2>&1
folded=0
if [ "$(json_field "${work}/fold.json" '["row_count"]')" != 0 ]; then
  spark lineage_steward_fold_to_silver.py --input "${container_work}/fold.json" \
    --summary-output "${container_work}/fold-summary.json" --allow-non-smoke-write
  FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_SUMMARY="${work}/fold-summary.json" \
    "${PUBLISHER_BIN}" record-lineage-steward-folds >> "${run_log}" 2>&1
  folded="$(json_field "${work}/fold-summary.json" '["rows"]')"
fi

# 2. 목록
spark lineage_review_queue_to_gold.py --allow-non-smoke-write \
  --summary-output "${container_work}/queue-summary.json" --handoff-output "${container_work}/queue.json"

# 3. 적재
FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_INPUT="${work}/queue.json" \
FOUNDATION_PLATFORM_LINEAGE_REVIEW_LOAD_CONFIRM=true \
  "${PUBLISHER_BIN}" load-lineage-review-items >> "${run_log}" 2>&1

# 4. 요약
summary="$(python3 - "${work}/queue-summary.json" "${folded}" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
regions = ", ".join(f"{k} {v}" for k, v in sorted(s.get("open_by_sido", {}).items())) or "없음"
print(f"🗂 필지 계보 검토: 대기 {s.get('open', 0)}건 (ownership 불일치 {s.get('open_needs_review', 0)}, 짝 없음 {s.get('open_pending', 0)}) · "
      f"지역 {regions} · 오늘 반영된 결정 {sys.argv[2]}건 · 새 근거로 다시 열림 {s.get('reopened_by_new_evidence', 0)}건")
PY
)"
job_journal "${journal}" "stewardship ok run=${run_id} ${summary}"
notify_slack "${summary}"
if [ "${derive_refused}" != 0 ]; then
  job_journal "${journal}" "stewardship FAILED: ${derive_refused} lineage group(s) refused run=${run_id}" >&2
  # What a person acts on: which group, and which 동 codes the code change table does not pair.
  notify_slack "$(lineage_refusal_text "${work}/derive-summary.json")"
  printf 'foundation-job-outcome failed %s lineage groups refused\n' "${derive_refused}"
  exit 1
fi
# The run's last line (root ADR-0171, ADR-0260): it loaded the review queue and folded decisions.
printf 'foundation-job-outcome changed\n'
