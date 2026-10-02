#!/usr/bin/env bash
# 필지 계보 스튜어드 순환 (root ADR-0115 §9·§11). systemd 타이머가 하루 한 번 돌린다.
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
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh"

STATE_ROOT="${FOUNDATION_LINEAGE_STEWARDSHIP_STATE_ROOT:-/var/lib/foundation-platform/lineage-stewardship}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
SLACK_TOKEN_FILE="${FOUNDATION_LINEAGE_STEWARDSHIP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_LINEAGE_STEWARDSHIP_SLACK_CHANNEL:-#alerts}"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — map-edit-fold.sh 와 같은 재료로 조립한다.
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
  printf '%s stewardship FAILED at line %s run=%s\n' "$(date -u +%FT%TZ)" "$1" "${run_id}" >> "${journal}"
  notify_slack "🔴 필지 계보 스튜어드 순환 실패(줄 $1) — ${run_log}"
}
trap 'on_error ${LINENO}' ERR

spark() {
  docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE}" \
  docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
    -p foundation-platform-compute --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[4]' --driver-memory 4g \
    --jars "${SPARK_RELEASE_JARS}" \
    "/workspace/infra/lakehouse/spark/jobs/$1" "${@:2}" >> "${run_log}" 2>&1
}

json_field() { python3 -c "import json,sys; print(json.load(open(sys.argv[1]))$2)" "$1"; }

# 0. 계보 표가 아직 없으면(9월 필지 적재 전) 할 일이 없다 — 조용히 넘기지 않고 그렇다고 남긴다.
# `|| probe_rc=$?`, not `set +e`: the ERR trap fires even with errexit off, so "no lineage yet"
# (exit 3) was logged and posted to Slack as a failure on every run (found 2026-10-01). A command
# on the left of `||` does not trip the trap.
probe_rc=0
spark lineage_review_queue_to_gold.py --probe-only --summary-output "${container_work}/probe.json" \
  || probe_rc=$?
if [ "${probe_rc}" = 3 ]; then
  printf '%s stewardship waiting: silver.parcel_lineage does not exist yet\n' "$(date -u +%FT%TZ)" >> "${journal}"
  notify_slack "⏳ 필지 계보 검토: 계보 표가 아직 없음 — 필지 스냅숏 적재 뒤 자동 시작"
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
printf '%s stewardship ok run=%s %s\n' "$(date -u +%FT%TZ)" "${run_id}" "${summary}" >> "${journal}"
notify_slack "${summary}"
