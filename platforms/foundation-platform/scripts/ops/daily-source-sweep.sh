#!/usr/bin/env bash
# 관은 매일 원천을 살핀다 (root ADR-0077).
#
# systemd 타이머가 매일 새벽 이 스크립트를 돌린다. provider 목록을 다시 계획하고,
# 이미 가진 파일은 지문(provider_file_id)으로 건너뛰며, 새 파일만 Bronze 로 받는다.
# 신규 0 인 날도 journal 에 한 줄을 남긴다 — "아무 일도 없었음"과 "확인 안 함"은
# 구별되어야 한다. 신규가 있거나 실패하면 슬랙 #alerts 가 안다.
set -euo pipefail

STATE_ROOT="${FOUNDATION_SOURCE_SWEEP_STATE_ROOT:-/var/lib/foundation-platform/source-sweep}"
PUBLISHER_BIN="${FOUNDATION_SOURCE_SWEEP_PUBLISHER_BIN:-/var/lib/foundation-platform/bin/foundation-outbox-publisher}"
SLACK_TOKEN_FILE="${FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_SOURCE_SWEEP_SLACK_CHANNEL:-#alerts}"

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_log="${STATE_ROOT}/last-run.log"
plan_path="${STATE_ROOT}/building-hub-plan.json"
evidence_path="${STATE_ROOT}/building-hub-evidence.json"

# 슬랙으로 한 줄 보낸다. 토큰은 변수로만 다루고 어디에도 찍지 않는다. 배달 실패가
# sweep 자체를 죽여서는 안 되므로 (이미 journal 이 정본이다) 오류는 삼킨다.
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
  local line="$1"
  printf '%s sweep FAILED at line %s (tail of run log follows)\n' \
    "$(date -u +%FT%TZ)" "${line}" >> "${journal}"
  tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true
  notify_slack "🔴 daily-source-sweep 실패 (line ${line}) — 서버 ${STATE_ROOT}/last-run.log 를 볼 것"
}
trap 'on_error ${LINENO}' ERR

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — postgres 컨테이너와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  : "${POSTGRES_USER:?recovery.env must provide POSTGRES_USER}"
  : "${POSTGRES_PASSWORD:?recovery.env must provide POSTGRES_PASSWORD}"
  DATABASE_URL="$(python3 - <<PY
import os, urllib.parse
q = lambda s: urllib.parse.quote(s, safe=str())
port = os.environ.get("FOUNDATION_DB_PORT", "15434")
print("postgres://" + q(os.environ["POSTGRES_USER"]) + ":" + q(os.environ["POSTGRES_PASSWORD"])
      + "@127.0.0.1:" + port + "/foundation")
PY
)"
  export DATABASE_URL
fi

export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH="${plan_path}"
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_EVIDENCE_PATH="${evidence_path}"
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE=1
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_CONFIRM_FULL_DOWNLOAD=1

"${PUBLISHER_BIN}" plan-building-hub-bulk-collection > "${run_log}" 2>&1
"${PUBLISHER_BIN}" ingest-building-hub-bulk-collection >> "${run_log}" 2>&1

# 증거를 요약해 journal 한 줄 + (신규가 있을 때만) 슬랙 알림으로 바꾼다.
summary="$(python3 - "${evidence_path}" <<'PY'
import json, sys
e = json.load(open(sys.argv[1], encoding="utf-8"))
new_files = [j for j in e.get("jobs", []) if j.get("status") == "succeeded"]
line = (
    f"planned={e.get('selected_job_count')} new={e.get('succeeded_job_count')} "
    f"skipped={e.get('skipped_job_count')} failed={e.get('failed_job_count')} "
    f"status={e.get('status')}"
)
names = ", ".join(
    f"{j.get('source_slug')}:{j.get('provider_file_id')}" for j in new_files[:10]
)
if len(new_files) > 10:
    names += f" 외 {len(new_files) - 10}건"
print(line)
print(names)
PY
)"
line="$(printf '%s' "${summary}" | head -1)"
names="$(printf '%s' "${summary}" | tail -n +2)"
printf '%s sweep %s\n' "$(date -u +%FT%TZ)" "${line}" >> "${journal}"

new_count="$(printf '%s' "${line}" | grep -oE 'new=[0-9]+' | cut -d= -f2)"
failed_count="$(printf '%s' "${line}" | grep -oE 'failed=[0-9]+' | cut -d= -f2)"
if [ "${failed_count:-0}" != "0" ]; then
  notify_slack "🔴 daily-source-sweep: 작업 ${failed_count}건 실패 (${line})"
  exit 1
fi
if [ "${new_count:-0}" != "0" ]; then
  notify_slack "📦 새 원천 파일 ${new_count}건 도착 — 오늘 반영할 것 (ADR-0077 §5): ${names}"
fi
