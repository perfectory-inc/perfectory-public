#!/usr/bin/env bash
# outbox 우체부 한 틱 (root ADR-0079). 30분 타이머가 돌린다.
#
# publish-outbox-once 는 성공 시 무음이므로 판정은 원장이 한다: 실행 후에도 pending 이
# 남는 것은 정상(다음 틱이 잇는다), 명령 실패만 슬랙으로 외친다.
set -euo pipefail

PUBLISHER_BIN="${FOUNDATION_OUTBOX_PUBLISHER_BIN:-/var/lib/foundation-platform/bin/foundation-outbox-publisher}"
SLACK_TOKEN_FILE="${FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_SOURCE_SWEEP_SLACK_CHANNEL:-#alerts}"

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
trap 'notify_slack "🔴 outbox-publish 틱 실패 (line ${LINENO}) — journalctl -u foundation-outbox-publish"' ERR

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — compose 와 같은 재료로 조립한다
# (foundation_admin + FOUNDATION_ADMIN_PASSWORD; 훑기 스크립트의 전례).
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

export FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER="${FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER:-r2}"
"${PUBLISHER_BIN}" publish-outbox-once
