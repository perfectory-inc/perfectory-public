#!/usr/bin/env bash
# Posts a unit's failure to Slack. Started by systemd as `foundation-unit-failed@<unit>.service`
# from a unit's `OnFailure=`, so a failure is reported even when the failing script never ran its
# own trap (the database backup failed every night for four weeks and nobody was told).
# Slack answers HTTP 200 with `"ok": false` on a refused post; that is a failure here, not a send.
set -euo pipefail

unit="${1:?usage: notify-unit-failure.sh <unit-name>}"
token_file="${FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
channel="${FOUNDATION_SOURCE_SWEEP_SLACK_CHANNEL:-#alerts}"

result="$(systemctl show -p Result --value "${unit}" 2>/dev/null || true)"
token="$(tr -d '\r\n' < "${token_file}")"
payload="$(python3 - "${channel}" "${unit}" "${result:-unknown}" "$(hostname)" <<'PY'
import json, sys
channel, unit, result, host = sys.argv[1:5]
text = f"🔴 {unit} 실패 (result={result}, host={host}) — journalctl -u {unit}"
print(json.dumps({"channel": channel, "text": text}, ensure_ascii=False))
PY
)"
response="$(curl -sS --max-time 30 -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "${payload}" https://slack.com/api/chat.postMessage)"
python3 -c 'import json, sys; sys.exit(0 if json.loads(sys.argv[1]).get("ok") is True else 1)' "${response}" || {
  printf 'slack refused the failure notice for %s\n' "${unit}" >&2
  exit 1
}
printf 'failure notice sent for %s\n' "${unit}"
