#!/usr/bin/env bash
# Posts a unit's failure to Slack. Started by systemd as `foundation-unit-failed@<unit>.service`
# from a unit's `OnFailure=`, so a failure is reported even when the failing script never ran its
# own trap (the database backup failed every night for four weeks and nobody was told).
# Slack answers HTTP 200 with `"ok": false` on a refused post; that is a failure here, not a send.
#
#   notify-unit-failure.sh <unit>              the unit failed
#   notify-unit-failure.sh --recovered <unit>  the unit succeeded (foundation-unit-recovered@)
#
# A unit that also says when it succeeds (`OnSuccess=foundation-unit-recovered@%n.service`) is
# reported through the platform's Alertmanager instead, as one alert per unit: Alertmanager sends
# one notice when it starts failing, one when it succeeds again, and a reminder a day while it
# keeps failing (infra/observability/alertmanager, route service="unit-failure"). 2026-10-28/29:
# every hourly by-PNU serving monitor failed every run, and ten notices an hour hid the real ones.
# Without that line a run's success is never reported, so such a unit keeps a notice per failure.
set -euo pipefail

recovered=false
if [[ "${1:-}" == "--recovered" ]]; then
  recovered=true
  shift
fi
unit="${1:?usage: notify-unit-failure.sh [--recovered] <unit-name>}"
token_file="${FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
channel="${FOUNDATION_SOURCE_SWEEP_SLACK_CHANNEL:-#alerts}"
alertmanager="http://127.0.0.1:${FOUNDATION_ALERTMANAGER_PORT:-19093}/api/v2/alerts"

result="$(systemctl show -p Result --value "${unit}" 2>/dev/null || true)"
# The unit's Documentation= (its runbook), when it names one.
runbook="$(systemctl show -p Documentation --value "${unit}" 2>/dev/null || true)"
# Whether the unit reports its successes, so a failure can be an alert that resolves.
reports_success=false
if [[ "$(systemctl show -p OnSuccess --value "${unit}" 2>/dev/null || true)" == *foundation-unit-recovered@* ]]; then
  reports_success=true
fi
text="$(python3 - "${unit}" "${result:-unknown}" "$(hostname)" "${runbook}" <<'PY'
import sys
unit, result, host, runbook = sys.argv[1:5]
text = f"🔴 {unit} 실패 (result={result}, host={host}) — journalctl -u {unit}"
if runbook:
    text += f" — 런북: {runbook}"
print(text)
PY
)"

# One alert per unit. A failure holds it for a day past the last failure, so a unit that stops
# running is not reported as failing forever; a success ends it now.
alert() {
  python3 - "$1" "${unit}" "${text}" "${runbook}" <<'PY'
import datetime, json, sys
state, unit, text, runbook = sys.argv[1:5]
now = datetime.datetime.now(datetime.timezone.utc)
ends = now if state == "resolved" else now + datetime.timedelta(hours=26)
print(json.dumps([{
    "labels": {"alertname": "FoundationUnitFailed", "service": "unit-failure",
               "environment": "prelaunch", "unit": unit},
    "annotations": {"summary": text, "runbook": runbook},
    "endsAt": ends.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
}], ensure_ascii=False))
PY
}
post_alert() {
  curl -sS --fail --max-time 10 -H "Content-Type: application/json" -d "$(alert "$1")" "${alertmanager}" >/dev/null
}

if [[ "${recovered}" == true ]]; then
  # Silent unless Alertmanager announced the failure: it sends a resolve only for what it sent.
  post_alert resolved
  printf 'recovery reported for %s\n' "${unit}"
  exit 0
fi
if [[ "${reports_success}" == true ]]; then
  if post_alert firing; then
    printf 'failure alert raised for %s\n' "${unit}"
    exit 0
  fi
  # Never lose a failure: without Alertmanager it goes to Slack as before.
  printf 'alertmanager refused the failure alert for %s; posting to slack\n' "${unit}" >&2
fi

token="$(tr -d '\r\n' < "${token_file}")"
payload="$(python3 -c 'import json, sys; print(json.dumps({"channel": sys.argv[1], "text": sys.argv[2]}, ensure_ascii=False))' "${channel}" "${text}")"
response="$(curl -sS --max-time 30 -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "${payload}" https://slack.com/api/chat.postMessage)"
python3 -c 'import json, sys; sys.exit(0 if json.loads(sys.argv[1]).get("ok") is True else 1)' "${response}" || {
  printf 'slack refused the failure notice for %s\n' "${unit}" >&2
  exit 1
}
printf 'failure notice sent for %s\n' "${unit}"
