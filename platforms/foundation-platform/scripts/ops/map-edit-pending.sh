# shellcheck shell=bash
# Sourced: what the edit store holds for one unit, the same for every fold (root ADR-0112 §5·§7).
#
#   map_edit_pending <unit>
#
# Sets `overlay_status` (the HTTP status customers' maps get from the unit's overlay), `pending`
# (the edits not folded yet) and `oldest_age_hours`, and tells Slack #alerts when the overlay does
# not answer 200 or an edit has waited MAX_PENDING_AGE_HOURS. Needs map-edit.env
# (FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL, FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN).
# Used by map-edit-fold.sh (complex, admin) and parcel-tile-bake.sh fold (parcels).

MAP_EDIT_SLACK_TOKEN_FILE="${FOUNDATION_MAP_EDIT_FOLD_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
MAP_EDIT_SLACK_CHANNEL="${FOUNDATION_MAP_EDIT_FOLD_SLACK_CHANNEL:-#alerts}"
# An edit older than this means the hourly fold has not run.
MAP_EDIT_MAX_PENDING_AGE_HOURS="${FOUNDATION_MAP_EDIT_FOLD_MAX_PENDING_AGE_HOURS:-6}"

notify_slack() {
  local token payload
  token="$(tr -d '\r\n' < "${MAP_EDIT_SLACK_TOKEN_FILE}")" || return 0
  payload="$(python3 - "${MAP_EDIT_SLACK_CHANNEL}" "$1" <<'PY'
import json, sys
print(json.dumps({"channel": sys.argv[1], "text": sys.argv[2]}, ensure_ascii=False))
PY
)"
  curl -sS --max-time 30 -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "${payload}" https://slack.com/api/chat.postMessage >/dev/null 2>&1 || true
}

map_edit_pending() {
  local unit="$1" base pending_json
  : "${FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL:?map-edit.env must provide the edit store URL}"
  : "${FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN:?map-edit.env must provide the writer token}"
  base="${FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL%/}"

  # 1. 손님이 읽는 오버레이가 살아 있는가. 죽어 있으면 편집은 저장돼도 아무도 못 본다.
  overlay_status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 "${base}/overlay/${unit}" || true)"
  if [ "${overlay_status}" != "200" ]; then
    notify_slack "🔴 지도 편집 오버레이(${unit})가 ${overlay_status:-응답없음} — 손님이 편집을 못 본다"
  fi

  # 2. 접히지 않은 편집이 몇 건이고 가장 오래된 것이 언제인가.
  pending_json="$(curl -sS --fail --max-time 30 \
    -H "Authorization: Bearer ${FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN}" \
    "${base}/edits/${unit}?after=0&limit=1000")"
  read -r pending oldest_age_hours <<<"$(python3 - "${pending_json}" <<'PY'
import json, sys
from datetime import datetime, timezone
edits = json.loads(sys.argv[1])["edits"]
oldest = min((datetime.fromisoformat(e["edited_at"].replace("Z", "+00:00")) for e in edits), default=None)
age = 0 if oldest is None else int((datetime.now(timezone.utc) - oldest).total_seconds() // 3600)
print(len(edits), age)
PY
)"
  if [ "${oldest_age_hours}" -ge "${MAP_EDIT_MAX_PENDING_AGE_HOURS}" ]; then
    notify_slack "🟠 지도 편집 ${pending}건이 ${oldest_age_hours}시간째 타일에 접히지 않음(${unit}) — 접기가 돌지 않는다"
  fi
}
