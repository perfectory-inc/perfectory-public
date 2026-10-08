#!/usr/bin/env bash
# 관리자 폴리곤 편집을 타일에 접는다 (root ADR-0112 §7·§9).
#
# systemd 타이머가 매시간 이 스크립트를 돌린다. 편집 저장소에 접히지 않은 편집이 없으면
# 굽지 않는다 — 같은 원천을 매시간 다시 구우면 타일 주소만 바뀌어 캐시가 버려진다.
# 편집이 있으면: 편집 내보내기 → 원장·Gold 서빙본(Spark) → 굽기·승격·접기.
# `FOUNDATION_MAP_EDIT_FOLD_FORCE=1` 은 편집이 없어도 굽는다(새 Silver 원천을 반영할 때).
#
# 감시: 오버레이가 응답하지 않거나, 접히지 않은 편집이 너무 오래 남았거나, 어느 단계든
# 실패하면 슬랙 #alerts 가 안다. 아무 일도 없던 시간에도 journal 에 한 줄을 남긴다 —
# "할 일 없음"과 "확인 안 함"은 구별되어야 한다.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# 유닛은 첫 인자다 — systemd 템플릿 `foundation-map-edit-fold@<unit>.service` 가 `%i` 로 넘긴다.
UNIT="${1:-${FOUNDATION_MAP_EDIT_FOLD_UNIT:-complex}}"
STATE_ROOT="${FOUNDATION_MAP_EDIT_FOLD_STATE_ROOT:-/var/lib/foundation-platform/map-edit-fold}"
# 전용 폴더다. 공용 /var/lib/foundation-platform/lakehouse 는 Spark(uid 185) 소유라 서비스 계정이 쓸 수 없고,
# 그 소유를 바꾸면 다른 적재 작업이 깨진다(2026-09-29 첫 설치에서 실제로 그랬다).
LAKEHOUSE_STATE_ROOT="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-/var/lib/foundation-platform/map-edit-fold/lakehouse}"
SLACK_TOKEN_FILE="${FOUNDATION_MAP_EDIT_FOLD_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_MAP_EDIT_FOLD_SLACK_CHANNEL:-#alerts}"
# 이보다 오래 접히지 않은 편집은 매시간 접기가 돌지 않았다는 뜻이다.
MAX_PENDING_AGE_HOURS="${FOUNDATION_MAP_EDIT_FOLD_MAX_PENDING_AGE_HOURS:-6}"

# 유닛마다 Silver 좌표계와 서빙본을 만드는 Spark 작업이 다르다. 좌표계의 정본은 각 서빙본
# 계약의 geometry_srid 관문이다 — 여기 값이 어긋나면 Spark 작업이 편집 내보내기를 거부한다.
case "${UNIT}" in
  complex)
    HANDOFF_SRID=5186
    SERVED_JOB=industrial_complex_boundary_served_gold.py
    ;;
  admin)
    HANDOFF_SRID=4326
    SERVED_JOB=administrative_boundary_served_gold.py
    ;;
  *)
    echo "map-edit-fold: 모르는 유닛 ${UNIT}" >&2
    exit 64
    ;;
esac

: "${FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL:?map-edit.env must provide the edit store URL}"
: "${FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN:?map-edit.env must provide the writer token}"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — daily-source-sweep.sh 와 같은 재료로 조립한다.
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

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_log="${STATE_ROOT}/last-run.log"
: > "${run_log}"

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
  printf '%s fold FAILED at line %s\n' "$(date -u +%FT%TZ)" "${line}" >> "${journal}"
  tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

base="${FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL%/}"

# 1. 손님이 읽는 오버레이가 살아 있는가. 죽어 있으면 편집은 저장돼도 아무도 못 본다.
overlay_status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 "${base}/overlay/${UNIT}" || true)"
if [ "${overlay_status}" != "200" ]; then
  notify_slack "🔴 지도 편집 오버레이(${UNIT})가 ${overlay_status:-응답없음} — 손님이 편집을 못 본다"
fi

# 2. 접히지 않은 편집이 몇 건이고 가장 오래된 것이 언제인가.
pending_json="$(curl -sS --fail --max-time 30 \
  -H "Authorization: Bearer ${FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN}" \
  "${base}/edits/${UNIT}?after=0&limit=1000")"
read -r pending oldest_age_hours <<<"$(python3 - "${pending_json}" <<'PY'
import json, sys
from datetime import datetime, timezone
edits = json.loads(sys.argv[1])["edits"]
oldest = min((datetime.fromisoformat(e["edited_at"].replace("Z", "+00:00")) for e in edits), default=None)
age = 0 if oldest is None else int((datetime.now(timezone.utc) - oldest).total_seconds() // 3600)
print(len(edits), age)
PY
)"
if [ "${oldest_age_hours}" -ge "${MAX_PENDING_AGE_HOURS}" ]; then
  notify_slack "🟠 지도 편집 ${pending}건이 ${oldest_age_hours}시간째 타일에 접히지 않음(${UNIT}) — 접기가 돌지 않는다"
fi
if [ "${pending}" = "0" ] && [ "${FOUNDATION_MAP_EDIT_FOLD_FORCE:-0}" != "1" ]; then
  printf '%s fold unit=%s pending=0 overlay=%s skipped\n' "$(date -u +%FT%TZ)" "${UNIT}" "${overlay_status}" >> "${journal}"
  exit 0
fi

# 3. 한 번의 접기 = 한 작업 폴더. 모든 산출물은 증거로 남고 덮어쓰지 않는다.
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/map-edit-fold/${UNIT}/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.

FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_UNIT="${UNIT}" \
FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_SRID="${HANDOFF_SRID}" \
FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF_OUTPUT="${work}/edits.jsonl" \
  "${PUBLISHER_BIN}" export-map-edit-handoff >> "${run_log}" 2>&1

container_work="/workspace/target/lakehouse/map-edit-fold/${UNIT}/${run_id}"
# 한 번 쓰고 끝나는 초기화 컨테이너가 이름을 붙잡고 있으면 compose run 이 이름 충돌로 죽는다.
docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[4]' --driver-memory 4g \
  --jars "${SPARK_RELEASE_JARS}" \
  "/workspace/infra/lakehouse/spark/jobs/${SERVED_JOB}" \
  --edits-input "${container_work}/edits.jsonl" \
  --output "${container_work}/served.jsonl" \
  --summary-output "${container_work}/served-summary.json" \
  --allow-non-smoke-write >> "${run_log}" 2>&1

FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_CONFIRM=1 \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_UNIT="${UNIT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_HANDOFF="${work}/served.jsonl" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_SERVED_SUMMARY="${work}/served-summary.json" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_WORK_ROOT="${work}/bake" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_BUILD_IDEMPOTENCY_KEY="map-edit-fold-${UNIT}-${run_id}-build" \
FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_PROMOTE_IDEMPOTENCY_KEY="map-edit-fold-${UNIT}-${run_id}-promote" \
  "${PUBLISHER_BIN}" bake-lakehouse-tiles >> "${run_log}" 2>&1

result="$(grep -o 'lakehouse-tile-bake-ok.*' "${run_log}" | tail -1)"
printf '%s fold unit=%s pending=%s %s\n' "$(date -u +%FT%TZ)" "${UNIT}" "${pending}" "${result}" >> "${journal}"
notify_slack "🗺️ 지도 편집 ${pending}건을 ${UNIT} 타일에 접었다 — ${result}"
