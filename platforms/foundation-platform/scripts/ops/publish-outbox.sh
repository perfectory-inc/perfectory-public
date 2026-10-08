#!/usr/bin/env bash
# outbox 우체부 한 틱 (root ADR-0079). 30분 타이머가 돌린다.
#
# publish-outbox-once 는 성공 시 무음이므로 판정은 원장이 한다: 실행 후에도 pending 이
# 남는 것은 정상(다음 틱이 잇는다), 명령 실패만 유닛이 슬랙으로 외친다
# (OnFailure=foundation-unit-failed@, 모든 예약 작업 공통).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

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
