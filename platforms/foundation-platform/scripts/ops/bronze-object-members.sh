#!/usr/bin/env bash
# Bronze ZIP 안 파일 이름을 장부 옆에 쌓는다 (root ADR-0169 §1).
#
#   bronze-object-members.sh [<sources>]
#
# <sources> 는 쉼표로 이은 원천 slug 이고, 끝의 `*` 는 "그것으로 시작하는 모든 slug" 다. 생략하면
# ADR-0169 가 판을 장부에서 고르는 원천 전부(아래 DEFAULT_SOURCES)를 잰다. 이 목록은 여기 한 곳에만 있다.
#
# 과거 객체를 한 번 채울 때 (운영자, 계약이 이 실행에 주는 환경 파일로; root ADR-0153):
#
#   sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
#     $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties bronze-object-members) \
#     /opt/foundation-platform/current/scripts/ops/bronze-object-members.sh
#
# 먼저 보기만: `-E FOUNDATION_PLATFORM_BRONZE_MEMBER_DRY_RUN=true` 를 더하면 재고 요약만 찍고 아무것도 쓰지
# 않는다. 한 번에 잴 개수는 FOUNDATION_PLATFORM_BRONZE_MEMBER_LIMIT (기본 2000), 동시 객체 수는
# FOUNDATION_PLATFORM_BRONZE_MEMBER_CONCURRENCY (기본 8). 다 잴 때까지 다시 돌리면 된다 — 이미 잰 객체는
# 고르지 않는다.
#
# 매일: 매일 수집(daily-source-sweep.sh) 이 레인들 뒤에 이 스크립트를 인자 없이 부른다(ADR-0169 §1). sweep
# 단위는 lakehouse-reader 묶음을 함께 싣는다. 한 실행은 위 개수 상한만큼만 재므로, 과거 객체는 매일 실행이
# 상한씩 채우거나 운영자가 위 명령을 여러 번 돌려 채운다. 그 둘이 겹쳐도 같은 객체를 두 번 쓰지 않는다.
#
# 보장:
# - 읽기 전용 키 한 쌍으로만 R2 를 읽는다. source-sweep 묶음이 함께 실어 오는 쓰기 키 쌍은 발행기를 띄우기
#   전에 지운다.
# - 객체마다 범위 GET 한두 번(끝 128 KiB, 디렉터리가 그 앞이면 한 번 더)이고 객체 전체를 받지 않는다.
# - 마지막 줄은 요약 JSON(`bronze-object-members-json`)이다. 읽지 못한 객체가 있으면 `failed` 로 남기고
#   0 이 아닌 값으로 끝난다; 다음 실행이 다시 잰다.
set -euo pipefail

log() { printf '%s bronze-object-members: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }

DEFAULT_SOURCES='vworldkr__land*,hubgokr__*'

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"

required_env=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
)
for name in "${required_env[@]}"; do
  [[ -n "${!name:-}" ]] || refuse "${name} is not set; run with: runtime_secrets.py properties bronze-object-members" 78
done
unset FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY

export FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES="${1:-${FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES:-${DEFAULT_SOURCES}}}"
# 행마다 그것을 쓴 코드를 남긴다(admitted-writer-runtime.sh 가 정한 릴리스).
export FOUNDATION_PLATFORM_RELEASE_ID="${RELEASE_ID}"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — daily-source-sweep.sh 와 같은 재료로 조립한다. sweep 이
# 부를 때는 이미 내보낸 값을 그대로 쓴다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

log "sources ${FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES}"
exec "${PUBLISHER_BIN}" measure-bronze-object-members
