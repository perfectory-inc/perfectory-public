#!/usr/bin/env bash
# Airflow의 spark pool이 전체 stage/history/native/scalar 실행을 소유한다.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
publisher="${root_dir}/bin/foundation-outbox-publisher"
readonly root_dir publisher

case "${1:-run}" in
  run) [[ "$#" -le 1 ]] || exit 64 ;;
  cleanup)
    [[ "$#" == 1 ]] || exit 64
    : "${INVOCATION_ID:?systemd invocation is required for cleanup}"
    exec "${publisher}" \
      stop-building-register-floor-cycle
    ;;
  *) echo 'expected run or cleanup' >&2; exit 64 ;;
esac

: "${FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT:?release root is required}"
[[ "${FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT}" == "${root_dir}" ]] || {
  echo 'FLOOR source root differs from the running release' >&2
  exit 65
}
: "${FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH:?protected history witness is required}"
: "${FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE:?pinned local control image is required}"
: "${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:?absolute mutable state root is required}"
: "${FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE:?absolute Ivy cache is required}"

# systemd supplies the existing runtime credentials. Compose owns the API role,
# database and published host port; do not store another DATABASE_URL or use admin.
if [[ -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(docker compose --project-directory "${root_dir}" \
    --env-file /dev/null -f "${root_dir}/docker-compose.yml" \
    config --format json --no-env-resolution 2>/dev/null \
    | python3 "${root_dir}/scripts/ops/runtime-database-url.py")"; then
    echo 'cannot resolve the existing runtime database connection' >&2
    exit 78
  fi
  export DATABASE_URL
fi

exec "${publisher}" \
  run-building-register-floor-cycle
