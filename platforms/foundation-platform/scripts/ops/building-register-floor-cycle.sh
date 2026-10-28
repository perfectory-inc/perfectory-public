#!/usr/bin/env bash
# Airflow의 spark pool이 전체 stage/history/native/scalar 실행을 소유한다.
set -euo pipefail

# The publisher is this release's trusted build output outside the release (root ADR-0134 §3).
# Any installed release, not only `current`: ExecStopPost cleans up with the release it started.
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
root_dir="${RELEASE_ROOT}"
publisher="${PUBLISHER_BIN}"
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

cycle_out="$(mktemp)"
trap 'rm -f "${cycle_out}"' EXIT
"${publisher}" run-building-register-floor-cycle | tee "${cycle_out}"
# Reached only when the cycle exited 0 (set -e, pipefail): it ran to its end (root ADR-0171, ADR-0260).
# `changed` only when it persisted rows: an edition already retained writes nothing (2026-10-27 said
# `changed` on `action=already_retained ... persisted_row_count=0`).
ok_line="$(grep '^remote-lakehouse-job-ok ' "${cycle_out}" | tail -n 1)" || true
if [[ "${ok_line}" =~ \ persisted_row_count=([0-9]+) ]]; then
  if (( BASH_REMATCH[1] > 0 )); then
    printf 'foundation-job-outcome changed\n'
  else
    printf 'foundation-job-outcome unchanged\n'
  fi
else
  printf 'foundation-job-outcome failed the cycle printed no remote-lakehouse-job-ok line\n'
fi
