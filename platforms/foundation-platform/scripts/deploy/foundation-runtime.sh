#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FOUNDATION_PLATFORM_COMPOSE_PROJECT="${FOUNDATION_PLATFORM_COMPOSE_PROJECT:-foundation-platform-runtime}"
FOUNDATION_PLATFORM_ENV_FILE="${FOUNDATION_PLATFORM_ENV_FILE:-/etc/foundation-platform/recovery.env}"
export FOUNDATION_PLATFORM_STATE_ROOT="${FOUNDATION_PLATFORM_STATE_ROOT:-/var/lib/foundation-platform}"
export FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-${FOUNDATION_PLATFORM_STATE_ROOT}/lakehouse}"
export FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT="${FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT:-${FOUNDATION_PLATFORM_STATE_ROOT}/remote-lakehouse}"

if [[ "$#" -eq 0 ]]; then
  printf 'usage: foundation-runtime.sh <docker-compose-command> [args...]\n' >&2
  exit 64
fi

if [[ ! -r "${FOUNDATION_PLATFORM_ENV_FILE}" ]]; then
  printf 'Foundation runtime environment file is not readable: %s\n' \
    "${FOUNDATION_PLATFORM_ENV_FILE}" >&2
  exit 66
fi

compose=(
  docker compose
  --project-directory "${root_dir}"
  -f "${root_dir}/docker-compose.yml"
  -f "${root_dir}/compose.recovery.yml"
  --project-name "${FOUNDATION_PLATFORM_COMPOSE_PROJECT}"
  --env-file "${FOUNDATION_PLATFORM_ENV_FILE}"
)

exec "${compose[@]}" "$@"
