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

# The identity bridge overlay needs the shared network to exist before compose
# parses it (root ADR-0080); creation is idempotent and owned by no one wrapper.
docker network inspect identity-shared >/dev/null 2>&1 \
  || docker network create identity-shared >/dev/null

# The bridge sidecars must listen exactly where the operative identity URLs in
# the runtime env file point (root ADR-0081) — the ports are derived from those
# URLs here, never written a second time. On a host whose env still carries
# placeholders the derivation falls back to the compose defaults, which the
# endpoint guard keeps equal to the identity platform's contract.
derive_port_from_env_url() {
  local key="$1"
  local url
  url="$(grep -E "^${key}=" "${FOUNDATION_PLATFORM_ENV_FILE}" | tail -1 | cut -d= -f2-)"
  [[ -n "${url}" ]] || return 0
  python3 -c "
import sys, urllib.parse
parts = urllib.parse.urlsplit(sys.argv[1])
if parts.port:
    print(parts.port)
" "${url}" 2>/dev/null || true
}
issuer_port="$(derive_port_from_env_url ZITADEL_ISSUER_URL)"
identity_api_port="$(derive_port_from_env_url IDENTITY_API_BASE_URL)"
[[ -z "${issuer_port}" ]] || export FOUNDATION_PLATFORM_IDENTITY_ISSUER_LOOPBACK_PORT="${issuer_port}"
[[ -z "${identity_api_port}" ]] || export FOUNDATION_PLATFORM_IDENTITY_API_LOOPBACK_PORT="${identity_api_port}"

compose=(
  docker compose
  --project-directory "${root_dir}"
  -f "${root_dir}/docker-compose.yml"
  -f "${root_dir}/compose.recovery.yml"
  -f "${root_dir}/compose.identity-bridge.yml"
  --project-name "${FOUNDATION_PLATFORM_COMPOSE_PROJECT}"
  --env-file "${FOUNDATION_PLATFORM_ENV_FILE}"
)

exec "${compose[@]}" "$@"
