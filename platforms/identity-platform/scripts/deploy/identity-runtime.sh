#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IDENTITY_PLATFORM_COMPOSE_PROJECT="${IDENTITY_PLATFORM_COMPOSE_PROJECT:-identity-platform-runtime}"
IDENTITY_PLATFORM_ENV_FILE="${IDENTITY_PLATFORM_ENV_FILE:-/etc/identity-platform/runtime.env}"

if [[ "$#" -eq 0 ]]; then
  printf 'usage: identity-runtime.sh <docker-compose-command> [args...]\n' >&2
  exit 64
fi

if [[ ! -r "${IDENTITY_PLATFORM_ENV_FILE}" ]]; then
  printf 'Identity runtime environment file is not readable: %s\n' \
    "${IDENTITY_PLATFORM_ENV_FILE}" >&2
  exit 66
fi

# The shared network is where Zitadel, identity-api and foundation-api meet
# (root ADR-0080). Creating it here keeps every wrapper able to start first.
docker network inspect identity-shared >/dev/null 2>&1 \
  || docker network create identity-shared >/dev/null

compose=(
  docker compose
  --project-directory "${root_dir}"
  -f "${root_dir}/docker-compose.yml"
  -f "${root_dir}/compose.server.yml"
  --project-name "${IDENTITY_PLATFORM_COMPOSE_PROJECT}"
  --env-file "${IDENTITY_PLATFORM_ENV_FILE}"
)

exec "${compose[@]}" "$@"
