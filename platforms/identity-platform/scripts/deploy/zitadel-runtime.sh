#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../infra/zitadel" && pwd)"
ZITADEL_PLATFORM_COMPOSE_PROJECT="${ZITADEL_PLATFORM_COMPOSE_PROJECT:-zitadel-platform}"
ZITADEL_PLATFORM_ENV_FILE="${ZITADEL_PLATFORM_ENV_FILE:-/etc/identity-platform/zitadel.env}"

if [[ "$#" -eq 0 ]]; then
  printf 'usage: zitadel-runtime.sh <docker-compose-command> [args...]\n' >&2
  exit 64
fi

if [[ ! -r "${ZITADEL_PLATFORM_ENV_FILE}" ]]; then
  printf 'Zitadel runtime environment file is not readable: %s\n' \
    "${ZITADEL_PLATFORM_ENV_FILE}" >&2
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
  --project-name "${ZITADEL_PLATFORM_COMPOSE_PROJECT}"
  --env-file "${ZITADEL_PLATFORM_ENV_FILE}"
)

exec "${compose[@]}" "$@"
