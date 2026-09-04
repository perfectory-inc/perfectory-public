#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
platform_root="$(cd "${script_dir}/../.." && pwd)"
root_dir="${platform_root}/infra/zitadel"
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

# The issuer's decided port lives in the endpoint contract and only there
# (root ADR-0081); the compose file requires this variable so a drifted copy
# cannot exist in it.
contract="${platform_root}/config/identity-runtime-endpoints.contract.json"
IDENTITY_ZITADEL_LOOPBACK_PORT="$(python3 -c "
import json, sys
print(json.load(open(sys.argv[1]))['issuer']['loopback_port'])
" "${contract}")"
export IDENTITY_ZITADEL_LOOPBACK_PORT

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
