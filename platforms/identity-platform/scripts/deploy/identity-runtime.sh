#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
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

# The decided ports live in the endpoint contract and only there (root
# ADR-0081): the sidecar's issuer port and the api's host port are derived
# here, so neither the overlay nor the host env file carries a copy.
contract="${root_dir}/config/identity-runtime-endpoints.contract.json"
eval "$(python3 -c "
import json, sys
contract = json.load(open(sys.argv[1]))
print(f\"IDENTITY_ZITADEL_LOOPBACK_PORT={contract['issuer']['loopback_port']}\")
print(f\"IDENTITY_API_PORT={contract['identity_api']['loopback_port']}\")
" "${contract}")"
export IDENTITY_ZITADEL_LOOPBACK_PORT IDENTITY_API_PORT

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
