#!/usr/bin/env bash
# Runs the data catalog (DataHub) on the host that runs Foundation (root ADR-0117 §7).
#
#   datahub-runtime.sh init-secrets        create the runtime env file once (0600); never prints it
#   datahub-runtime.sh provision EMAIL...  pre-create staff accounts and make them admins
#   datahub-runtime.sh <compose args...>   e.g. `up -d`, `ps`, `logs frontend-quickstart`
#
# After `up`/`restart`/`start` it waits until the UI and GMS answer and staff sign-in hands the
# browser to Zitadel, and exits 1 otherwise — a catalog whose sign-in is broken is not "up".
#
# Environment:
#   DATAHUB_RUNTIME_DIR       state directory (default /var/lib/foundation-platform/datahub):
#                             holds runtime.env and user.props, mode 0700
#   DATAHUB_OIDC_CLIENT_FILE  Zitadel client credentials written by identity-platform's
#                             configure-zitadel.sh (default
#                             /etc/identity-platform/secrets/datahub-oidc-client.env)
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
compose_dir="${root_dir}/infra/datahub"
runtime_dir="${DATAHUB_RUNTIME_DIR:-/var/lib/foundation-platform/datahub}"
env_file="${runtime_dir}/runtime.env"
user_props="${runtime_dir}/user.props"
oidc_file="${DATAHUB_OIDC_CLIENT_FILE:-/etc/identity-platform/secrets/datahub-oidc-client.env}"
ui=http://127.0.0.1:19002
gms=http://127.0.0.1:18095

fail() {
  printf 'FAIL datahub-runtime: %s\n' "$*" >&2
  exit 1
}

random_hex() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

init_secrets() {
  mkdir -p "${runtime_dir}"
  chmod 0700 "${runtime_dir}"
  umask 077
  if [[ ! -s "${env_file}" ]]; then
    {
      printf 'DATAHUB_SYSTEM_CLIENT_SECRET=%s\n' "$(random_hex)"
      printf 'DATAHUB_DB_PASSWORD=%s\n' "$(random_hex)"
      printf 'DATAHUB_FRONTEND_SECRET=%s\n' "$(random_hex)"
      printf 'DATAHUB_TOKEN_SERVICE_SIGNING_KEY=%s\n' "$(random_hex)"
      printf 'DATAHUB_TOKEN_SERVICE_SALT=%s\n' "$(random_hex)"
      printf 'UI_INGESTION_DEFAULT_CLI_VERSION=1.7.0.1\n'
    } >"${env_file}"
    printf 'created %s\n' "${env_file}"
  fi
  if [[ ! -s "${user_props}" ]]; then
    # Password sign-in is off; the file only replaces the image's well-known default account.
    printf 'datahub:%s\n' "$(random_hex)" >"${user_props}"
    chmod 0644 "${user_props}" # read inside the container by a non-root user; the dir is 0700
    printf 'created %s\n' "${user_props}"
  fi
}

compose() {
  [[ -r "${env_file}" ]] || fail "missing ${env_file}; run: $0 init-secrets"
  [[ -r "${oidc_file}" ]] || fail "missing ${oidc_file}; run identity-platform configure-zitadel.sh"
  docker network inspect identity-shared >/dev/null 2>&1 ||
    fail "the identity-shared network is missing; start identity-platform first"
  # Foundation reaches GMS here (root ADR-0119); either side may start first, so both create it.
  docker network inspect metadata-shared >/dev/null 2>&1 ||
    docker network create metadata-shared >/dev/null
  DATAHUB_USER_PROPS_FILE="${user_props}" docker compose -p datahub \
    --project-directory "${compose_dir}" \
    --env-file "${env_file}" --env-file "${oidc_file}" \
    -f "${compose_dir}/compose.yml" "$@"
}

verify_up() {
  local ui_code gms_code redirect
  for _ in $(seq 1 60); do
    ui_code="$(curl -s -o /dev/null -w '%{http_code}' "${ui}/" || true)"
    gms_code="$(curl -s -o /dev/null -w '%{http_code}' "${gms}/health" || true)"
    [[ "${ui_code}" == 200 && "${gms_code}" == 200 ]] && break
    sleep 10
  done
  [[ "${ui_code}" == 200 && "${gms_code}" == 200 ]] ||
    fail "ui=${ui_code} gms=${gms_code} after 10 minutes"
  redirect="$(curl -s -o /dev/null -w '%{redirect_url}' "${ui}/authenticate?redirect_uri=%2F" || true)"
  [[ "${redirect}" == http://127.0.0.1:18453/oauth/v2/authorize\?* ]] ||
    fail "sign-in does not hand the browser to Zitadel (got '${redirect}')"
  printf 'datahub-runtime: ui and gms answer; sign-in goes to Zitadel\n'
}

provision() {
  (($# > 0)) || fail "usage: $0 provision EMAIL..."
  local email body code role
  for email in "$@"; do
    body="$(EMAIL="${email}" python3 -c '
import json, os
e = os.environ["EMAIL"]
info = {"active": True, "displayName": e, "email": e}
print(json.dumps({"proposal": {
    "entityType": "corpuser", "entityUrn": "urn:li:corpuser:" + e, "changeType": "UPSERT",
    "aspectName": "corpUserInfo",
    "aspect": {"contentType": "application/json", "value": json.dumps(info)}}}))')"
    code="$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
      -H 'X-RestLi-Protocol-Version: 2.0.0' -d "${body}" "${gms}/aspects?action=ingestProposal")"
    [[ "${code}" == 200 ]] || fail "creating ${email} returned ${code}"
    role="$(EMAIL="${email}" python3 -c '
import json, os
urn = "urn:li:corpuser:" + os.environ["EMAIL"]
print(json.dumps({"query": "mutation($u:[String!]!){batchAssignRole(input:{roleUrn:\"urn:li:dataHubRole:Admin\",actors:$u})}",
                  "variables": {"u": [urn]}}))' |
      curl -s -H 'Content-Type: application/json' --data-binary @- "${gms}/api/graphql")"
    [[ "${role}" == *'"batchAssignRole":true'* ]] || fail "making ${email} an admin returned ${role}"
    printf 'provisioned %s as admin\n' "${email}"
  done
}

(($# > 0)) || fail "usage: $0 init-secrets | provision EMAIL... | <docker compose args>"
case "$1" in
  init-secrets) init_secrets ;;
  provision)
    shift
    provision "$@"
    ;;
  *)
    compose "$@"
    case "$1" in
      up | restart | start) verify_up ;;
    esac
    ;;
esac
