#!/usr/bin/env bash
set -Eeuo pipefail
set +x

# Disposable cross-repository Task 9 smoke. This script provisions all credentials and state at
# runtime, prints only assertion status/counts, and removes every resource on exit.

POSTGRES_IMAGE="postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
BUSYBOX_IMAGE="busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
ZITADEL_IMAGE="ghcr.io/zitadel/zitadel:v2.65.1@sha256:013d23b69aa681f03d36a7fd61e4837a7b049a7e22bd7215eb3e98e9dbf5543c"
PYTHON_IMAGE="python:3.11-bookworm@sha256:b7ae8a4dcc0ab327e333c5e46a3eaa6c1b0ff585bed77e01cd6de4be1325837e"
CURL_CONNECT_TIMEOUT_SECONDS="${CURL_CONNECT_TIMEOUT_SECONDS:-5}"
CURL_MAX_TIME_SECONDS="${CURL_MAX_TIME_SECONDS:-30}"
DOCKER_COMMAND_TIMEOUT_SECONDS="${DOCKER_COMMAND_TIMEOUT_SECONDS:-120}"
STACK_COMMAND_TIMEOUT_SECONDS="${STACK_COMMAND_TIMEOUT_SECONDS:-900}"
PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"
LOGIN_PAGE_ATTEMPTS="${LOGIN_PAGE_ATTEMPTS:-5}"
DEVICE_INTERVAL_CAP_SECONDS="${DEVICE_INTERVAL_CAP_SECONDS:-5}"
MACHINE_TOKEN_ATTEMPTS="${MACHINE_TOKEN_ATTEMPTS:-20}"
ZITADEL_PROJECTION_ATTEMPTS="${ZITADEL_PROJECTION_ATTEMPTS:-20}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_FOUNDATION="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDENTITY_CHECKOUT="${IDENTITY_CHECKOUT:-${1:-}}"
FOUNDATION_CHECKOUT="${FOUNDATION_CHECKOUT:-${2:-${DEFAULT_FOUNDATION}}}"

if [[ -z "${IDENTITY_CHECKOUT}" ]]; then
  printf 'FAIL stage=configuration reason=identity_checkout_required\n' >&2
  exit 2
fi

IDENTITY_CHECKOUT="$(cd "${IDENTITY_CHECKOUT}" && pwd)"
FOUNDATION_CHECKOUT="$(cd "${FOUNDATION_CHECKOUT}" && pwd)"
for required in \
  "${IDENTITY_CHECKOUT}/docker-compose.yml" \
  "${IDENTITY_CHECKOUT}/scripts/compose-smoke.sh" \
  "${FOUNDATION_CHECKOUT}/docker-compose.yml" \
  "${FOUNDATION_CHECKOUT}/scripts/compose-smoke.sh"; do
  if [[ ! -f "${required}" ]]; then
    printf 'FAIL stage=configuration reason=checkout_contract_missing\n' >&2
    exit 2
  fi
done

for command in docker curl python3 openssl sed grep mktemp timeout; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    printf 'FAIL stage=configuration reason=missing_command command=%s\n' "${command}" >&2
    exit 2
  fi
done

validate_bounded_integer() {
  local name=$1 value=$2 minimum=$3 maximum=$4
  if [[ ! "${value}" =~ ^[0-9]+$ ]] \
    || ((10#${value} < minimum || 10#${value} > maximum)); then
    printf 'FAIL stage=configuration reason=invalid_bounded_integer setting=%s\n' "${name}" >&2
    exit 2
  fi
}

validate_bounded_integer CURL_CONNECT_TIMEOUT_SECONDS "${CURL_CONNECT_TIMEOUT_SECONDS}" 1 30
validate_bounded_integer CURL_MAX_TIME_SECONDS "${CURL_MAX_TIME_SECONDS}" 1 60
validate_bounded_integer DOCKER_COMMAND_TIMEOUT_SECONDS "${DOCKER_COMMAND_TIMEOUT_SECONDS}" 1 300
validate_bounded_integer STACK_COMMAND_TIMEOUT_SECONDS "${STACK_COMMAND_TIMEOUT_SECONDS}" 60 1800
validate_bounded_integer PGCONNECT_TIMEOUT "${PGCONNECT_TIMEOUT}" 1 30
validate_bounded_integer LOGIN_PAGE_ATTEMPTS "${LOGIN_PAGE_ATTEMPTS}" 1 10
validate_bounded_integer DEVICE_INTERVAL_CAP_SECONDS "${DEVICE_INTERVAL_CAP_SECONDS}" 1 10
validate_bounded_integer MACHINE_TOKEN_ATTEMPTS "${MACHINE_TOKEN_ATTEMPTS}" 1 60
validate_bounded_integer ZITADEL_PROJECTION_ATTEMPTS "${ZITADEL_PROJECTION_ATTEMPTS}" 1 60

bounded_docker() {
  timeout --foreground "${DOCKER_COMMAND_TIMEOUT_SECONDS}s" docker "$@"
}

run_id="$(date +%s)-${RANDOM}"
resource_id="task9${run_id//[^0-9]/}"
network="${resource_id}-net"
zitadel_db_container="${resource_id}-zitadel-db"
zitadel_container="${resource_id}-zitadel"
zitadel_host_relay="${resource_id}-zitadel-host-relay"
zitadel_db_volume="${resource_id}-zitadel-db"
identity_zitadel_relay="${resource_id}-identity-zitadel-relay"
foundation_zitadel_relay="${resource_id}-foundation-zitadel-relay"
foundation_identity_relay="${resource_id}-foundation-identity-relay"
identity_provisioner_container="${resource_id}-identity-provisioner"
identity_cross_probe_container="${resource_id}-identity-cross-probe"
foundation_cross_probe_container="${resource_id}-foundation-cross-probe"
identity_project="${resource_id}i"
foundation_project="${resource_id}f"
identity_runtime_image="${resource_id}-identity-runtime:task9"
identity_worker_image="${resource_id}-identity-worker:task9"
foundation_runtime_image="${resource_id}-foundation-runtime:task9"
zitadel_internal_port=8080
temp_root="${TASK9_SMOKE_TEMP_ROOT:-${FOUNDATION_CHECKOUT}/target}"
mkdir -p "${temp_root}"
temp_dir="$(mktemp -d "${temp_root%/}/task9-smoke.XXXXXX")"
chmod 700 "${temp_dir}"
mkdir "${temp_dir}/docker-config"
printf '{"auths":{}}' > "${temp_dir}/docker-config/config.json"
export DOCKER_CONFIG="${temp_dir}/docker-config"
export PGCONNECT_TIMEOUT
bounded_docker compose version >/dev/null

stage="initialization"
identity_started=0
foundation_started=0

force_remove_compose_project() {
  local project=$1
  local resource

  while IFS= read -r resource; do
    [[ -n "${resource}" ]] && bounded_docker rm -f "${resource}" >/dev/null 2>&1
  done < <(bounded_docker ps -aq --filter "label=com.docker.compose.project=${project}")
  while IFS= read -r resource; do
    [[ -n "${resource}" ]] && bounded_docker volume rm -f "${resource}" >/dev/null 2>&1
  done < <(bounded_docker volume ls -q --filter "label=com.docker.compose.project=${project}")
  while IFS= read -r resource; do
    [[ -n "${resource}" ]] && bounded_docker network rm "${resource}" >/dev/null 2>&1
  done < <(bounded_docker network ls -q --filter "label=com.docker.compose.project=${project}")
}

task9_resources_remain() {
  local resources image
  if ! resources="$(bounded_docker ps -aq --filter "name=${resource_id}")"; then
    return 0
  fi
  [[ -n "${resources}" ]] && return 0
  if ! resources="$(bounded_docker volume ls -q --filter "name=${resource_id}")"; then
    return 0
  fi
  [[ -n "${resources}" ]] && return 0
  if ! resources="$(bounded_docker network ls -q --filter "name=${resource_id}")"; then
    return 0
  fi
  [[ -n "${resources}" ]] && return 0
  for image in "${identity_runtime_image}" "${identity_worker_image}" "${foundation_runtime_image}"; do
    if bounded_docker image inspect "${image}" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}

cleanup() {
  local rc=$?
  local cleanup_failed=0 container
  set +e
  if [[ ${foundation_started} -eq 1 ]]; then
    timeout --foreground 120s docker compose --env-file "${temp_dir}/foundation.env" \
      -p "${foundation_project}" \
      -f "${FOUNDATION_CHECKOUT}/docker-compose.yml" \
      -f "${temp_dir}/foundation.override.yml" \
      down --volumes --remove-orphans >/dev/null 2>&1
    force_remove_compose_project "${foundation_project}"
  fi
  if [[ ${identity_started} -eq 1 ]]; then
    timeout --foreground 120s docker compose --env-file "${temp_dir}/identity.env" \
      -p "${identity_project}" \
      -f "${IDENTITY_CHECKOUT}/docker-compose.yml" \
      -f "${temp_dir}/identity.override.yml" \
      down --volumes --remove-orphans >/dev/null 2>&1
    force_remove_compose_project "${identity_project}"
  fi
  for container in \
    "${identity_zitadel_relay}" \
    "${foundation_zitadel_relay}" \
    "${foundation_identity_relay}" \
    "${identity_provisioner_container}" \
    "${identity_cross_probe_container}" \
    "${foundation_cross_probe_container}" \
    "${zitadel_host_relay}" \
    "${zitadel_container}" \
    "${zitadel_db_container}"; do
    bounded_docker rm -f "${container}" >/dev/null 2>&1 || true
  done
  bounded_docker volume rm -f "${zitadel_db_volume}" >/dev/null 2>&1
  bounded_docker network rm "${network}" >/dev/null 2>&1
  bounded_docker image rm -f "${identity_runtime_image}" "${identity_worker_image}" \
    "${foundation_runtime_image}" >/dev/null 2>&1
  if task9_resources_remain; then
    cleanup_failed=1
  fi
  rm -rf "${temp_dir}" || cleanup_failed=1
  if [[ "${cleanup_failed}" != "0" ]]; then
    printf 'FAIL stage=cleanup_residual_resources\n' >&2
    [[ ${rc} -eq 0 ]] && rc=1
  fi
  if [[ ${rc} -eq 0 && "${cleanup_failed}" == "0" && "${stage}" == "complete" ]]; then
    printf 'smoke=PASS assertions=8 issuer=disposable-local tokens=RS256 databases=2\n'
  elif [[ "${stage}" != "complete" ]]; then
    printf 'FAIL stage=%s\n' "${stage}" >&2
  fi
  exit "${rc}"
}
trap cleanup EXIT

parse_host_port() {
  local endpoint=$1
  local port="${endpoint##*:}"
  if [[ ! "${port}" =~ ^[0-9]+$ ]] || ((10#${port} < 1 || 10#${port} > 65535)); then
    return 1
  fi
  printf '%d' "$((10#${port}))"
}

random_hex() {
  openssl rand -hex "$1"
}

bounded_curl() {
  command curl --connect-timeout "${CURL_CONNECT_TIMEOUT_SECONDS}" \
    --max-time "${CURL_MAX_TIME_SECONDS}" "$@"
}

json_get() {
  local file=$1
  local path=$2
  python3 -c '
import json, sys
value = json.load(sys.stdin)
for key in sys.argv[1].split("."):
    value = value[int(key)] if isinstance(value, list) else value[key]
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (dict, list)):
    print(json.dumps(value, separators=(",", ":")))
else:
    print(value)
' "${path}" < "${file}"
}

html_input() {
  local file=$1
  local name=$2
  python3 -c '
from html.parser import HTMLParser
import sys
class Inputs(HTMLParser):
    def __init__(self):
        super().__init__(); self.value = None
    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "input" and values.get("name") == sys.argv[1]:
            self.value = values.get("value", "")
p = Inputs(); p.feed(sys.stdin.read())
if p.value is None: raise SystemExit(1)
print(p.value)
' "${name}" < "${file}"
}

html_shape() {
  local file=$1
  python3 -c '
from html.parser import HTMLParser
from urllib.parse import urlsplit
import sys
class Shape(HTMLParser):
    def __init__(self):
        super().__init__(); self.actions = []; self.inputs = []
    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "form": self.actions.append(urlsplit(values.get("action", "none")).path)
        if tag == "input" and values.get("name"): self.inputs.append(values["name"])
p = Shape(); p.feed(sys.stdin.read())
value = "forms_" + "_".join(p.actions or ["none"]) + "_inputs_" + "_".join(p.inputs or ["none"])
print("".join(c if c.isalnum() or c in "_-" else "_" for c in value))
' < "${file}"
}

write_bearer_config() {
  local token_file=$1
  local config_file=$2
  printf 'header = "Authorization: Bearer %s"\n' "$(<"${token_file}")" > "${config_file}"
  chmod 600 "${config_file}"
}

expect_status() {
  local expected=$1
  local actual=$2
  [[ "${actual}" == "${expected}" ]]
}

validate_device_interval() {
  local value=$1
  if [[ ! "${value}" =~ ^[0-9]+$ ]] || ((10#${value} < 1)); then
    return 1
  fi
  if ((10#${value} > DEVICE_INTERVAL_CAP_SECONDS)); then
    printf '%s' "${DEVICE_INTERVAL_CAP_SECONDS}"
  else
    printf '%d' "$((10#${value}))"
  fi
}

identity_port=0
foundation_port=0
identity_relay_port=19081
zitadel_port=0
issuer=""
zitadel_resolve=""

zitadel_db_password="$(random_hex 24)"
zitadel_master_key="$(random_hex 16)"
human_password="Aa1!$(random_hex 18)"
identity_admin_password="$(random_hex 24)"
identity_migrator_password="$(random_hex 24)"
identity_api_password="$(random_hex 24)"
identity_worker_password="$(random_hex 24)"
foundation_admin_password="$(random_hex 24)"
foundation_migrator_password="$(random_hex 24)"
foundation_api_password="$(random_hex 24)"

printf '%s' "${human_password}" > "${temp_dir}/human-password"
chmod 600 "${temp_dir}/human-password"

stage="zitadel_start"
bounded_docker network create "${network}" >/dev/null
bounded_docker volume create "${zitadel_db_volume}" >/dev/null
cat > "${temp_dir}/tcp-relay.py" <<'PY'
import asyncio
import os

LISTEN_PORT = int(os.environ["LISTEN_PORT"])
TARGET_HOST = os.environ["TARGET_HOST"]
TARGET_PORT = int(os.environ["TARGET_PORT"])


async def pump(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        try:
            writer.write_eof()
        except (AttributeError, OSError):
            pass


async def relay(client_reader, client_writer):
    try:
        target_reader, target_writer = await asyncio.open_connection(TARGET_HOST, TARGET_PORT)
    except OSError:
        client_writer.close()
        await client_writer.wait_closed()
        return
    await asyncio.gather(
        pump(client_reader, target_writer),
        pump(target_reader, client_writer),
        return_exceptions=True,
    )
    target_writer.close()
    client_writer.close()
    await asyncio.gather(
        target_writer.wait_closed(),
        client_writer.wait_closed(),
        return_exceptions=True,
    )


async def main():
    server = await asyncio.start_server(relay, "0.0.0.0", LISTEN_PORT)
    async with server:
        await server.serve_forever()


asyncio.run(main())
PY
chmod 644 "${temp_dir}/tcp-relay.py"
bounded_docker run -d --name "${zitadel_host_relay}" --label "task9.run_id=${resource_id}" \
  --user 65534:65534 --network "${network}" -p "127.0.0.1::${zitadel_internal_port}" \
  -e LISTEN_PORT="${zitadel_internal_port}" -e TARGET_HOST=task9-zitadel \
  -e TARGET_PORT="${zitadel_internal_port}" \
  -v "${temp_dir}/tcp-relay.py:/run/task9/tcp-relay.py:ro" \
  "${PYTHON_IMAGE}" python3 /run/task9/tcp-relay.py >/dev/null
zitadel_host_endpoint="$(bounded_docker port "${zitadel_host_relay}" \
  "${zitadel_internal_port}/tcp" | tr -d '\r' | tail -n 1)"
if ! zitadel_port="$(parse_host_port "${zitadel_host_endpoint}")"; then
  stage="zitadel_host_relay_dynamic_port_invalid"
  exit 1
fi
issuer="http://127.0.0.1:${zitadel_port}"
zitadel_resolve="127.0.0.1:${zitadel_port}:127.0.0.1"
: > "${temp_dir}/admin.pat"
chmod 666 "${temp_dir}/admin.pat"

cat > "${temp_dir}/zitadel-db.env" <<EOF
POSTGRES_USER=zitadel
POSTGRES_PASSWORD=${zitadel_db_password}
POSTGRES_DB=zitadel
EOF
write_zitadel_env() {
  cat > "${temp_dir}/zitadel.env" <<EOF
ZITADEL_MASTERKEY=${zitadel_master_key}
ZITADEL_PORT=${zitadel_internal_port}
ZITADEL_DATABASE_POSTGRES_HOST=${zitadel_db_container}
ZITADEL_DATABASE_POSTGRES_PORT=5432
ZITADEL_DATABASE_POSTGRES_DATABASE=zitadel
ZITADEL_DATABASE_POSTGRES_USER_USERNAME=zitadel
ZITADEL_DATABASE_POSTGRES_USER_PASSWORD=${zitadel_db_password}
ZITADEL_DATABASE_POSTGRES_USER_SSL_MODE=disable
ZITADEL_DATABASE_POSTGRES_ADMIN_USERNAME=zitadel
ZITADEL_DATABASE_POSTGRES_ADMIN_PASSWORD=${zitadel_db_password}
ZITADEL_DATABASE_POSTGRES_ADMIN_SSL_MODE=disable
ZITADEL_EXTERNALSECURE=false
ZITADEL_EXTERNALDOMAIN=127.0.0.1
ZITADEL_EXTERNALPORT=${zitadel_port}
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_USERNAME=bootstrap-admin
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_PASSWORD=${human_password}
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_PASSWORDCHANGEREQUIRED=false
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_FIRSTNAME=Bootstrap
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_LASTNAME=Admin
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_EMAIL_ADDRESS=bootstrap-admin@smoke.invalid
ZITADEL_FIRSTINSTANCE_ORG_HUMAN_EMAIL_VERIFIED=true
ZITADEL_FIRSTINSTANCE_PATPATH=/pat-out/pat
ZITADEL_FIRSTINSTANCE_ORG_MACHINE_MACHINE_USERNAME=task9-admin
ZITADEL_FIRSTINSTANCE_ORG_MACHINE_MACHINE_NAME=Task9Admin
ZITADEL_FIRSTINSTANCE_ORG_MACHINE_PAT_EXPIRATIONDATE=2099-01-01T00:00:00Z
EOF
}
write_zitadel_env
chmod 600 "${temp_dir}/zitadel-db.env" "${temp_dir}/zitadel.env"

bounded_docker run -d --name "${zitadel_db_container}" --network "${network}" \
  --env-file "${temp_dir}/zitadel-db.env" \
  -v "${zitadel_db_volume}:/var/lib/postgresql/data" \
  "${POSTGRES_IMAGE}" >/dev/null
for _ in $(seq 1 90); do
  if bounded_docker exec -e PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT}" \
    "${zitadel_db_container}" pg_isready -U zitadel -d zitadel >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
bounded_docker exec -e PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT}" \
  "${zitadel_db_container}" pg_isready -U zitadel -d zitadel >/dev/null

bounded_docker run -d --name "${zitadel_container}" --user 1000:1000 --network "${network}" \
  --network-alias task9-zitadel \
  --env-file "${temp_dir}/zitadel.env" \
  -v "${temp_dir}/admin.pat:/pat-out/pat" \
  "${ZITADEL_IMAGE}" start-from-init --masterkeyFromEnv --tlsMode disabled >/dev/null
for _ in $(seq 1 300); do
  if bounded_curl --silent --fail --resolve "${zitadel_resolve}" \
    "${issuer}/debug/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
bounded_curl --silent --show-error --fail --resolve "${zitadel_resolve}" \
  "${issuer}/debug/healthz" >/dev/null

chmod 600 "${temp_dir}/admin.pat"
write_bearer_config "${temp_dir}/admin.pat" "${temp_dir}/admin.curl"

zcurl() {
  bounded_curl --silent --show-error --fail --resolve "${zitadel_resolve}" "$@"
}

projection_not_found() {
  local response=$1
  grep -Fq 'Errors.User.NotFound' "${response}" \
    || grep -Fq 'Errors.Project.NotFound' "${response}"
}

zcurl_projection_retry() {
  local output=$1
  shift
  local attempt status curl_status category
  for attempt in $(seq 1 "${ZITADEL_PROJECTION_ATTEMPTS}"); do
    set +e
    status="$(bounded_curl --silent --show-error --resolve "${zitadel_resolve}" \
      -o "${output}" -w '%{http_code}' "$@")"
    curl_status=$?
    set -e
    if [[ ${curl_status} -ne 0 ]]; then
      stage="${stage}_curl_exit_${curl_status}"
      return 1
    fi
    [[ "${status}" =~ ^2[0-9][0-9]$ ]] && return 0
    if [[ "${status}" == "404" \
      && ${attempt} -lt ${ZITADEL_PROJECTION_ATTEMPTS} ]] \
      && projection_not_found "${output}"; then
      sleep 1
      continue
    fi
    break
  done
  category="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
    detail = value.get("message") or value.get("error_description") or "unknown"
except Exception:
    detail = "non_json"
print("".join(c for c in str(detail) if c.isalnum() or c in "_-")[:80])
' "${output}")"
  stage="${stage}_http_${status}_${category}"
  return 1
}

stage="zitadel_management_ready"
printf '{}' > "${temp_dir}/search.json"
management_status=000
for _ in $(seq 1 30); do
  management_status="$(bounded_curl --silent --show-error --resolve "${zitadel_resolve}" \
    --config "${temp_dir}/admin.curl" -o "${temp_dir}/search-response.json" \
    -w '%{http_code}' -H 'Content-Type: application/json' \
    --data-binary @"${temp_dir}/search.json" \
    "${issuer}/management/v1/projects/_search")"
  if [[ "${management_status}" == "200" ]]; then
    break
  fi
  sleep 1
done
expect_status 200 "${management_status}"

stage="zitadel_provision_project"
printf '{"name":"task9-cross-platform-smoke"}' > "${temp_dir}/project.json"
zcurl --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/project.json" \
  "${issuer}/management/v1/projects" > "${temp_dir}/project-response.json"
project_id="$(json_get "${temp_dir}/project-response.json" id)"

stage="zitadel_provision_role"
printf '{"roleKey":"smoke_access","displayName":"Smoke access","group":"task9"}' \
  > "${temp_dir}/role.json"
zcurl_projection_retry "${temp_dir}/project-role-response.json" \
  --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/role.json" \
  "${issuer}/management/v1/projects/${project_id}/roles"

stage="zitadel_provision_oidc_app"
cat > "${temp_dir}/oidc-app.json" <<'EOF'
{"name":"task9-device","redirectUris":[],"postLogoutRedirectUris":[],"responseTypes":["OIDC_RESPONSE_TYPE_CODE"],"grantTypes":["OIDC_GRANT_TYPE_DEVICE_CODE"],"appType":"OIDC_APP_TYPE_NATIVE","authMethodType":"OIDC_AUTH_METHOD_TYPE_NONE","version":"OIDC_VERSION_1_0","devMode":true,"accessTokenType":"OIDC_TOKEN_TYPE_JWT","idTokenRoleAssertion":true,"accessTokenRoleAssertion":true,"skipNativeAppSuccessPage":true}
EOF
zcurl_projection_retry "${temp_dir}/oidc-app-response.json" \
  --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/oidc-app.json" \
  "${issuer}/management/v1/projects/${project_id}/apps/oidc"
device_client_id="$(json_get "${temp_dir}/oidc-app-response.json" clientId)"

stage="zitadel_provision_action"
cat > "${temp_dir}/action.json" <<'EOF'
{"name":"principalKind","script":"function principalKind(ctx, api) { var user = ctx.v1.getUser(); var kind = (user !== undefined && user.machine !== undefined) ? \"service\" : \"staff\"; api.v1.claims.setClaim(\"principal_kind\", kind); }","timeout":"10s","allowedToFail":false}
EOF
zcurl --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/action.json" \
  "${issuer}/management/v1/actions" > "${temp_dir}/action-response.json"
action_id="$(json_get "${temp_dir}/action-response.json" id)"
stage="zitadel_provision_action_flow"
printf '{"actionIds":["%s"]}' "${action_id}" > "${temp_dir}/flow.json"
zcurl --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/flow.json" \
  "${issuer}/management/v1/flows/2/trigger/5" >/dev/null

create_human() {
  local username=$1
  local output=$2
  stage="zitadel_provision_human_${username}"
  python3 -c '
import json, pathlib, sys
username, password_path = sys.argv[1:]
password = pathlib.Path(password_path).read_text()
json.dump({
  "userName": username,
  "profile": {"firstName": username, "lastName": "Smoke"},
  "email": {"email": username + "@smoke.invalid", "isEmailVerified": True},
  "password": password,
  "passwordChangeRequired": False,
}, sys.stdout, separators=(",", ":"))
' "${username}" "${temp_dir}/human-password" > "${temp_dir}/${username}.json"
  zcurl --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
    --data-binary @"${temp_dir}/${username}.json" \
    "${issuer}/management/v1/users/human/_import" > "${output}"
  local subject
  subject="$(json_get "${output}" userId)"
  printf '{"projectId":"%s","roleKeys":["smoke_access"]}' "${project_id}" \
    > "${temp_dir}/${username}-grant.json"
  zcurl_projection_retry "${temp_dir}/${username}-grant-response.json" \
    --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
    --data-binary @"${temp_dir}/${username}-grant.json" \
    "${issuer}/management/v1/users/${subject}/grants"
}

create_human master "${temp_dir}/master-user.json"
create_human catalog "${temp_dir}/catalog-user.json"
create_human viewer "${temp_dir}/viewer-user.json"
master_subject="$(json_get "${temp_dir}/master-user.json" userId)"
catalog_subject="$(json_get "${temp_dir}/catalog-user.json" userId)"
viewer_subject="$(json_get "${temp_dir}/viewer-user.json" userId)"

stage="zitadel_provision_machine"
cat > "${temp_dir}/machine.json" <<'EOF'
{"userName":"intelligence-smoke","name":"Intelligence Smoke","description":"Disposable Task 9 service identity","accessTokenType":"ACCESS_TOKEN_TYPE_JWT"}
EOF
zcurl --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/machine.json" \
  "${issuer}/management/v1/users/machine" > "${temp_dir}/machine-response.json"
machine_subject="$(json_get "${temp_dir}/machine-response.json" userId)"
stage="zitadel_provision_machine_grant"
printf '{"projectId":"%s","roleKeys":["smoke_access"]}' "${project_id}" \
  > "${temp_dir}/machine-grant.json"
zcurl_projection_retry "${temp_dir}/machine-grant-response.json" \
  --config "${temp_dir}/admin.curl" -H 'Content-Type: application/json' \
  --data-binary @"${temp_dir}/machine-grant.json" \
  "${issuer}/management/v1/users/${machine_subject}/grants"
stage="zitadel_provision_machine_secret"
zcurl_projection_retry "${temp_dir}/machine-secret.json" \
  --config "${temp_dir}/admin.curl" -X PUT \
  "${issuer}/management/v1/users/${machine_subject}/secret"
machine_client_id="$(json_get "${temp_dir}/machine-secret.json" clientId)"
machine_client_secret="$(json_get "${temp_dir}/machine-secret.json" clientSecret)"
printf 'user = "%s:%s"\n' "${machine_client_id}" "${machine_client_secret}" \
  > "${temp_dir}/machine-basic.curl"
chmod 600 "${temp_dir}/machine-basic.curl"

stage="zitadel_provision_machine_token"
scope="openid urn:zitadel:iam:org:project:id:${project_id}:aud urn:zitadel:iam:org:project:roles"
machine_token_status=000
for machine_token_attempt in $(seq 1 "${MACHINE_TOKEN_ATTEMPTS}"); do
  set +e
  machine_token_status="$(bounded_curl --silent --show-error --resolve "${zitadel_resolve}" \
    --config "${temp_dir}/machine-basic.curl" \
    -o "${temp_dir}/machine-token-response.json" -w '%{http_code}' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=client_credentials' \
    --data-urlencode "scope=${scope}" \
    "${issuer}/oauth/v2/token")"
  machine_token_curl_status=$?
  set -e
  if [[ ${machine_token_curl_status} -ne 0 ]]; then
    stage="zitadel_provision_machine_token_curl_exit_${machine_token_curl_status}"
    exit 1
  fi
  [[ "${machine_token_status}" == "200" ]] && break
  machine_token_detail_raw="$(python3 -c '
import json, sys
try:
    print(str(json.load(open(sys.argv[1], encoding="utf-8")).get("error_description", "")))
except Exception:
    print("")
' "${temp_dir}/machine-token-response.json")"
  if [[ "${machine_token_status}" == "400" \
    && "${machine_token_detail_raw}" == *"Errors.User.Machine.Secret.NotExisting"* \
    && ${machine_token_attempt} -lt ${MACHINE_TOKEN_ATTEMPTS} ]]; then
    sleep 1
    continue
  fi
  break
done
stage="zitadel_provision_machine_token_http_${machine_token_status}"
if [[ "${machine_token_status}" != "200" ]]; then
  machine_token_error="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8")).get("error", "unknown")
except Exception:
    value = "non_json"
print("".join(c for c in str(value) if c.isalnum() or c in "_-"))
' "${temp_dir}/machine-token-response.json")"
  machine_token_detail="$(python3 -c '
import json, re, sys
try:
    value = str(json.load(open(sys.argv[1], encoding="utf-8")).get("error_description", "unknown"))
except Exception:
    value = "non_json"
value = re.sub(r"[A-Za-z0-9_-]{16,}", "REDACTED", value)
print("".join(c if c.isalpha() or c in "._-" else "_" for c in value)[:160])
' "${temp_dir}/machine-token-response.json")"
  stage="zitadel_provision_machine_token_http_${machine_token_status}_error_${machine_token_error}_${machine_token_detail}"
  exit 1
fi
stage="zitadel_provision_machine_token_parse"
if ! json_get "${temp_dir}/machine-token-response.json" access_token > "${temp_dir}/machine.token"; then
  machine_token_keys="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
    print("_".join(sorted(str(key) for key in value.keys())))
except Exception:
    print("non_json")
' "${temp_dir}/machine-token-response.json")"
  stage="zitadel_provision_machine_token_parse_keys_${machine_token_keys}"
  exit 1
fi
chmod 600 "${temp_dir}/machine.token"

mint_staff_token() {
  local username=$1
  local subject=$2
  local token_file=$3
  local prefix="${temp_dir}/${username}-device"
  local csrf auth_request login_page_status login_page_curl_status
  local verification_uri device_code device_interval login_page_attempt

  for login_page_attempt in $(seq 1 "${LOGIN_PAGE_ATTEMPTS}"); do
    stage="zitadel_staff_${username}_device_authorization_attempt_${login_page_attempt}"
    zcurl -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode "client_id=${device_client_id}" \
      --data-urlencode "scope=${scope}" \
      "${issuer}/oauth/v2/device_authorization" > "${prefix}.json"
    verification_uri="$(json_get "${prefix}.json" verification_uri_complete)"
    device_code="$(json_get "${prefix}.json" device_code)"
    device_interval="$(validate_device_interval "$(json_get "${prefix}.json" interval)")" || {
      stage="zitadel_staff_${username}_invalid_device_interval"
      exit 1
    }

    rm -f "${prefix}.cookies"
    stage="zitadel_staff_${username}_login_page_attempt_${login_page_attempt}"
    set +e
    login_page_status="$(bounded_curl --silent --show-error --resolve "${zitadel_resolve}" -L \
      -c "${prefix}.cookies" -b "${prefix}.cookies" -o "${prefix}-login.html" \
      -w '%{http_code}' "${verification_uri}")"
    login_page_curl_status=$?
    set -e
    if [[ ${login_page_curl_status} -eq 0 && "${login_page_status}" == "200" ]] \
      && csrf="$(html_input "${prefix}-login.html" gorilla.csrf.Token)" \
      && auth_request="$(html_input "${prefix}-login.html" authRequestID)"; then
      break
    fi
    if [[ ${login_page_attempt} -eq ${LOGIN_PAGE_ATTEMPTS} ]]; then
      if [[ ${login_page_curl_status} -ne 0 || "${login_page_status}" != "200" ]]; then
        stage="zitadel_staff_${username}_login_page_http_${login_page_status}_curl_${login_page_curl_status}"
      else
        stage="zitadel_staff_${username}_login_page_shape_$(html_shape "${prefix}-login.html")"
      fi
      exit 1
    fi
    sleep 1
  done
  stage="zitadel_staff_${username}_login_name"
  zcurl -L -c "${prefix}.cookies" -b "${prefix}.cookies" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "gorilla.csrf.Token=${csrf}" \
    --data-urlencode "authRequestID=${auth_request}" \
    --data-urlencode "loginName=${username}" \
    "${issuer}/ui/login/loginname" > "${prefix}-password.html"

  if ! csrf="$(html_input "${prefix}-password.html" gorilla.csrf.Token)"; then
    stage="zitadel_staff_${username}_password_page_shape_$(html_shape "${prefix}-password.html")"
    exit 1
  fi
  stage="zitadel_staff_${username}_password"
  zcurl -L -c "${prefix}.cookies" -b "${prefix}.cookies" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "gorilla.csrf.Token=${csrf}" \
    --data-urlencode "authRequestID=${auth_request}" \
    --data-urlencode "loginName=${username}" \
    --data-urlencode "password@${temp_dir}/human-password" \
    "${issuer}/ui/login/password" > "${prefix}-mfa.html"

  if ! csrf="$(html_input "${prefix}-mfa.html" gorilla.csrf.Token)"; then
    stage="zitadel_staff_${username}_mfa_page_shape_$(html_shape "${prefix}-mfa.html")"
    exit 1
  fi
  stage="zitadel_staff_${username}_mfa"
  zcurl -L -c "${prefix}.cookies" -b "${prefix}.cookies" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "gorilla.csrf.Token=${csrf}" \
    --data-urlencode "authRequestID=${auth_request}" \
    --data-urlencode 'skip=true' \
    "${issuer}/ui/login/mfa/prompt" > "${prefix}-consent.html"

  if ! csrf="$(html_input "${prefix}-consent.html" gorilla.csrf.Token)"; then
    stage="zitadel_staff_${username}_consent_page_shape_$(html_shape "${prefix}-consent.html")"
    exit 1
  fi
  stage="zitadel_staff_${username}_device_allowed"
  zcurl -L -c "${prefix}.cookies" -b "${prefix}.cookies" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "gorilla.csrf.Token=${csrf}" \
    --data-urlencode "authRequestID=${auth_request}" \
    "${issuer}/ui/login/device/allowed" > "${prefix}-allowed.html"

  local token_status token_error consent_shape allowed_shape
  stage="zitadel_staff_${username}_token_poll"
  for _ in $(seq 1 20); do
    sleep "${device_interval}"
    token_status="$(bounded_curl --silent --show-error --resolve "${zitadel_resolve}" \
      -o "${prefix}-token.json" -w '%{http_code}' \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:device_code' \
      --data-urlencode "device_code=${device_code}" \
      --data-urlencode "client_id=${device_client_id}" \
      "${issuer}/oauth/v2/token")"
    if [[ "${token_status}" == "200" ]]; then
      break
    fi
  done
  if [[ "${token_status}" != "200" ]]; then
    token_error="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8")).get("error", "unknown")
except Exception:
    value = "non_json"
print("".join(c for c in str(value) if c.isalnum() or c in "_-"))
' "${prefix}-token.json")"
    consent_shape="$(html_shape "${prefix}-consent.html")"
    allowed_shape="$(html_shape "${prefix}-allowed.html")"
    stage="zitadel_staff_${username}_token_poll_http_${token_status}_error_${token_error}_${consent_shape}_${allowed_shape}"
    exit 1
  fi
  json_get "${prefix}-token.json" access_token > "${token_file}"
  chmod 600 "${token_file}"

  stage="zitadel_staff_${username}_token_validate"
  python3 -c '
import base64, json, pathlib, sys, time
token_path, issuer, audience, kind, subject = sys.argv[1:]
token = pathlib.Path(token_path).read_text().strip()
parts = token.split(".")
assert len(parts) == 3
decode = lambda value: json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
header, claims = decode(parts[0]), decode(parts[1])
assert header.get("alg") == "RS256" and header.get("kid")
assert claims.get("iss") == issuer
aud = claims.get("aud", [])
assert audience in ([aud] if isinstance(aud, str) else aud)
assert claims.get("principal_kind") == kind
assert claims.get("sub") == subject
assert isinstance(claims.get("exp"), int) and claims["exp"] > int(time.time())
' "${token_file}" "${issuer}" "${project_id}" staff "${subject}"
}

sleep 2
mint_staff_token master "${master_subject}" "${temp_dir}/master.token"
mint_staff_token catalog "${catalog_subject}" "${temp_dir}/catalog.token"
mint_staff_token viewer "${viewer_subject}" "${temp_dir}/viewer.token"
stage="zitadel_jwks_fetch"
zcurl "${issuer}/oauth/v2/keys" > "${temp_dir}/jwks.json"
stage="zitadel_staff_jwks_validate"
python3 -c '
import base64, json, pathlib, sys
token = pathlib.Path(sys.argv[1]).read_text().strip()
header = json.loads(base64.urlsafe_b64decode(token.split(".")[0] + "=" * (-len(token.split(".")[0]) % 4)))
jwks = json.load(open(sys.argv[2], encoding="utf-8"))
assert any(key.get("kid") == header["kid"] and key.get("kty") == "RSA" for key in jwks["keys"])
' "${temp_dir}/master.token" "${temp_dir}/jwks.json"
stage="zitadel_machine_token_validate"
python3 -c '
import base64, json, pathlib, sys, time
token = pathlib.Path(sys.argv[1]).read_text().strip()
header = json.loads(base64.urlsafe_b64decode(token.split(".")[0] + "=" * (-len(token.split(".")[0]) % 4)))
claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
jwks = json.load(open(sys.argv[2], encoding="utf-8"))
assert header.get("alg") == "RS256"
assert any(key.get("kid") == header.get("kid") and key.get("kty") == "RSA" for key in jwks["keys"])
assert claims.get("iss") == sys.argv[3]
aud = claims.get("aud", [])
assert sys.argv[4] in ([aud] if isinstance(aud, str) else aud)
assert claims.get("principal_kind") == "service"
assert claims.get("sub") == sys.argv[5]
assert isinstance(claims.get("exp"), int) and claims["exp"] > int(time.time())
' "${temp_dir}/machine.token" "${temp_dir}/jwks.json" "${issuer}" "${project_id}" "${machine_subject}"

write_bearer_config "${temp_dir}/master.token" "${temp_dir}/master.curl"
write_bearer_config "${temp_dir}/catalog.token" "${temp_dir}/catalog.curl"
write_bearer_config "${temp_dir}/viewer.token" "${temp_dir}/viewer.curl"
write_bearer_config "${temp_dir}/machine.token" "${temp_dir}/machine.curl"

stage="identity_compose"
cat > "${temp_dir}/identity.env" <<EOF
IDENTITY_ADMIN_PASSWORD=${identity_admin_password}
IDENTITY_MIGRATOR_PASSWORD=${identity_migrator_password}
IDENTITY_API_PASSWORD=${identity_api_password}
IDENTITY_POLICY_WORKER_PASSWORD=${identity_worker_password}
IDENTITY_DB_PORT=0
IDENTITY_API_PORT=${identity_port}
IDENTITY_ZITADEL_ISSUER_URL=${issuer}
IDENTITY_API_AUDIENCE=${project_id}
IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT=${master_subject}
IDENTITY_BOOTSTRAP_ADMIN_EMAIL=master@smoke.invalid
IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME=Master Smoke
IDENTITY_POLICY_EVENT_ENDPOINT=http://127.0.0.1:9/unused
IDENTITY_RUNTIME_IMAGE=${identity_runtime_image}
IDENTITY_WORKER_IMAGE=${identity_worker_image}
RUST_LOG=warn
EOF
cat > "${temp_dir}/identity.override.yml" <<EOF
services:
  identity-db:
    ports: !override
      - "127.0.0.1::5432"
    networks:
      default: {}
      smoke:
        aliases: [task9-identity-db]
  identity-api:
    image: ${identity_runtime_image}
    ports: !override
      - "127.0.0.1::8080"
    networks:
      default: {}
      smoke:
        aliases: [task9-identity-api]
  identity-migrate:
    image: ${identity_runtime_image}
  identity-policy-worker:
    image: ${identity_worker_image}
networks:
  smoke:
    external: true
    name: ${network}
EOF
chmod 600 "${temp_dir}/identity.env"

i_compose() {
  bounded_docker compose --env-file "${temp_dir}/identity.env" \
    -p "${identity_project}" \
    -f "${IDENTITY_CHECKOUT}/docker-compose.yml" \
    -f "${temp_dir}/identity.override.yml" "$@"
}

resolve_identity_port() {
  local endpoint
  endpoint="$(i_compose port identity-api 8080 | tr -d '\r' | tail -n 1)"
  parse_host_port "${endpoint}"
}

i_psql() {
  i_compose exec -T \
    -e PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT}" \
    -e PGOPTIONS="--search_path=identity,public" \
    identity-db psql "$@"
}

identity_started=1
set +e
timeout --foreground "${STACK_COMMAND_TIMEOUT_SECONDS}s" \
  bash "${IDENTITY_CHECKOUT}/scripts/compose-smoke.sh" \
  --env-file "${temp_dir}/identity.env" \
  -p "${identity_project}" \
  -f "${IDENTITY_CHECKOUT}/docker-compose.yml" \
  -f "${temp_dir}/identity.override.yml" \
  -- start-api > "${temp_dir}/identity-compose.log" 2>&1
identity_up_status=$?
identity_container_id="$(i_compose ps -q identity-api)"
identity_container_state=missing
identity_start_reason=unknown
if [[ -n "${identity_container_id}" ]]; then
  identity_container_state="$(bounded_docker inspect --format '{{.State.Status}}_exit_{{.State.ExitCode}}' "${identity_container_id}")"
  bounded_docker logs "${identity_container_id}" > "${temp_dir}/identity-api.log" 2>&1
  if grep -q 'IDENTITY_BOOTSTRAP_ADMIN' "${temp_dir}/identity-api.log"; then
    identity_start_reason=bootstrap_config
  elif grep -qi 'password authentication failed' "${temp_dir}/identity-api.log"; then
    identity_start_reason=database_auth
  elif grep -qi 'permission denied' "${temp_dir}/identity-api.log"; then
    identity_start_reason=database_permission
  elif grep -q 'Identity administrator bootstrap failed' "${temp_dir}/identity-api.log"; then
    identity_start_reason=bootstrap_database
  fi
fi
set -e
if [[ ${identity_up_status} -ne 0 ]]; then
  grep -Ei 'compose_smoke=FAIL|error response|error while|failed|denied|missing|invalid' \
    "${temp_dir}/identity-compose.log" | tail -n 40 >&2 || true
  stage="identity_compose_up_exit_${identity_up_status}_${identity_container_state}_${identity_start_reason}"
  exit 1
fi
stage="identity_dynamic_port"
if ! identity_port="$(resolve_identity_port)"; then
  stage="identity_dynamic_port_invalid"
  exit 1
fi
for _ in $(seq 1 180); do
  if bounded_curl --silent --fail "http://127.0.0.1:${identity_port}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
set +e
identity_ready_status="$(bounded_curl --silent --show-error -o "${temp_dir}/identity-ready.json" \
  -w '%{http_code}' "http://127.0.0.1:${identity_port}/readyz")"
identity_ready_curl_status=$?
identity_container_id="$(i_compose ps -q identity-api)"
if [[ -n "${identity_container_id}" ]]; then
  identity_container_state="$(bounded_docker inspect --format '{{.State.Status}}_exit_{{.State.ExitCode}}' "${identity_container_id}")"
else
  identity_container_state=missing
fi
set -e
if [[ ${identity_ready_curl_status} -ne 0 || "${identity_ready_status}" != "200" ]]; then
  identity_ready_fields="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
    print("_".join(str(value.get(key, "missing")) for key in ("database", "verifier_configuration")))
except Exception:
    print("unavailable")
' "${temp_dir}/identity-ready.json")"
  stage="identity_compose_http_${identity_ready_status}_curl_${identity_ready_curl_status}_${identity_ready_fields}_${identity_container_state}"
  exit 1
fi
stage="identity_zitadel_relay"
bounded_docker run -d --name "${identity_zitadel_relay}" --user 65534:65534 \
  --network "container:${identity_container_id}" "${BUSYBOX_IMAGE}" \
  nc -lk -p "${zitadel_port}" -e nc task9-zitadel "${zitadel_internal_port}" >/dev/null

stage="identity_master_bootstrap_check"
master_staff_id="$(i_psql -X -U identity_admin -d identity -Atc \
  "SELECT id FROM staff WHERE zitadel_subject = '${master_subject}'")"
[[ -n "${master_staff_id}" ]]
catalog_staff_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
viewer_staff_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
stage="identity_staff_seed"
i_psql -X -U identity_admin -d identity \
  --set=ON_ERROR_STOP=1 >/dev/null <<SQL
INSERT INTO staff (id, zitadel_subject, email, display_name, primary_role_code)
VALUES
  ('${catalog_staff_id}', '${catalog_subject}', 'catalog@smoke.invalid', 'Catalog Smoke', 'STAFF'),
  ('${viewer_staff_id}', '${viewer_subject}', 'viewer@smoke.invalid', 'Viewer Smoke', 'STAFF');
SQL

service_principal_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
cat > "${temp_dir}/service-principals.json" <<EOF
{"schema_version":"identity.service-principals.v1","principals":[{"principal_id":"${service_principal_id}","zitadel_subject":"${machine_subject}","display_name":"Intelligence Smoke","capabilities":["foundation.normalization:propose"]}]}
EOF
cat > "${temp_dir}/provision.env" <<EOF
IDENTITY_PROVISIONER_DATABASE_URL=postgres://identity_admin:${identity_admin_password}@identity-db:5432/identity
IDENTITY_WORKLOAD_PRINCIPALS_MANIFEST=/run/task9/service-principals.json
EOF
chmod 600 "${temp_dir}/service-principals.json" "${temp_dir}/provision.env"
stage="identity_service_principal_provision"
set +e
bounded_docker run --name "${identity_provisioner_container}" \
  --label "task9.run_id=${resource_id}" --network "${identity_project}_default" \
  --env-file "${temp_dir}/provision.env" \
  -v "${temp_dir}/service-principals.json:/run/task9/service-principals.json:ro" \
  --entrypoint /usr/local/bin/identity-service-provisioner \
  "${identity_runtime_image}" > "${temp_dir}/provision-report.json"
provision_status=$?
set -e
if [[ ${provision_status} -ne 0 ]]; then
  provision_category="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8")).get("category", "unknown")
except Exception:
    value = "non_json"
print("".join(c for c in str(value) if c.isalnum() or c in "_-"))
' "${temp_dir}/provision-report.json")"
  stage="identity_service_principal_provision_exit_${provision_status}_${provision_category}"
  exit 1
fi

stage="foundation_compose"
cat > "${temp_dir}/foundation.env" <<EOF
FOUNDATION_ADMIN_PASSWORD=${foundation_admin_password}
FOUNDATION_MIGRATOR_PASSWORD=${foundation_migrator_password}
FOUNDATION_API_PASSWORD=${foundation_api_password}
FOUNDATION_DB_PORT=0
FOUNDATION_API_PORT=${foundation_port}
FOUNDATION_REDIS_PORT=0
DATABASE_URL=postgres://foundation_api:${foundation_api_password}@postgres:5432/foundation
IDENTITY_API_BASE_URL=http://127.0.0.1:${identity_relay_port}
ZITADEL_ISSUER_URL=${issuer}
FOUNDATION_PLATFORM_ZITADEL_AUDIENCE=${project_id}
FOUNDATION_PLATFORM_IDENTITY_AUTHORIZATION_TIMEOUT_MS=2000
FOUNDATION_RUNTIME_IMAGE=${foundation_runtime_image}
RUST_LOG=warn
EOF
cat > "${temp_dir}/foundation.override.yml" <<EOF
services:
  postgres:
    ports: !override
      - "127.0.0.1::5432"
    networks:
      default: {}
      smoke:
        aliases: [task9-foundation-db]
  foundation-api:
    image: ${foundation_runtime_image}
    ports: !override
      - "127.0.0.1::8080"
    networks:
      default: {}
      smoke:
        aliases: [task9-foundation-api]
  foundation-migrate:
    image: ${foundation_runtime_image}
  redis:
    ports: !override
      - "127.0.0.1::6379"
networks:
  smoke:
    external: true
    name: ${network}
EOF
chmod 600 "${temp_dir}/foundation.env"

f_compose() {
  bounded_docker compose --env-file "${temp_dir}/foundation.env" \
    -p "${foundation_project}" \
    -f "${FOUNDATION_CHECKOUT}/docker-compose.yml" \
    -f "${temp_dir}/foundation.override.yml" "$@"
}

f_psql() {
  f_compose exec -T -e PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT}" postgres psql "$@"
}

foundation_started=1
set +e
ZITADEL_ISSUER_URL="${issuer}" \
IDENTITY_API_BASE_URL="http://127.0.0.1:${identity_relay_port}" \
timeout --foreground "${STACK_COMMAND_TIMEOUT_SECONDS}s" \
  bash "${FOUNDATION_CHECKOUT}/scripts/compose-smoke.sh" \
  --env-file "${temp_dir}/foundation.env" \
  -p "${foundation_project}" \
  -f "${FOUNDATION_CHECKOUT}/docker-compose.yml" \
  -f "${temp_dir}/foundation.override.yml" \
  -- start-api > "${temp_dir}/foundation-compose.log" 2>&1
foundation_up_status=$?
foundation_container_id="$(f_compose ps -q foundation-api)"
foundation_container_state=missing
foundation_start_reason=unknown
if [[ -n "${foundation_container_id}" ]]; then
  foundation_container_state="$(bounded_docker inspect --format '{{.State.Status}}_exit_{{.State.ExitCode}}' "${foundation_container_id}")"
  bounded_docker logs "${foundation_container_id}" > "${temp_dir}/foundation-api.log" 2>&1
  if grep -qi 'password authentication failed' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_auth
  elif grep -qi 'permission denied' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_permission
  elif grep -q 'DATABASE_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_config
  elif grep -q 'IDENTITY_API_BASE_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=identity_config
  elif grep -q 'ZITADEL_ISSUER_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=issuer_config
  fi
fi
set -e
if [[ ${foundation_up_status} -ne 0 ]]; then
  grep -Ei 'compose_smoke=FAIL|error response|error while|failed|denied|missing|invalid' \
    "${temp_dir}/foundation-compose.log" | tail -n 40 >&2 || true
  stage="foundation_compose_up_exit_${foundation_up_status}_${foundation_container_state}_${foundation_start_reason}"
  exit 1
fi
stage="foundation_dynamic_port"
foundation_published_endpoint="$(f_compose port foundation-api 8080 | tr -d '\r' | tail -n 1)"
if ! foundation_port="$(parse_host_port "${foundation_published_endpoint}")"; then
  stage="foundation_dynamic_port_invalid"
  exit 1
fi
for _ in $(seq 1 180); do
  if bounded_curl --silent --fail "http://127.0.0.1:${foundation_port}/readyz" >/dev/null 2>&1; then
    break
  fi
  if [[ -z "$(f_compose ps -q foundation-api)" ]]; then
    break
  fi
  sleep 1
done
set +e
foundation_ready_status="$(bounded_curl --silent --show-error -o "${temp_dir}/foundation-ready.json" \
  -w '%{http_code}' "http://127.0.0.1:${foundation_port}/readyz")"
foundation_ready_curl_status=$?
foundation_container_id="$(f_compose ps -a -q foundation-api)"
if [[ -n "${foundation_container_id}" ]]; then
  foundation_container_state="$(bounded_docker inspect --format '{{.State.Status}}_exit_{{.State.ExitCode}}' "${foundation_container_id}")"
  bounded_docker logs "${foundation_container_id}" > "${temp_dir}/foundation-api.log" 2>&1
  if grep -qi 'password authentication failed' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_auth
  elif grep -qi 'permission denied' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_permission
  elif grep -q 'DATABASE_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=database_config
  elif grep -q 'IDENTITY_API_BASE_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=identity_config
  elif grep -q 'ZITADEL_ISSUER_URL' "${temp_dir}/foundation-api.log"; then
    foundation_start_reason=issuer_config
  fi
else
  foundation_container_state=missing
fi
set -e
if [[ ${foundation_ready_curl_status} -ne 0 || "${foundation_ready_status}" != "200" ]]; then
  foundation_ready_fields="$(python3 -c '
import json, sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
    print("_".join(str(value.get(key, "missing")) for key in ("database", "status")))
except Exception:
    print("unavailable")
' "${temp_dir}/foundation-ready.json")"
  stage="foundation_compose_http_${foundation_ready_status}_curl_${foundation_ready_curl_status}_${foundation_ready_fields}_${foundation_container_state}_${foundation_start_reason}"
  exit 1
fi
stage="foundation_loopback_relays"
bounded_docker run -d --name "${foundation_zitadel_relay}" --user 65534:65534 \
  --network "container:${foundation_container_id}" "${BUSYBOX_IMAGE}" \
  nc -lk -p "${zitadel_port}" -e nc task9-zitadel "${zitadel_internal_port}" >/dev/null
bounded_docker run -d --name "${foundation_identity_relay}" --user 65534:65534 \
  --network "container:${foundation_container_id}" "${BUSYBOX_IMAGE}" \
  nc -lk -p "${identity_relay_port}" -e nc task9-identity-api 8080 >/dev/null

api_call() {
  local method=$1
  local url=$2
  local auth_config=$3
  local body=$4
  local output=$5
  bounded_curl --silent --show-error --config "${auth_config}" \
    -o "${output}" -w '%{http_code}' -X "${method}" \
    -H 'Content-Type: application/json' --data-binary @"${body}" "${url}"
}

stage="assertion_1"
printf '{}' > "${temp_dir}/empty.json"
status="$(api_call POST "http://127.0.0.1:${identity_port}/identity/v1/staff/sessions/verify" \
  "${temp_dir}/catalog.curl" "${temp_dir}/empty.json" "${temp_dir}/session-response.json")"
expect_status 200 "${status}"
catalog_jti="$(python3 -c '
import base64, json, pathlib, sys
token = pathlib.Path(sys.argv[1]).read_text().strip()
claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
jti = claims.get("jti", "")
assert isinstance(jti, str) and 0 < len(jti) <= 512
assert all(char.isalnum() or char in "._-" for char in jti)
print(jti)
' "${temp_dir}/catalog.token")"
catalog_exp="$(python3 -c '
import base64, json, pathlib, sys
token = pathlib.Path(sys.argv[1]).read_text().strip()
claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
exp = claims.get("exp")
assert isinstance(exp, int) and exp > 0
print(exp)
' "${temp_dir}/catalog.token")"
session_count="$(i_psql -X -U identity_admin -d identity \
  --set=expected_staff_id="${catalog_staff_id}" \
  --set=expected_jti="${catalog_jti}" \
  --set=expected_exp="${catalog_exp}" -At <<'SQL'
SELECT count(*)
FROM staff_session
WHERE staff_id = :'expected_staff_id'::uuid
  AND expires_at > issued_at
  AND jti = :'expected_jti'
  AND extract(epoch FROM expires_at)::bigint = :'expected_exp'::bigint;
SQL
)"
[[ "${session_count}" == "1" ]]
printf 'assertion_1=PASS identity_sessions=%s expiry_ordered=1 expiry_matches_token=1 stored_jti=1\n' "${session_count}"

stage="assertion_2"
printf '{"role_code":"CATALOG_ADMIN"}' > "${temp_dir}/grant-role.json"
status="$(api_call POST "http://127.0.0.1:${identity_port}/identity/v1/staff/${catalog_staff_id}/roles" \
  "${temp_dir}/master.curl" "${temp_dir}/grant-role.json" "${temp_dir}/grant-response.json")"
expect_status 200 "${status}"
role_count="$(i_psql -X -U identity_admin -d identity -Atc \
  "SELECT count(*) FROM staff_role WHERE staff_id = '${catalog_staff_id}' AND role_code = 'CATALOG_ADMIN'")"
outbox_count="$(i_psql -X -U identity_admin -d identity -Atc \
  "SELECT count(*) FROM outbox_event WHERE type = 'identity.staff.role_assigned.v1' AND payload->>'staff_id' = '${catalog_staff_id}' AND payload->>'role_code' = 'CATALOG_ADMIN'")"
[[ "${role_count}" == "1" && "${outbox_count}" == "1" ]]
printf 'assertion_2=PASS role_rows=%s outbox_rows=%s\n' "${role_count}" "${outbox_count}"

stage="assertion_3"
cat > "${temp_dir}/complex-admin.json" <<'EOF'
{"official_complex_code":"9900001","name":"Task9 Allowed","kind":"national","primary_bjdong_code":"1111010100","area_m2":1000}
EOF
status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/complexes" \
  "${temp_dir}/catalog.curl" "${temp_dir}/complex-admin.json" "${temp_dir}/complex-admin-response.json")"
expect_status 201 "${status}"
allowed_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.industrial_complex WHERE official_complex_code = '9900001'")"
[[ "${allowed_count}" == "1" ]]
printf 'assertion_3=PASS catalog_rows=%s\n' "${allowed_count}"

stage="assertion_4"
cat > "${temp_dir}/complex-viewer.json" <<'EOF'
{"official_complex_code":"9900002","name":"Task9 Denied","kind":"national","primary_bjdong_code":"1111010100","area_m2":1000}
EOF
status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/complexes" \
  "${temp_dir}/viewer.curl" "${temp_dir}/complex-viewer.json" "${temp_dir}/complex-viewer-response.json")"
expect_status 403 "${status}"
denied_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.industrial_complex WHERE official_complex_code = '9900002'")"
[[ "${denied_count}" == "0" ]]
printf 'assertion_4=PASS catalog_rows=%s\n' "${denied_count}"

stage="assertion_5"
i_compose stop identity-api >/dev/null 2>&1
cat > "${temp_dir}/complex-unavailable.json" <<'EOF'
{"official_complex_code":"9900003","name":"Task9 Fail Closed","kind":"national","primary_bjdong_code":"1111010100","area_m2":1000}
EOF
status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/complexes" \
  "${temp_dir}/catalog.curl" "${temp_dir}/complex-unavailable.json" "${temp_dir}/complex-unavailable-response.json")"
if ! expect_status 503 "${status}"; then
  stage="assertion_5_http_${status}"
  exit 1
fi
unavailable_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.industrial_complex WHERE official_complex_code = '9900003'")"
[[ "${unavailable_count}" == "0" ]]
printf 'assertion_5=PASS http=%s catalog_rows=%s\n' "${status}" "${unavailable_count}"
bounded_docker rm -f "${identity_zitadel_relay}" >/dev/null
stage="identity_restart"
i_compose up -d --no-deps identity-api >/dev/null 2>&1
stage="identity_restart_dynamic_port"
if ! identity_port="$(resolve_identity_port)"; then
  stage="identity_restart_dynamic_port_invalid"
  exit 1
fi
for _ in $(seq 1 90); do
  if bounded_curl --silent --fail "http://127.0.0.1:${identity_port}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
bounded_curl --silent --show-error --fail "http://127.0.0.1:${identity_port}/readyz" >/dev/null
stage="identity_zitadel_relay_restart"
identity_container_id="$(i_compose ps -q identity-api)"
bounded_docker run -d --name "${identity_zitadel_relay}" --user 65534:65534 \
  --network "container:${identity_container_id}" "${BUSYBOX_IMAGE}" \
  nc -lk -p "${zitadel_port}" -e nc task9-zitadel "${zitadel_internal_port}" >/dev/null

stage="assertion_6"
cat > "${temp_dir}/normalization.json" <<'EOF'
{"request":{"source_system":"task9-smoke","raw_record_id":"task9-raw-1","target_kind":"industrial_complex","target_identity":{"official_complex_code":"9900004"},"target_schema_version":"industrial_complex.normalized.v1"},"proposal":{"raw_record_id":"task9-raw-1","schema_version":"industrial_complex.normalized.v1","record":{"official_complex_code":"9900004","name":"Task9 Proposal","area_m2":1200},"confidence":0.9,"evidence":{"source":"task9-smoke"}},"validation":{"accepted":true,"issues":[]},"trace_context":{"trace_id":"task9-normalization-1"},"commit_allowed":false,"requires_human_review":true,"submission_metadata":{"model_profile_id":"task9","model_id":"task9","prompt_id":"task9","prompt_version":"v1","policy_id":"task9-normalization","policy_version":"v1"}}
EOF
status="$(api_call POST "http://127.0.0.1:${foundation_port}/internal/normalization/proposals" \
  "${temp_dir}/machine.curl" "${temp_dir}/normalization.json" "${temp_dir}/normalization-response.json")"
if [[ "${status}" != "202" ]]; then
  stage="assertion_6_submit_http_${status}"
  exit 1
fi
proposal_id="$(json_get "${temp_dir}/normalization-response.json" submission_id)"
printf '{"reason":"service review forbidden"}' > "${temp_dir}/review.json"
printf '{"expected_version":1}' > "${temp_dir}/apply.json"
printf '{"expected_current_version":1,"reason":"service rollback forbidden"}' > "${temp_dir}/rollback.json"
for action in approve reject; do
  status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/normalization/proposals/${proposal_id}/${action}" \
    "${temp_dir}/machine.curl" "${temp_dir}/review.json" "${temp_dir}/${action}-response.json")"
  if [[ "${status}" != "403" ]]; then
    stage="assertion_6_${action}_http_${status}"
    exit 1
  fi
done
status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/normalization/proposals/${proposal_id}/apply" \
  "${temp_dir}/machine.curl" "${temp_dir}/apply.json" "${temp_dir}/apply-response.json")"
if [[ "${status}" != "403" ]]; then
  stage="assertion_6_apply_http_${status}"
  exit 1
fi
random_application_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
status="$(api_call POST "http://127.0.0.1:${foundation_port}/catalog/v1/normalization/applications/${random_application_id}/rollback" \
  "${temp_dir}/machine.curl" "${temp_dir}/rollback.json" "${temp_dir}/rollback-response.json")"
if [[ "${status}" != "403" ]]; then
  stage="assertion_6_rollback_http_${status}"
  exit 1
fi
proposal_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.normalization_proposal WHERE id = '${proposal_id}' AND status = 'pending_review' AND submitted_by_service = 'intelligence-platform'")"
review_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.normalization_proposal_review WHERE proposal_id = '${proposal_id}'")"
application_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM catalog.normalization_application WHERE proposal_id = '${proposal_id}'")"
[[ "${proposal_count}" == "1" && "${review_count}" == "0" && "${application_count}" == "0" ]]
printf 'assertion_6=PASS proposals=%s reviews=%s applications=%s forbidden_http=4\n' \
  "${proposal_count}" "${review_count}" "${application_count}"

stage="assertion_7"
identity_foundation_role_count="$(i_psql -X -U identity_admin -d identity -Atc \
  "SELECT count(*) FROM pg_roles WHERE rolname = 'foundation_api'")"
identity_catalog_schema_count="$(i_psql -X -U identity_admin -d identity -Atc \
  "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'catalog'")"
cat > "${temp_dir}/identity-cross-probe.env" <<EOF
PGPASSWORD=${foundation_api_password}
PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT}
EOF
chmod 600 "${temp_dir}/identity-cross-probe.env"
set +e
bounded_docker run --name "${identity_cross_probe_container}" \
  --label "task9.run_id=${resource_id}" --network "${network}" \
  --env-file "${temp_dir}/identity-cross-probe.env" \
  "${POSTGRES_IMAGE}" psql -X -h task9-identity-db -U foundation_api -d identity \
  -c 'SELECT 1' >/dev/null 2>&1
cross_status=$?
set -e
[[ ${cross_status} -ne 0 && "${identity_foundation_role_count}" == "0" && "${identity_catalog_schema_count}" == "0" ]]
printf 'assertion_7=PASS foreign_roles=%s foreign_schemas=%s login_exit_nonzero=1\n' \
  "${identity_foundation_role_count}" "${identity_catalog_schema_count}"

stage="assertion_8"
foundation_identity_role_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM pg_roles WHERE rolname = 'identity_api'")"
foundation_identity_schema_count="$(f_psql -X -U foundation_admin -d foundation -Atc \
  "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'identity'")"
cat > "${temp_dir}/foundation-cross-probe.env" <<EOF
PGPASSWORD=${identity_api_password}
PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT}
EOF
chmod 600 "${temp_dir}/foundation-cross-probe.env"
set +e
bounded_docker run --name "${foundation_cross_probe_container}" \
  --label "task9.run_id=${resource_id}" --network "${network}" \
  --env-file "${temp_dir}/foundation-cross-probe.env" \
  "${POSTGRES_IMAGE}" psql -X -h task9-foundation-db -U identity_api -d foundation \
  -c 'SELECT 1' >/dev/null 2>&1
cross_status=$?
set -e
[[ ${cross_status} -ne 0 && "${foundation_identity_role_count}" == "0" && "${foundation_identity_schema_count}" == "0" ]]
printf 'assertion_8=PASS foreign_roles=%s foreign_schemas=%s login_exit_nonzero=1\n' \
  "${foundation_identity_role_count}" "${foundation_identity_schema_count}"

stage="complete"
