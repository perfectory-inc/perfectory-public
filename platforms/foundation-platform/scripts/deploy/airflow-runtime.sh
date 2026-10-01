#!/usr/bin/env bash
# Runs the data-job scheduler (Airflow) on the host that runs Foundation (root ADR-0118, ADR-0122).
#
#   airflow-runtime.sh init-secrets        create the runtime env file and the scheduler's SSH key
#                                         once (0600); never prints a secret
#   airflow-runtime.sh provision EMAIL...  pre-create staff accounts (Admin); sign-in is Zitadel only
#   airflow-runtime.sh trigger JOB         run one enabled job now, e.g. `trigger outbox_publish`
#   airflow-runtime.sh <compose args...>   e.g. `up -d`, `ps`, `logs airflow-scheduler`
#
# After `up`/`restart`/`start` it waits for the API, makes sure the `spark` pool has its one slot
# (ADR-0122 §5), pauses or unpauses each job's DAG as orchestration/jobs.v1.json says, and checks
# that staff sign-in hands the browser to Zitadel; it exits 1 otherwise.
#
# The scheduler reaches the host only through the account `foundation-scheduler`, which
# `foundation-release.sh timers <public key>` creates and limits to starting enabled jobs' services.
#
# Environment:
#   AIRFLOW_RUNTIME_DIR       state directory holding runtime.env and the scheduler key, mode 0700
#                             (default $HOME/airflow-state: the operator who deploys owns it)
#   AIRFLOW_OIDC_CLIENT_FILE  Zitadel client credentials written by identity-platform's
#                             configure-zitadel.sh (default
#                             /etc/identity-platform/secrets/airflow-oidc-client.env)
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
compose_file="${root_dir}/compose.orchestration.yml"
jobs_file="${root_dir}/orchestration/jobs.v1.json"
runtime_dir="${AIRFLOW_RUNTIME_DIR:-${HOME}/airflow-state}"
env_file="${runtime_dir}/runtime.env"
scheduler_key="${runtime_dir}/scheduler_ed25519"
host_key_file=/etc/ssh/ssh_host_ed25519_key.pub
oidc_file="${AIRFLOW_OIDC_CLIENT_FILE:-/etc/identity-platform/secrets/airflow-oidc-client.env}"
api=http://127.0.0.1:19080

fail() {
  printf 'FAIL airflow-runtime: %s\n' "$*" >&2
  exit 1
}

random_hex() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

init_secrets() {
  mkdir -p "${runtime_dir}"
  chmod 0700 "${runtime_dir}"
  umask 077
  if [[ ! -s "${env_file}" ]]; then
    {
      printf 'AIRFLOW_DB_PASSWORD=%s\n' "$(random_hex)"
      printf 'AIRFLOW_JWT_SECRET=%s\n' "$(random_hex)"
      printf 'AIRFLOW_API_SECRET_KEY=%s\n' "$(random_hex)"
      # Fernet wants 32 url-safe base64 bytes.
      printf 'AIRFLOW_FERNET_KEY=%s\n' "$(head -c 32 /dev/urandom | base64 | tr '+/' '-_')"
    } >"${env_file}"
    printf 'created %s\n' "${env_file}"
  fi
  if [[ ! -s "${scheduler_key}" ]]; then
    ssh-keygen -q -t ed25519 -N '' -C airflow-scheduler -f "${scheduler_key}"
    printf 'created %s.pub; install it with: sudo foundation-release.sh timers %s.pub\n' \
      "${scheduler_key}" "${scheduler_key}"
  fi
  if ! grep -q '^AIRFLOW_CONN_FOUNDATION_HOST=' "${env_file}"; then
    [[ -r "${host_key_file}" ]] || fail "cannot read the host key ${host_key_file}"
    # One JSON connection; single quotes keep compose from interpolating it. The host key pins
    # the server, so the scheduler never trusts an unknown one.
    python3 - "${scheduler_key}" "${host_key_file}" >>"${env_file}" <<'PY'
import json, sys
private = open(sys.argv[1]).read()
host_key = " ".join(open(sys.argv[2]).read().split()[:2])
connection = {
    "conn_type": "ssh",
    "host": "foundation-host",
    "port": 22,
    "login": "foundation-scheduler",
    "extra": {"private_key": private, "host_key": host_key, "allow_host_key_change": False},
}
print("AIRFLOW_CONN_FOUNDATION_HOST='" + json.dumps(connection) + "'")
PY
    printf 'added the host connection to %s\n' "${env_file}"
  fi
}

compose() {
  [[ -r "${env_file}" ]] || fail "missing ${env_file}; run: $0 init-secrets"
  [[ -r "${oidc_file}" ]] || fail "missing ${oidc_file}; run identity-platform configure-zitadel.sh"
  docker network inspect identity-shared >/dev/null 2>&1 ||
    fail "the identity-shared network is missing; start identity-platform first"
  docker network inspect metadata-shared >/dev/null 2>&1 ||
    docker network create metadata-shared >/dev/null
  docker compose -p airflow \
    --project-directory "${root_dir}" \
    --env-file "${env_file}" --env-file "${oidc_file}" \
    -f "${compose_file}" "$@"
}

airflow_cli() {
  compose exec -T airflow-scheduler airflow "$@"
}

ensure_pools() {
  airflow_cli pools set spark 1 "Spark runs one at a time: the host memory budget assumes one (root ADR-0122)" >/dev/null
}

# Paused or not is decided by orchestration/jobs.v1.json `enabled`, never by a click in the UI: the
# release that switches a job on is the one that retires its systemd timer (ADR-0122 §4).
sync_enabled() {
  local id enabled
  while read -r id enabled; do
    # `compose exec` reads stdin: without </dev/null the first call swallows the rest of the
    # job list, and only the first job's state was set (measured on ai-server 2026-10-01).
    if [[ "${enabled}" == true ]]; then
      airflow_cli dags unpause "foundation_${id}" </dev/null >/dev/null
    else
      airflow_cli dags pause "foundation_${id}" </dev/null >/dev/null
    fi
    printf 'job %-24s enabled=%s\n' "${id}" "${enabled}"
  done < <(python3 -c '
import json, sys
for job in json.load(open(sys.argv[1]))["jobs"]:
    print(job["id"], str(job["enabled"]).lower())
' "${jobs_file}")
}

verify_up() {
  local code redirect
  for _ in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "${api}/api/v2/monitor/health" || true)"
    [[ "${code}" == 200 ]] && break
    sleep 10
  done
  [[ "${code}" == 200 ]] || fail "the API answered ${code} after 10 minutes"
  # The DAG processor parses the jobs a moment after start; pausing waits for them to exist.
  for _ in $(seq 1 30); do
    airflow_cli dags list -o plain 2>/dev/null | grep -q '^foundation_' && break
    sleep 5
  done
  ensure_pools
  sync_enabled
  redirect="$(curl -s -o /dev/null -w '%{redirect_url}' "${api}/auth/login/zitadel?next=" || true)"
  [[ "${redirect}" == http://127.0.0.1:18453/oauth/v2/authorize\?* ]] ||
    fail "sign-in does not hand the browser to Zitadel (got '${redirect}')"
  printf 'airflow-runtime: API answers, jobs match jobs.v1.json, sign-in goes to Zitadel\n'
}

provision() {
  (($# > 0)) || fail "usage: $0 provision EMAIL..."
  local email
  for email in "$@"; do
    if airflow_cli users list --output json | EMAIL="${email}" python3 -c '
import json, os, sys
sys.exit(0 if any(u.get("email") == os.environ["EMAIL"] for u in json.load(sys.stdin)) else 1)'; then
      printf 'exists      %s\n' "${email}"
      continue
    fi
    # The password is random and never shown: password sign-in does not exist (webserver_config).
    airflow_cli users create --username "${email}" --email "${email}" \
      --firstname "${email%%@*}" --lastname "-" --role Admin --use-random-password >/dev/null
    printf 'provisioned %s as Admin\n' "${email}"
  done
}

trigger() {
  (($# == 1)) || fail "usage: $0 trigger JOB"
  [[ "$1" =~ ^[a-z][a-z0-9_]*$ ]] || fail "job ids are lower_snake names"
  airflow_cli dags trigger "foundation_$1"
}

(($# > 0)) || fail "usage: $0 init-secrets | provision EMAIL... | trigger JOB | <docker compose args>"
case "$1" in
  init-secrets) init_secrets ;;
  provision)
    shift
    provision "$@"
    ;;
  trigger)
    shift
    trigger "$@"
    ;;
  *)
    compose "$@"
    case "$1" in
      up | restart | start) verify_up ;;
    esac
    ;;
esac
