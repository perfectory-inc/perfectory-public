#!/usr/bin/env bash
# Runs the data-job scheduler (Airflow) on the host that runs Foundation (root ADR-0118, ADR-0122).
#
#   airflow-runtime.sh init-secrets        create the runtime env file and the scheduler's SSH key
#                                         once (0600); never prints a secret
#   airflow-runtime.sh provision EMAIL...  pre-create staff accounts (Admin); sign-in is Zitadel only
#   airflow-runtime.sh trigger JOB         run one enabled job now, e.g. `trigger outbox_publish`
#   airflow-runtime.sh api-reader          create (or re-key) the read-only account Foundation's
#                                         operations map reads failed runs' logs with (root
#                                         ADR-0195) and write its sign-in to
#                                         $AIRFLOW_RUNTIME_DIR/operations-map-reader.env (0600),
#                                         for the operator to merge into operations-map.env
#   airflow-runtime.sh <compose args...>   e.g. `up -d`, `ps`, `logs airflow-scheduler`
#
# After `up`/`restart`/`start` it waits for the API, makes sure the `spark` pool has its one slot
# (ADR-0122 §5), pauses or unpauses each job's DAG as orchestration/jobs.v1.json says, starts or
# removes the Dagster observer as its `observers.dagster.enabled` says (root ADR-0333 stage 0; a
# Dagster that does not come up is a warning, never a failed deploy), and checks that staff sign-in
# hands the browser to Zitadel; it exits 1 otherwise.
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

# Dagster's two secrets (root ADR-0333 stage 0): its database role's and its Airflow account's.
# compose.orchestration.yml requires them even while the profile is off, so a host whose env file
# predates them gets them here, before any compose call, instead of a deploy failing at step 5.
ensure_dagster_secrets() {
  [[ -s "${env_file}" ]] || return 0
  local name
  for name in DAGSTER_DB_PASSWORD DAGSTER_AIRFLOW_PASSWORD; do
    grep -q "^${name}=" "${env_file}" && continue
    (umask 077 && printf '%s=%s\n' "${name}" "$(random_hex)" >>"${env_file}")
    printf 'added %s to %s\n' "${name}" "${env_file}"
  done
}

# orchestration/jobs.v1.json `observers.dagster.enabled`: whether Dagster observes this Airflow.
dagster_enabled() {
  python3 -c '
import json, sys
sys.exit(0 if json.load(open(sys.argv[1])).get("observers", {}).get("dagster", {}).get("enabled") is True else 1)
' "${jobs_file}"
}

dagster_network=foundation-platform-compute_default
# Tagged by release admission from the signed release (OPTIONAL_COMPOSE_IMAGES), as compose names it.
dagster_image=foundation-platform-dagster:local
dagster_services=(dagster-webserver dagster-daemon)

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
  ensure_dagster_secrets
}

compose() {
  [[ -r "${env_file}" ]] || fail "missing ${env_file}; run: $0 init-secrets"
  [[ -r "${oidc_file}" ]] || fail "missing ${oidc_file}; run identity-platform configure-zitadel.sh"
  docker network inspect identity-shared >/dev/null 2>&1 ||
    fail "the identity-shared network is missing; start identity-platform first"
  docker network inspect metadata-shared >/dev/null 2>&1 ||
    docker network create metadata-shared >/dev/null
  ensure_dagster_secrets
  # The Dagster profile is on only when the contract says so, the release carried its image (a
  # release from before it, or one the host built, does not) and Trino's network exists: a missing
  # image or external network would stop `up` for Airflow too.
  unset COMPOSE_PROFILES
  if dagster_enabled; then
    if ! docker image inspect "${dagster_image}" >/dev/null 2>&1; then
      printf 'airflow-runtime: Dagster stays off: the release carries no %s\n' "${dagster_image}" >&2
    elif ! docker network inspect "${dagster_network}" >/dev/null 2>&1; then
      printf 'airflow-runtime: Dagster stays off: the lakehouse network %s is missing\n' "${dagster_network}" >&2
    else
      export COMPOSE_PROFILES=dagster-observe
    fi
  fi
  docker compose -p airflow \
    --project-directory "${root_dir}" \
    --env-file "${env_file}" --env-file "${oidc_file}" \
    -f "${compose_file}" "$@"
}

airflow_cli() {
  compose exec -T airflow-scheduler airflow "$@"
}

# The pools are declared once, in orchestration/jobs.v1.json `pools`; job_specs.py refuses a job
# naming any other.
ensure_pools() {
  local name slots description
  while IFS=$'\t' read -r name slots description; do
    # `compose exec` reads stdin; see sync_enabled.
    airflow_cli pools set "${name}" "${slots}" "${description}" </dev/null >/dev/null
  done < <(python3 -c '
import json, sys
for name, pool in json.load(open(sys.argv[1]))["pools"].items():
    print(name, pool["slots"], pool["description"], sep="\t")
' "${jobs_file}")
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

wait_for_declared_dags() {
  local listing
  for _ in $(seq 1 30); do
    if listing="$(airflow_cli dags list --output json)" &&
      printf '%s' "${listing}" | python3 -c '
import json, sys
try:
    expected = {"foundation_" + job["id"] for job in json.load(open(sys.argv[1]))["jobs"]}
    rows = json.load(sys.stdin)
    if not expected or not isinstance(rows, list) or any(
        not isinstance(row, dict) or not isinstance(row.get("dag_id"), str) for row in rows
    ):
        raise ValueError("invalid DAG list")
    missing = expected - {row["dag_id"] for row in rows}
    if missing:
        print("waiting for declared DAGs: " + ", ".join(sorted(missing)), file=sys.stderr)
        sys.exit(1)
except (ValueError, TypeError, KeyError, OSError) as error:
    print("cannot validate declared DAG readiness: " + str(error), file=sys.stderr)
    sys.exit(1)
' "${jobs_file}"; then
      return 0
    fi
    sleep 5
  done
  fail "declared DAGs did not become ready; pool and activation state were not changed"
}

env_value() { sed -n "s/^$1=//p" "${env_file}" | tail -1; }

# Dagster follows orchestration/jobs.v1.json `observers.dagster` (root ADR-0333 stage 0). It runs
# no job, so it never fails the deploy: what goes wrong is said on stderr and Airflow goes on.
sync_dagster() {
  local code
  if [[ "${COMPOSE_PROFILES:-}" != dagster-observe ]]; then
    # Off: its containers go; its database and history stay.
    compose --profile dagster-observe rm --stop --force "${dagster_services[@]}" >/dev/null 2>&1 || true
    printf 'dagster: off\n'
    return 0
  fi
  # Its own role and database in airflow-db; the password reaches psql through the environment.
  if ! DAGSTER_DB_PASSWORD="$(env_value DAGSTER_DB_PASSWORD)" compose exec -T -e DAGSTER_DB_PASSWORD airflow-db \
    sh -c 'psql -X -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v password="$DAGSTER_DB_PASSWORD"' \
    <"${root_dir}/orchestration/dagster/dagster-database.sql" >/dev/null; then
    printf 'airflow-runtime: WARN dagster: its database could not be made; Dagster is not observing\n' >&2
    return 0
  fi
  # Its Airflow account reads runs and changes nothing (Viewer); re-made so the password is the env file's.
  airflow_cli users delete --username foundation-dagster-observe </dev/null >/dev/null 2>&1 || true
  if ! DAGSTER_AIRFLOW_PASSWORD="$(env_value DAGSTER_AIRFLOW_PASSWORD)" compose exec -T -e DAGSTER_AIRFLOW_PASSWORD \
    airflow-scheduler sh -c 'exec airflow users create --username foundation-dagster-observe --email foundation-dagster-observe@foundation.invalid --firstname dagster --lastname observe --role Viewer --password "${DAGSTER_AIRFLOW_PASSWORD}"' \
    </dev/null >/dev/null; then
    printf 'airflow-runtime: WARN dagster: its Airflow account could not be made; Dagster is not observing\n' >&2
    return 0
  fi
  # A restart loads the release's code location (it is read when the process starts).
  compose up -d "${dagster_services[@]}" >/dev/null && compose restart "${dagster_services[@]}" >/dev/null
  for _ in $(seq 1 30); do
    code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19085/server_info || true)"
    [[ "${code}" == 200 ]] && { printf 'dagster: observing; UI on 127.0.0.1:19085\n'; return 0; }
    sleep 10
  done
  printf 'airflow-runtime: WARN dagster: the UI answered %s after 5 minutes\n' "${code}" >&2
}

verify_up() {
  local code redirect
  for _ in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "${api}/api/v2/monitor/health" || true)"
    [[ "${code}" == 200 ]] && break
    sleep 10
  done
  [[ "${code}" == 200 ]] || fail "the API answered ${code} after 10 minutes"
  # Existing DAGs do not prove a newly released job has been parsed. Do not change any
  # scheduler state until every job in the single authoritative list is present.
  wait_for_declared_dags
  ensure_pools
  sync_enabled
  sync_dagster
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

# The one account that signs in with a password (staff sign in through Zitadel): Viewer, so it
# reads runs and logs and changes nothing. Re-running replaces its password.
api_reader() {
  local username=foundation-operations-map password file="${runtime_dir}/operations-map-reader.env"
  password="$(random_hex)"
  if airflow_cli users list --output json | USERNAME="${username}" python3 -c '
import json, os, sys
sys.exit(0 if any(u.get("username") == os.environ["USERNAME"] for u in json.load(sys.stdin)) else 1)'; then
    airflow_cli users delete --username "${username}" >/dev/null
  fi
  compose exec -T -e READER_PASSWORD="${password}" airflow-scheduler sh -c \
    'exec airflow users create --username "$1" --email "$1@foundation.invalid" --firstname operations --lastname map --role Viewer --password "${READER_PASSWORD}"' \
    sh "${username}" >/dev/null
  umask 077
  printf 'FOUNDATION_PLATFORM_AIRFLOW_API_USERNAME=%s\nFOUNDATION_PLATFORM_AIRFLOW_API_PASSWORD=%s\n' \
    "${username}" "${password}" >"${file}"
  printf 'api-reader: %s is a Viewer; its sign-in is in %s\n' "${username}" "${file}"
}

(($# > 0)) || fail "usage: $0 init-secrets | provision EMAIL... | trigger JOB | api-reader | <docker compose args>"
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
  api-reader) api_reader ;;
  *)
    compose "$@"
    case "$1" in
      up | restart | start) verify_up ;;
    esac
    ;;
esac
