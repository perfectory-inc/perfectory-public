#!/usr/bin/env bash
set -Eeuo pipefail

ONE_SHOT_TIMEOUT_SECONDS="${ONE_SHOT_TIMEOUT_SECONDS:-45}"
COMPOSE_COMMAND_TIMEOUT_SECONDS="${COMPOSE_COMMAND_TIMEOUT_SECONDS:-600}"
POSTCONDITION_TIMEOUT_SECONDS="${POSTCONDITION_TIMEOUT_SECONDS:-20}"
READINESS_TIMEOUT_SECONDS="${READINESS_TIMEOUT_SECONDS:-90}"

compose_args=()
while [[ $# -gt 0 && "$1" != "--" ]]; do
  compose_args+=("$1")
  shift
done
if [[ ${1:-} == "--" ]]; then
  shift
fi
mode="${1:-start-api}"
oidc_fixture_issuer="http://127.0.0.1:18081"
if [[ -z "${ZITADEL_ISSUER_URL:-}" ]]; then
  export ZITADEL_ISSUER_URL="${oidc_fixture_issuer}"
fi
use_oidc_fixture=0
runtime_service=foundation-api
if [[ "${ZITADEL_ISSUER_URL}" == "${oidc_fixture_issuer}" ]]; then
  use_oidc_fixture=1
  runtime_service=foundation-api-smoke
fi

for command in docker timeout; do
  command -v "${command}" >/dev/null 2>&1 || {
    printf 'compose_smoke=FAIL reason=missing_command command=%s\n' "${command}" >&2
    exit 2
  }
done

bounded_compose() {
  local seconds=$1
  shift
  timeout --foreground "${seconds}s" docker compose "${compose_args[@]}" "$@"
}

remove_container() {
  local container_id=$1
  timeout --foreground 10s docker stop --time 2 "${container_id}" >/dev/null 2>&1 || true
  timeout --foreground 10s docker rm -f "${container_id}" >/dev/null 2>&1 || true
}

remove_service_containers() {
  local service=$1 container_id
  local container_ids=''
  set +e
  container_ids="$(bounded_compose 10 ps -a -q "${service}" 2>/dev/null)"
  set -e
  while IFS= read -r container_id; do
    [[ -n "${container_id}" ]] && remove_container "${container_id}"
  done <<<"${container_ids}"
}

container_health() {
  local service=$1 container_id
  container_id="$(bounded_compose 10 ps -q "${service}")"
  [[ -n "${container_id}" ]] || return 1
  timeout --foreground 10s docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "${container_id}"
}

database_bool() {
  local statement=$1
  local output status
  set +e
  output="$(timeout --foreground "${POSTCONDITION_TIMEOUT_SECONDS}s" \
    docker compose "${compose_args[@]}" exec -T postgres \
    psql -X -A -t -v ON_ERROR_STOP=1 -U foundation_admin -d foundation -c "${statement}" 2>/dev/null)"
  status=$?
  set -e
  if [[ ${status} -ne 0 ]]; then
    printf 'f'
    return
  fi
  printf '%s' "${output//$'\r'/}" | tail -n 1
}

postcondition() {
  local service=$1
  case "${service}" in
    foundation-bootstrap)
      database_bool "
        SELECT
          (SELECT count(*) = 2 FROM pg_catalog.pg_roles
           WHERE rolname IN ('foundation_migrator','foundation_api'))
          AND has_database_privilege('foundation_migrator', current_database(), 'CONNECT')
          AND has_database_privilege('foundation_migrator', current_database(), 'CREATE')
          AND has_database_privilege('foundation_api', current_database(), 'CONNECT')
          AND EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'postgis')
          AND EXISTS (
            SELECT 1 FROM pg_catalog.pg_namespace AS namespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
            WHERE namespace.nspname = 'catalog' AND owner.rolname = 'foundation_migrator');"
      ;;
    foundation-migrate)
      database_bool "
        SELECT to_regnamespace('catalog') IS NOT NULL
          AND to_regclass('catalog.industrial_complex') IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM public._sqlx_migrations WHERE NOT success);"
      ;;
    foundation-runtime-grants)
      database_bool "
        SELECT has_schema_privilege('foundation_api', 'catalog', 'USAGE')
          AND NOT has_schema_privilege('foundation_api', 'catalog', 'CREATE')
          AND has_table_privilege('foundation_api', 'catalog.industrial_complex', 'SELECT')
          AND has_table_privilege('foundation_api', 'catalog.industrial_complex', 'INSERT')
          AND has_table_privilege('foundation_api', 'catalog.industrial_complex', 'UPDATE')
          AND has_table_privilege('foundation_api', 'catalog.industrial_complex', 'DELETE');"
      ;;
    foundation-finalize)
      database_bool "
        SELECT NOT has_database_privilege('foundation_migrator', current_database(), 'CREATE')
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles
            WHERE rolname IN ('foundation_migrator','foundation_api')
              AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit
                OR rolreplication OR rolbypassrls))
          AND EXISTS (
            SELECT 1 FROM pg_catalog.pg_namespace AS namespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
            WHERE namespace.nspname = 'catalog' AND owner.rolname = 'foundation_migrator');"
      ;;
    *)
      printf 'f'
      ;;
  esac
}

run_one_shot() {
  local service=$1
  local helper_status result
  set +e
  bounded_compose "${ONE_SHOT_TIMEOUT_SECONDS}" run --rm -T --no-deps \
    --interactive=false "${service}"
  helper_status=$?
  set -e
  if [[ ${helper_status} -ne 0 ]]; then
    remove_service_containers "${service}"
    printf 'compose_smoke=FAIL service=%s reason=helper_timeout_or_exit exit=%s\n' \
      "${service}" "${helper_status}" >&2
    return 1
  fi

  result="$(postcondition "${service}")"
  if [[ "${result}" != "t" ]]; then
    printf 'compose_smoke=FAIL service=%s reason=postcondition\n' "${service}" >&2
    return 1
  fi
  printf 'compose_helper=%s state=exited exit=0 postcondition=PASS\n' "${service}"
}

run_uid_probe() {
  local service=$1
  local probe_status uid output
  set +e
  output="$(bounded_compose 20 run --rm -T --no-deps --interactive=false \
    --entrypoint sh "${service}" -c 'id -u')"
  probe_status=$?
  set -e
  if [[ ${probe_status} -ne 0 ]]; then
    remove_service_containers "${service}"
    printf 'compose_smoke=FAIL service=%s reason=uid_probe_timeout_or_exit exit=%s\n' \
      "${service}" "${probe_status}" >&2
    return 1
  fi
  uid="$(printf '%s\n' "${output//$'\r'/}" | tail -n 1)"
  if [[ ! "${uid:-}" =~ ^[0-9]+$ || "${uid}" == "0" ]]; then
    printf 'compose_smoke=FAIL service=%s reason=effective_uid\n' "${service}" >&2
    return 1
  fi
  printf 'compose_uid=%s uid=%s\n' "${service}" "${uid}"
}

wait_database() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health postgres 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=postgres reason=readiness_timeout\n' >&2
  return 1
}

wait_oidc_fixture() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health foundation-oidc-smoke 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=foundation-oidc-smoke reason=readiness_timeout\n' >&2
  return 1
}

start_oidc_fixture() {
  if [[ ${use_oidc_fixture} -eq 1 ]]; then
    bounded_compose 60 up -d foundation-oidc-smoke
    wait_oidc_fixture
  fi
}

wait_api() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health "${runtime_service}" 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=%s reason=readiness_timeout\n' "${runtime_service}" >&2
  return 1
}

run_chain() {
  run_one_shot foundation-bootstrap
  run_one_shot foundation-migrate
  run_one_shot foundation-runtime-grants
  run_one_shot foundation-finalize
}

start_api() {
  bounded_compose "${COMPOSE_COMMAND_TIMEOUT_SECONDS}" build foundation-api
  bounded_compose 60 up -d postgres
  wait_database
  start_oidc_fixture
  run_chain
  bounded_compose 60 up -d --no-deps "${runtime_service}"
  wait_api
}

rerun_api() {
  bounded_compose 60 stop "${runtime_service}" || true
  start_oidc_fixture
  run_chain
  bounded_compose 60 up -d --no-deps "${runtime_service}"
  wait_api
}

case "${mode}" in
  start-api) start_api ;;
  rerun-api) rerun_api ;;
  verify-uids)
    run_uid_probe foundation-bootstrap
    run_uid_probe foundation-migrate
    run_uid_probe foundation-runtime-grants
    run_uid_probe foundation-finalize
    ;;
  *)
    printf 'compose_smoke=FAIL reason=unknown_mode mode=%s\n' "${mode}" >&2
    exit 2
    ;;
esac
