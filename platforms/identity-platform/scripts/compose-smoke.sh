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
mode="${1:-start-all}"

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
    docker compose "${compose_args[@]}" exec -T identity-db \
    psql -X -A -t -v ON_ERROR_STOP=1 -U identity_admin -d identity -c "${statement}" 2>/dev/null)"
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
    identity-bootstrap)
      database_bool "
        SELECT
          (SELECT count(*) = 6 FROM pg_catalog.pg_roles
           WHERE rolname IN ('identity_migrator','identity_api','identity_policy_worker',
             'identity_provisioner','identity_recovery','identity_operations_admin'))
          AND has_database_privilege('identity_migrator', current_database(), 'CONNECT')
          AND has_database_privilege('identity_migrator', current_database(), 'CREATE')
          AND has_database_privilege('identity_api', current_database(), 'CONNECT')
          AND has_database_privilege('identity_policy_worker', current_database(), 'CONNECT')
          AND has_database_privilege('identity_provisioner', current_database(), 'CONNECT');"
      ;;
    identity-database-migrator)
      database_bool "
        SELECT to_regnamespace('identity') IS NOT NULL
          AND to_regclass('identity.staff') IS NOT NULL
          AND (SELECT count(*) FROM public._sqlx_migrations WHERE success) = 1;"
      ;;
    identity-runtime-grants)
      database_bool "
        SELECT has_table_privilege('identity_api', 'identity.staff', 'SELECT')
          AND has_table_privilege('identity_api', 'identity.staff', 'INSERT')
          AND NOT has_table_privilege('identity_api', 'identity.staff', 'UPDATE')
          AND has_table_privilege('identity_api', 'identity.staff_role', 'INSERT')
          AND has_table_privilege('identity_api', 'identity.outbox_event', 'INSERT')
          AND NOT has_table_privilege('identity_api', 'identity.outbox_event', 'UPDATE')
          AND has_table_privilege('identity_policy_worker', 'identity.outbox_event', 'UPDATE')
          AND NOT has_table_privilege('identity_policy_worker', 'identity.staff', 'SELECT')
          AND has_table_privilege('identity_provisioner', 'identity.service_principal', 'SELECT')
          AND has_table_privilege('identity_provisioner', 'identity.service_principal', 'INSERT')
          AND has_table_privilege('identity_provisioner', 'identity.service_principal', 'UPDATE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.service_principal', 'DELETE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.service_principal', 'TRUNCATE')
          AND has_table_privilege('identity_provisioner', 'identity.service_capability_grant', 'SELECT')
          AND has_table_privilege('identity_provisioner', 'identity.service_capability_grant', 'INSERT')
          AND has_table_privilege('identity_provisioner', 'identity.service_capability_grant', 'DELETE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.service_capability_grant', 'UPDATE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.service_capability_grant', 'TRUNCATE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.staff', 'SELECT')
          AND NOT has_table_privilege('identity_provisioner', 'identity.staff', 'INSERT')
          AND NOT has_table_privilege('identity_provisioner', 'identity.staff', 'UPDATE')
          AND NOT has_table_privilege('identity_provisioner', 'identity.staff', 'DELETE');"
      ;;
    identity-workload-provisioner)
      database_bool "
        SELECT (SELECT count(*) FROM identity.service_principal) = 4
          AND (SELECT count(*) FROM identity.service_capability_grant) = 4
          AND NOT EXISTS (
            SELECT 1
            FROM identity.service_principal AS principal
            LEFT JOIN identity.service_capability_grant AS capability_grant
              ON capability_grant.service_principal_id = principal.id
            WHERE principal.zitadel_subject NOT LIKE 'compose-smoke-%'
               OR capability_grant.capability IS NULL);"
      ;;
    identity-finalize)
      database_bool "
        SELECT NOT has_database_privilege('identity_migrator', current_database(), 'CREATE')
          AND has_schema_privilege('identity_migrator', 'identity', 'CREATE')
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles
            WHERE rolname IN ('identity_migrator','identity_api','identity_policy_worker',
              'identity_provisioner')
              AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit
                OR rolreplication OR rolbypassrls))
          AND EXISTS (
            SELECT 1 FROM pg_catalog.pg_namespace AS namespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
            WHERE namespace.nspname = 'identity' AND owner.rolname = 'identity_migrator');"
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

provisioner_psql() {
  local statement=$1
  bounded_compose 20 exec -T \
    -e PGPASSWORD="${IDENTITY_PROVISIONER_PASSWORD:?set IDENTITY_PROVISIONER_PASSWORD}" \
    identity-db psql -X -v ON_ERROR_STOP=1 -U identity_provisioner -d identity \
    -c "${statement}"
}

verify_provisioner_acl() {
  provisioner_psql "
    SELECT count(*) = 4 FROM identity.service_principal;
    BEGIN;
    UPDATE identity.service_principal SET display_name = display_name WHERE false;
    DELETE FROM identity.service_capability_grant WHERE false;
    ROLLBACK;" >/dev/null

  local statement
  for statement in \
    "SELECT count(*) FROM identity.staff" \
    "DELETE FROM identity.service_principal WHERE false" \
    "UPDATE identity.service_capability_grant SET capability = capability WHERE false" \
    "CREATE TABLE identity.forbidden_probe(id integer)"; do
    if provisioner_psql "${statement}" >/dev/null 2>&1; then
      printf 'compose_smoke=FAIL service=identity-workload-provisioner reason=forbidden_sql_succeeded\n' >&2
      return 1
    fi
  done
  printf 'compose_provisioner_acl=PASS\n'
}

wait_database() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health identity-db 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=identity-db reason=readiness_timeout\n' >&2
  return 1
}

wait_api() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health identity-api 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=identity-api reason=readiness_timeout\n' >&2
  return 1
}

wait_worker() {
  for _ in $(seq 1 "${READINESS_TIMEOUT_SECONDS}"); do
    if [[ "$(container_health identity-policy-worker 2>/dev/null)" == "healthy" ]]; then
      return
    fi
    sleep 1
  done
  printf 'compose_smoke=FAIL service=identity-policy-worker reason=readiness_timeout\n' >&2
  return 1
}

run_chain() {
  run_one_shot identity-bootstrap
  run_one_shot identity-database-migrator
  run_one_shot identity-runtime-grants
  run_one_shot identity-workload-provisioner
  run_one_shot identity-finalize
}

start() {
  local runtime_mode=$1
  if [[ "${runtime_mode}" == "all" ]]; then
    bounded_compose "${COMPOSE_COMMAND_TIMEOUT_SECONDS}" build identity-api identity-policy-worker
  else
    bounded_compose "${COMPOSE_COMMAND_TIMEOUT_SECONDS}" build identity-api
  fi
  bounded_compose 60 up -d identity-db
  wait_database
  run_chain
  if [[ "${runtime_mode}" == "all" ]]; then
    bounded_compose 60 up -d --no-deps identity-api identity-policy-worker
  else
    bounded_compose 60 up -d --no-deps identity-api
  fi
  wait_api
  if [[ "${runtime_mode}" == "all" ]]; then
    wait_worker
  fi
}

rerun() {
  local runtime_mode=$1
  if [[ "${runtime_mode}" == "all" ]]; then
    bounded_compose 60 stop identity-api identity-policy-worker || true
  else
    bounded_compose 60 stop identity-api || true
  fi
  run_chain
  if [[ "${runtime_mode}" == "all" ]]; then
    bounded_compose 60 up -d --no-deps identity-api identity-policy-worker
  else
    bounded_compose 60 up -d --no-deps identity-api
  fi
  wait_api
  if [[ "${runtime_mode}" == "all" ]]; then
    wait_worker
  fi
}

case "${mode}" in
  start-all) start all ;;
  start-api) start api ;;
  rerun-all) rerun all ;;
  rerun-api) rerun api ;;
  verify-uids)
    run_uid_probe identity-bootstrap
    run_uid_probe identity-database-migrator
    run_uid_probe identity-runtime-grants
    run_uid_probe identity-workload-provisioner
    run_uid_probe identity-finalize
    ;;
  verify-provisioner-acl) verify_provisioner_acl ;;
  *)
    printf 'compose_smoke=FAIL reason=unknown_mode mode=%s\n' "${mode}" >&2
    exit 2
    ;;
esac
