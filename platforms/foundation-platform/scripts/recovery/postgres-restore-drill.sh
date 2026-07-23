#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
evidence_dir="${FOUNDATION_RECOVERY_EVIDENCE_DIR:-${root_dir}/target/recovery}"
run_id="${FOUNDATION_RECOVERY_RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
[[ "${run_id}" =~ ^[0-9]{14}$ ]] || { printf 'recovery run id must be 14 UTC digits\n' >&2; exit 2; }
project_name="foundation-recovery-${run_id}"
compose=(docker compose --project-name "${project_name}" --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml" -f "${root_dir}/compose.recovery.yml" --profile recovery)
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export MSYS2_ENV_CONV_EXCL="${MSYS2_ENV_CONV_EXCL:-};FOUNDATION_RECOVERY_REPOSITORY_PATH;FOUNDATION_RECOVERY_EVIDENCE_DIR"

cleanup() {
    local exit_code=$?
    if [[ "${exit_code}" -ne 0 ]]; then
        mkdir -p "${evidence_dir}"
        "${compose[@]}" logs --no-color >"${evidence_dir}/failure-compose.log" 2>&1 || true
        cat "${evidence_dir}/failure-compose.log" >&2 || true
    fi
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
    trap - EXIT
    exit "${exit_code}"
}
trap cleanup EXIT

mkdir -p "${evidence_dir}"

"${compose[@]}" build postgres foundation-api
"${compose[@]}" up -d --wait postgres
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-bootstrap
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-migrate
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-runtime-grants
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-finalize
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup stanza-create
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup check
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup backup --type=full
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup info --output=json >"${evidence_dir}/backup-info.json"

source_psql=("${compose[@]}" run --rm -T --interactive=false --no-deps --entrypoint psql -e "PGPASSWORD=${FOUNDATION_ADMIN_PASSWORD}" foundation-runtime-grants -X -h postgres -U foundation_admin -d foundation)
"${source_psql[@]}" -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA IF NOT EXISTS recovery_drill; CREATE TABLE IF NOT EXISTS recovery_drill.recovery_probe (run_id text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT clock_timestamp())'
"${source_psql[@]}" -v ON_ERROR_STOP=1 -c "INSERT INTO recovery_drill.recovery_probe (run_id) VALUES ('${project_name}')"
recovery_target_name="${project_name//-/_}"
recovery_target_lsn="$("${source_psql[@]}" -Atc "SELECT pg_create_restore_point('${recovery_target_name}')")"
"${source_psql[@]}" -Atc 'SELECT pg_switch_wal()' >/dev/null
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup check

"${compose[@]}" stop postgres
"${compose[@]}" run --rm -T --interactive=false foundation-restore-drill pgbackrest-restore restore --type=name --target="${recovery_target_name}" --target-action=promote
"${compose[@]}" up -d --wait foundation-restored-postgres

restored_internal_url="postgres://foundation_migrator:${FOUNDATION_MIGRATOR_PASSWORD}@foundation-restored-postgres:5432/foundation"
"${compose[@]}" run --rm -T --interactive=false --no-deps -e PGHOST=foundation-restored-postgres foundation-bootstrap
"${compose[@]}" run --rm -T --interactive=false --no-deps -e "FOUNDATION_MIGRATOR_DATABASE_URL=${restored_internal_url}" foundation-migrate
"${compose[@]}" run --rm -T --interactive=false --no-deps -e PGHOST=foundation-restored-postgres foundation-runtime-grants
"${compose[@]}" run --rm -T --interactive=false --no-deps -e PGHOST=foundation-restored-postgres foundation-finalize

migration_state="$("${compose[@]}" run --rm -T --interactive=false --no-deps --entrypoint psql -e "PGPASSWORD=${FOUNDATION_ADMIN_PASSWORD}" foundation-runtime-grants -X -h foundation-restored-postgres -U foundation_admin -d foundation -Atc 'SELECT count(*) FROM _sqlx_migrations WHERE success')"
read_smoke="$("${compose[@]}" run --rm -T --interactive=false --no-deps --entrypoint psql -e "PGPASSWORD=${FOUNDATION_ADMIN_PASSWORD}" foundation-runtime-grants -X -h foundation-restored-postgres -U foundation_admin -d foundation -Atc "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")"
pitr_smoke="$("${compose[@]}" run --rm -T --interactive=false --no-deps --entrypoint psql -e "PGPASSWORD=${FOUNDATION_ADMIN_PASSWORD}" foundation-runtime-grants -X -h foundation-restored-postgres -U foundation_admin -d foundation -Atc "SELECT count(*) FROM recovery_drill.recovery_probe WHERE run_id = '${project_name}'")"

[[ "${migration_state}" -gt 0 ]] || { printf 'restored migration state is empty\n' >&2; exit 1; }
[[ "${read_smoke}" -gt 0 ]] || { printf 'restored database read smoke found no application tables\n' >&2; exit 1; }
[[ "${pitr_smoke}" == "1" ]] || { printf 'PITR marker was not recovered\n' >&2; exit 1; }

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat >"${evidence_dir}/evidence.json" <<EOF
{
  "schema": "foundation-platform.postgres-restore-rehearsal.v1",
  "project": "${project_name}",
  "started_at": "${started_at}",
  "finished_at": "${finished_at}",
  "recovery_target_name": "${recovery_target_name}",
  "recovery_target_lsn": "${recovery_target_lsn}",
  "migration_state": ${migration_state},
  "read_smoke": ${read_smoke},
  "pitr_smoke": ${pitr_smoke},
  "backup_info": "backup-info.json",
  "result": "pass"
}
EOF

printf 'restore-rehearsal-ok evidence=%s\n' "${evidence_dir}/evidence.json"
