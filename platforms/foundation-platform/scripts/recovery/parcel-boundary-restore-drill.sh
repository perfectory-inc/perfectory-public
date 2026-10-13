#!/usr/bin/env bash
# Restores the newest production backup into a throwaway side instance and checks that it holds
# the two PostGIS parcel boundary tables whole (root ADR-0194 §6, §8 first way back).
#
#   parcel-boundary-restore-drill.sh <work directory on /data>
#
# Before serving_postgis.parcel_boundary_mirror and parcel_boundary_publication are dropped, the
# first way to get them back — the pgBackRest backup — must have been seen to work. This:
#
# 1. reads, from production, only the two ledgers that state how many rows each table holds: the
#    mirror's succeeded national rebuild run (parcel_boundary_mirror_rebuild_run) and the parcels
#    unit's succeeded projection load (spatial_projection_load). The tables themselves are not
#    read, so their scan counters, the evidence that nothing reads them, do not move;
# 2. restores the newest backup with the production container's image, configuration and
#    repository settings into <work>, with `--archive-mode=off`: a promoted side instance must
#    never push WAL into the production repository;
# 3. starts it with no TCP listener and archive_mode=off, waits for the end of recovery, and
#    counts each table's rows for that run and that load;
# 4. prints one JSON verdict and exits 0 when both counts equal the ledgers, 1 otherwise.
#
# The side instance and the work directory are removed afterwards (FOUNDATION_RESTORE_DRILL_KEEP=1
# keeps them for inspection). Measured 2026-10-12 for the catalog tables: 23 minutes. No value of
# the repository's settings is printed; the file that carries them is mode 600 and removed.
set -euo pipefail

work="${1:?usage: parcel-boundary-restore-drill.sh <work directory on /data>}"
production="${FOUNDATION_RESTORE_DRILL_SOURCE_CONTAINER:-foundation-platform-runtime-postgres-1}"
name="foundation-parcel-boundary-restore-drill-$(date -u +%Y%m%d%H%M%S)"
data_root="${FOUNDATION_RESTORE_DRILL_DATA_ROOT:-/data}"
[[ "${work}" == "${data_root}"/* ]] || { echo "restore-drill: refused: the work directory must be under ${data_root}, the root disk is too small" >&2; exit 64; }
[[ ! -e "${work}" ]] || { echo "restore-drill: refused: ${work} exists; a drill starts empty" >&2; exit 64; }

production_sql() {
  docker exec -i "${production}" sh -c 'psql -X -U "$POSTGRES_USER" -d foundation -v ON_ERROR_STOP=1 -At -F,' <<<"$1"
}

# 1. What production says each table holds.
IFS=, read -r mirror_run mirror_rows < <(production_sql "
  SELECT id, loaded_row_count FROM serving_postgis.parcel_boundary_mirror_rebuild_run
  WHERE status = 'succeeded' AND publication_scope ->> 'kind' = 'national'
  ORDER BY finished_at DESC LIMIT 1")
IFS=, read -r load_id load_rows < <(production_sql "
  SELECT load.id, load.loaded_row_count FROM serving_postgis.spatial_projection_load AS load
  JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = load.publication_unit_id
  WHERE unit.unit_key = 'parcels' AND load.status = 'succeeded'
  ORDER BY load.finished_at DESC LIMIT 1")
[[ -n "${mirror_run:-}" && -n "${load_id:-}" ]] || { echo "restore-drill: refused: production names no mirror run or projection load" >&2; exit 65; }

image="$(docker inspect "${production}" --format '{{.Config.Image}}')"
config="$(docker inspect "${production}" --format '{{range .Mounts}}{{if eq .Destination "/etc/pgbackrest/pgbackrest.conf"}}{{.Source}}{{end}}{{end}}')"
[[ -n "${config}" ]] || { echo "restore-drill: refused: the production container mounts no pgbackrest.conf" >&2; exit 65; }
settings_from_production() {
  docker inspect "${production}" --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^PGBACKREST_' || true
}
max_settings="$(production_sql "SELECT string_agg('-c ' || name || '=' || setting, ' ') FROM pg_settings
  WHERE name IN ('max_connections', 'max_worker_processes', 'max_wal_senders',
                 'max_prepared_transactions', 'max_locks_per_transaction')")"

mkdir -p "${work}"
env_file="${work}/repository.env"
cleanup() {
  rm -f "${env_file}"
  if [[ "${FOUNDATION_RESTORE_DRILL_KEEP:-0}" != 1 ]]; then
    docker rm -f "${name}" >/dev/null 2>&1 || true
    # The restored files belong to the image's postgres user.
    docker run --rm -v "${work}:/w" --entrypoint sh "${image}" -c 'rm -rf /w/pgdata /w/tablespaces /w/spool /w/lock' >/dev/null 2>&1 || true
    rmdir "${work}" 2>/dev/null || true
  fi
}
trap cleanup EXIT
(umask 077; settings_from_production >"${env_file}")
[[ -s "${env_file}" ]] || { echo "restore-drill: refused: the production container carries no PGBACKREST_ settings" >&2; exit 65; }

volumes=(-v "${config}:/etc/pgbackrest/pgbackrest.conf:ro" -v "${work}/pgdata:/var/lib/postgresql/data"
         -v "${work}/tablespaces:/tablespaces" -v "${work}/spool:/var/spool/pgbackrest" -v "${work}/lock:/var/lock/pgbackrest")
docker run --rm -v "${work}:/w" --entrypoint sh "${image}" \
  -c 'mkdir -p /w/pgdata /w/tablespaces /w/spool /w/lock && chown -R postgres:postgres /w/pgdata /w/tablespaces /w/spool /w/lock && chmod 700 /w/pgdata'

# 2. Restore.
started=${SECONDS}
docker run --rm --user postgres --env-file "${env_file}" "${volumes[@]}" --entrypoint pgbackrest "${image}" \
  --stanza=foundation --type=immediate --target-action=promote --archive-mode=off restore
restore_seconds=$((SECONDS - started))

# 3. Start, recover, count.
# shellcheck disable=SC2086 # max_settings is "-c name=value" pairs from pg_settings
docker run -d --name "${name}" --user postgres "${volumes[@]}" --entrypoint postgres "${image}" \
  -c archive_mode=off -c listen_addresses= ${max_settings} >/dev/null
restored_sql() { docker exec -i "${name}" psql -X -U "${FOUNDATION_RESTORE_DRILL_USER:-foundation_admin}" -d foundation -v ON_ERROR_STOP=1 -At <<<"$1"; }
for _ in $(seq 1 720); do
  [[ "$(restored_sql 'SELECT pg_is_in_recovery()' 2>/dev/null || true)" == f ]] && break
  sleep 5
done
[[ "$(restored_sql 'SELECT pg_is_in_recovery()')" == f ]] || { echo "restore-drill: the side instance did not finish recovery in an hour" >&2; exit 1; }
restored_mirror="$(restored_sql "SELECT count(*) FROM serving_postgis.parcel_boundary_mirror WHERE rebuild_run_id = '${mirror_run}'")"
restored_load="$(restored_sql "SELECT count(*) FROM serving_postgis.parcel_boundary_publication WHERE projection_load_id = '${load_id}'")"
backup="$(restored_sql "SELECT pg_size_pretty(pg_database_size('foundation'))")"

# 4. Verdict.
equal=false
[[ "${restored_mirror}" == "${mirror_rows}" && "${restored_load}" == "${load_rows}" ]] && equal=true
printf '{"equal": %s, "restore_seconds": %d, "restored_database_size": "%s", "mirror": {"rebuild_run_id": "%s", "ledger_rows": %s, "restored_rows": %s}, "publication": {"projection_load_id": "%s", "ledger_rows": %s, "restored_rows": %s}}\n' \
  "${equal}" "${restore_seconds}" "${backup}" "${mirror_run}" "${mirror_rows}" "${restored_mirror}" "${load_id}" "${load_rows}" "${restored_load}"
[[ "${equal}" == true ]]
