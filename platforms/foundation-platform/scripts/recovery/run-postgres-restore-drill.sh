#!/usr/bin/env bash
# Weekly drill for the production PostgreSQL backup (run-postgres-backup.sh), the twin of
# identity-restore-drill.sh (root ADR-0300 K). With the repository's READ-ONLY key it restores the
# newest backup set and replays the archived WAL to its end into throwaway volumes of the live
# image, starts it with archiving off, and compares it with the live database: the same server
# version, tables and applied migrations, and every table the live database holds rows in has rows.
# A backup nobody restores is a hope: the nightly backup did not run at all from 2026-07-26 to
# 10-02 and nothing said so, and the only restores of production so far were by hand (10-12, 10-14).
#
# postgres-restore-drill.sh is a different thing: a synthetic rehearsal that makes its own backup
# first, so it must never run against this repository. This one writes nothing to it: the reader
# key cannot, and --archive-mode=off keeps the promoted copy from pushing WAL.
#
# The whole set, every week, is a declared full read (the owner's partial-processing rule): only a
# whole restore proves the backup restores. Measured 2026-10-14 by hand: 130.1GB, 2,664 files, 23
# minutes; R2 reads are Class B and egress is free.
set -Eeuo pipefail

# The release this run started from, not whatever `current` points at after a deploy mid-drill.
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
evidence_dir="${FOUNDATION_RECOVERY_EVIDENCE_DIR:?set FOUNDATION_RECOVERY_EVIDENCE_DIR}"
live_project="${FOUNDATION_RESTORE_DRILL_LIVE_PROJECT:-foundation-platform-runtime}"
drill="foundation-restore-drill-$(date -u +%Y%m%d%H%M%S)"
restored="${drill}-postgres"
recovery_wait_seconds="${FOUNDATION_RESTORE_DRILL_RECOVERY_WAIT_SECONDS:-1800}"
compose=(docker compose --project-name "${drill}" --project-directory "${root_dir}"
    -f "${root_dir}/docker-compose.yml" -f "${root_dir}/compose.recovery.yml" --profile recovery)
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cleanup() {
    # The restored volumes are the size of the database; they never outlive the drill.
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

mapfile -t found < <(docker ps -q --filter "label=com.docker.compose.project=${live_project}" \
    --filter "label=com.docker.compose.service=postgres")
[[ "${#found[@]}" -eq 1 ]] || { printf 'expected one running postgres of %s, found %s\n' "${live_project}" "${#found[@]}" >&2; exit 1; }
live="${found[0]}"
psql_at() { # <container> <sql>: the container's own socket, as the live checks of this host do
    docker exec "$1" psql -X -v ON_ERROR_STOP=1 -U foundation_admin -d foundation -Atc "$2"
}

# The newest set and its size, from the repository itself (the read-only key, as the freshness check).
info="$("${compose[@]}" run --rm -T --interactive=false --no-deps foundation-restore-drill pgbackrest-backup info --output=json)"
read -r backup_label backup_bytes < <(python3 -c '
import json, sys
sets = [b for s in json.load(sys.stdin) for b in s.get("backup", [])]
if not sets:
    sys.exit("pgBackRest info: the repository holds no backup")
newest = max(sets, key=lambda b: b["timestamp"]["stop"])
print(newest["label"], int(newest["info"]["size"]))
' <<<"${info}")

# The restore lands in docker volumes: refuse to start one the disk cannot hold (a fifth spare for WAL).
docker_root="$(docker info --format '{{.DockerRootDir}}')"
free_bytes="$(($(df -Pk "${docker_root}" | awk 'NR == 2 {print $4}') * 1024))"
need_bytes="$((backup_bytes + backup_bytes / 5))"
[[ "${free_bytes}" -ge "${need_bytes}" ]] \
    || { printf 'restore of %s needs %s bytes under %s, %s free\n' "${backup_label}" "${need_bytes}" "${docker_root}" "${free_bytes}" >&2; exit 1; }

# The default target is the end of the archived WAL, so the drill proves the archive as well as the set.
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-restore-drill \
    pgbackrest-restore restore --archive-mode=off
restore_seconds="$(($(date -u +%s) - $(date -u -d "${started_at}" +%s)))"

# Archiving off and no TCP listener: the copy serves nobody and pushes nothing.
"${compose[@]}" run -d --no-deps --name "${restored}" foundation-restored-postgres \
    postgres -c archive_mode=off -c "listen_addresses=" >/dev/null
promoted=""
for _ in $(seq "$((recovery_wait_seconds / 5))"); do
    [[ "$(docker inspect --format '{{.State.Running}}' "${restored}")" == true ]] || break
    if [[ "$(psql_at "${restored}" 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" == t ]]; then
        promoted=yes
        break
    fi
    sleep 5
done
if [[ -z "${promoted}" ]]; then
    printf 'the restored %s did not finish recovery within %ss; its last log lines:\n' "${backup_label}" "${recovery_wait_seconds}" >&2
    docker logs --tail 20 "${restored}" 2>&1 | sed 's/^/  /' >&2
    exit 1
fi

failures=()
version_sql="SHOW server_version_num"
tables_sql="SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
migrations_sql="SELECT count(*) FROM _sqlx_migrations WHERE success"
for check in version tables migrations; do
    sql_var="${check}_sql"
    live_value="$(psql_at "${live}" "${!sql_var}")"
    restored_value="$(psql_at "${restored}" "${!sql_var}")"
    printf -v "live_${check}" '%s' "${live_value}"
    printf -v "restored_${check}" '%s' "${restored_value}"
    [[ "${restored_value}" == "${live_value}" ]] || failures+=("${check}: restored ${restored_value}, live ${live_value}")
done

# Rows, not only tables: every table the live statistics say holds rows must hold one in the copy.
# The live side reads statistics only, no table scans on production (a count of the parcel mirror
# took ten minutes of production IO on 2026-10-12); the copy is ours to scan.
mapfile -t nonempty < <(psql_at "${live}" \
    "SELECT format('%I.%I', schemaname, relname) FROM pg_stat_user_tables WHERE n_live_tup > 0 ORDER BY 1")
[[ "${#nonempty[@]}" -gt 0 ]] || failures+=("the live statistics name no table with rows")
empty_in_restore=()
for table in "${nonempty[@]}"; do
    [[ "$(psql_at "${restored}" "SELECT EXISTS (SELECT FROM ${table})")" == t ]] || empty_in_restore+=("${table}")
done
[[ "${#empty_in_restore[@]}" -eq 0 ]] || failures+=("empty in the restore: ${empty_in_restore[*]}")
recovered_to="$(psql_at "${restored}" "SELECT coalesce(to_char(pg_last_xact_replay_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '')")"

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
result=pass
[[ "${#failures[@]}" -eq 0 ]] || result=fail
mkdir -p "${evidence_dir}"
python3 - "${evidence_dir}/postgres-restore-drill.json" <<EOF
import json, sys
json.dump({
    "schema": "foundation-platform.postgres-restore-drill.v1",
    "backup_label": "${backup_label}", "backup_bytes": ${backup_bytes},
    "started_at": "${started_at}", "finished_at": "${finished_at}", "restore_seconds": ${restore_seconds},
    "recovered_to": "${recovered_to}" or None,
    "server_version_num": "${restored_version}", "tables": ${restored_tables:-0},
    "migrations": ${restored_migrations:-0}, "nonempty_tables_checked": ${#nonempty[@]},
    "result": "${result}",
}, open(sys.argv[1], "w"), indent=1)
EOF
if [[ "${result}" != pass ]]; then
    printf 'postgres restore drill of %s failed:\n' "${backup_label}" >&2
    printf '  %s\n' "${failures[@]}" >&2
    exit 1
fi
printf 'postgres-restore-drill-ok backup=%s bytes=%s restore_seconds=%s recovered_to=%s tables=%s migrations=%s nonempty_tables=%s\n' \
    "${backup_label}" "${backup_bytes}" "${restore_seconds}" "${recovered_to:-none}" "${restored_tables}" \
    "${restored_migrations}" "${#nonempty[@]}"
