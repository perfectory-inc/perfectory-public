#!/usr/bin/env bash
# Fails when the newest PostgreSQL backup in the pgBackRest repository is older than the nightly
# cadence allows. The backup unit that never starts (timer off, host down at night) reports nothing
# itself; this does. It reads the repository, not the live database, so it needs no running Postgres.
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose=(docker compose --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml" -f "${root_dir}/compose.recovery.yml" --profile recovery)
export MSYS2_ENV_CONV_EXCL="${MSYS2_ENV_CONV_EXCL:-};FOUNDATION_RECOVERY_REPOSITORY_PATH;FOUNDATION_RECOVERY_EVIDENCE_DIR"
max_age_hours=26 # one nightly run, its randomized start and a slow night

# The restore-drill service carries the repository's read-only key, never the writer's; its
# `pgbackrest-backup` entrypoint branch runs pgbackrest as-is without touching the restore volume.
info="$("${compose[@]}" run --rm -T --interactive=false --no-deps foundation-restore-drill pgbackrest-backup info --output=json)"
# Newest backup stop time across every stanza; any unexpected shape fails closed.
newest="$(python3 -c '
import json, sys
stops = [b["timestamp"]["stop"] for s in json.load(sys.stdin) for b in s.get("backup", [])]
if not all(isinstance(t, int) for t in stops):
    sys.exit("pgBackRest info: a backup stop time is not an integer")
print(max(stops) if stops else "")
' <<<"${info}")"
[[ -n "${newest}" ]] || { printf 'postgres backup: the pgBackRest repository holds no backup\n' >&2; exit 1; }

now="$(date -u +%s)"
age_hours=$(((now - newest) / 3600))
printf 'postgres-backup-age stop=%s hours=%s\n' "${newest}" "${age_hours}"
[[ "${age_hours}" -lt "${max_age_hours}" ]] || { printf 'postgres backup older than %sh: %sh\n' "${max_age_hours}" "${age_hours}" >&2; exit 1; }
