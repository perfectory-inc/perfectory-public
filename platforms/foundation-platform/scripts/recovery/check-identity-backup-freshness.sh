#!/usr/bin/env bash
# Fails when an identity database's newest whole dump is older than the nightly cadence allows. A
# backup unit that never starts (timer off, host down at night) reports nothing itself; this does.
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY}"
# shellcheck source=identity-backup-lib.sh
source "${script_dir}/identity-backup-lib.sh"
max_age_hours=26 # one nightly run, its randomized start and a slow night

now="$(date -u +%s)"
stale=()
for target in "${IDENTITY_BACKUP_TARGETS[@]}"; do
    read -r _ _ name <<<"${target}"
    newest="$(dump_keys "${name}" | tail -n 1)"
    if [[ -z "${newest}" ]]; then
        stale+=("${name}:none")
        continue
    fi
    stamp="$(basename "${newest}" .dump.gpg)"
    taken="$(date -u -d "${stamp:0:8} ${stamp:8:2}:${stamp:10:2}:${stamp:12:2}" +%s)"
    age_hours=$(((now - taken) / 3600))
    printf 'identity-backup-age target=%s key=%s hours=%s\n' "${name}" "${newest}" "${age_hours}"
    [[ "${age_hours}" -lt "${max_age_hours}" ]] || stale+=("${name}:${age_hours}h")
done
[[ "${#stale[@]}" -eq 0 ]] || { printf 'identity backups older than %sh: %s\n' "${max_age_hours}" "${stale[*]}" >&2; exit 1; }
