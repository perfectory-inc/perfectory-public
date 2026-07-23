#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
evidence_dir="${FOUNDATION_RECOVERY_EVIDENCE_DIR:-${root_dir}/target/recovery}"
compose=(docker compose --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml" -f "${root_dir}/compose.recovery.yml" --profile recovery)
export MSYS2_ENV_CONV_EXCL="${MSYS2_ENV_CONV_EXCL:-};FOUNDATION_RECOVERY_REPOSITORY_PATH;FOUNDATION_RECOVERY_EVIDENCE_DIR"

mkdir -p "${evidence_dir}"
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup stanza-create
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup check

backup_type=diff
if ! "${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup info --output=json \
    | grep -q '"backup"[[:space:]]*:[[:space:]]*\[[^]]'; then
    backup_type=full
elif [[ "$(date -u +%u)" == "7" ]]; then
    backup_type=full
fi

"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup backup "--type=${backup_type}"
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup expire
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup check
"${compose[@]}" run --rm -T --interactive=false --no-deps foundation-backup pgbackrest-backup info --output=json \
    >"${evidence_dir}/backup-info.json"

printf 'postgres-backup-ok type=%s evidence=%s\n' "${backup_type}" "${evidence_dir}/backup-info.json"
