#!/usr/bin/env bash
# Fails when an identity database's newest whole dump is older than the nightly cadence allows. A
# backup unit that never starts (timer off, host down at night) reports nothing itself; this does.
#
# It lists with the release's own publisher (list-recovery-objects, the pinned Rust object-store
# client) and the recovery bucket's read-only pair, and starts no container: it ran the aws-cli image
# only to list, and 4 of its runs in 14 days (2026-11-12..17) hung in that container start, silent,
# under 37-48% host iowait until the unit's 10-minute limit; a run is otherwise 30-40 s (W83c).
set -Eeuo pipefail
# A publisher that fails inside $(...) fails the check; it is not read as "no dump".
shopt -s inherit_errexit

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID}"
: "${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY}"
# shellcheck source=identity-backup-lib.sh
source "${script_dir}/identity-backup-lib.sh"
# PUBLISHER_BIN: the current release's verified publisher.
# shellcheck source=../ops/admitted-writer-runtime.sh
source "${script_dir}/../ops/admitted-writer-runtime.sh" --current
max_age_hours=26 # one nightly run, its randomized start and a slow night

listing="$(mktemp)"
trap 'rm -f "${listing}"' EXIT
# Whole dumps of postgres/<name>/, oldest first: dump_keys of identity-backup-lib.sh, through the publisher.
listed_dump_keys() {
    FOUNDATION_PLATFORM_RECOVERY_OBJECT_LISTING_PREFIX="postgres/$1/" \
        FOUNDATION_PLATFORM_RECOVERY_OBJECT_LISTING_OUTPUT="${listing}" \
        "${PUBLISHER_BIN}" list-recovery-objects >/dev/null
    python3 -I -c '
import json, sys
lines = open(sys.argv[1], encoding="utf-8")
next(lines)  # the header line
for line in lines:
    print(json.loads(line)["key"])
' "${listing}" | grep -E "${DUMP_KEY_PATTERN}" | sort || true
}

now="$(date -u +%s)"
stale=()
for target in "${IDENTITY_BACKUP_TARGETS[@]}"; do
    read -r _ _ name <<<"${target}"
    newest="$(listed_dump_keys "${name}" | tail -n 1)"
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
