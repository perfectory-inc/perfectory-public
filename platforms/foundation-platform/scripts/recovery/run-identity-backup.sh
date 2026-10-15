#!/usr/bin/env bash
# Nightly logical backup of the databases login needs (identity-backup-lib.sh names them) into the
# Postgres recovery bucket, beside the foundation's pgBackRest repository
# (docs/runbooks/foundation-platform-low-cost-production-hardening.md). They are small and run
# without WAL archiving, so a pg_dump is the whole backup: encrypted with the recovery repository's
# cipher pass and streamed to R2, never written to this host's disk.
#
# Without an argument it backs up every target, each in its own process so one failing does not
# skip the others, and fails if any did. With a target's name it backs up that one.
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
evidence_dir="${FOUNDATION_RECOVERY_EVIDENCE_DIR:?set FOUNDATION_RECOVERY_EVIDENCE_DIR}"
export AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY}"
# shellcheck source=identity-backup-lib.sh
source "${script_dir}/identity-backup-lib.sh"
retention_days=35 # the foundation repository's repo1-retention-full (infra/postgres/pgbackrest.conf)

if [[ "$#" -eq 0 ]]; then
    failed=()
    for target in "${IDENTITY_BACKUP_TARGETS[@]}"; do
        read -r _ _ name <<<"${target}"
        "${BASH_SOURCE[0]}" "${name}" || failed+=("${name}")
    done
    [[ "${#failed[@]}" -eq 0 ]] || { printf 'identity backup failed for: %s\n' "${failed[*]}" >&2; exit 1; }
    exit 0
fi

name="$1"
for target in "${IDENTITY_BACKUP_TARGETS[@]}"; do
    read -r project service candidate <<<"${target}"
    [[ "${candidate}" == "${name}" ]] && break
done
[[ "${candidate}" == "${name}" ]] || { printf 'unknown identity backup target %s\n' "${name}" >&2; exit 64; }
container="$(service_container "${project}" "${service}")"

prefix="postgres/${name}"
key="${prefix}/$(date -u +%Y%m%d%H%M%S).dump.gpg"
partial="${key}.partial"
# A dump cut off midway still uploads what came through; it must never sit under a whole dump's name.
remove_partial() { aws s3api delete-object --bucket "${bucket}" --key "${partial}" </dev/null >/dev/null 2>&1 || true; }
trap remove_partial ERR
trap 'remove_partial; exit 143' TERM INT

# shellcheck disable=SC2016 # expanded by the container's shell, from the container's own env
docker exec "${container}" sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' \
    | encrypt \
    | aws s3 cp --only-show-errors - "s3://${bucket}/${partial}"

size="$(aws s3api head-object --bucket "${bucket}" --key "${partial}" --query ContentLength --output text </dev/null)"
[[ "${size}" =~ ^[0-9]+$ && "${size}" -gt 0 ]] || { printf 'uploaded %s is empty\n' "${partial}" >&2; false; }
aws s3 mv --only-show-errors "s3://${bucket}/${partial}" "s3://${bucket}/${key}" </dev/null
trap - ERR TERM INT

# Keys carry their UTC time, so the expired ones sort before the cutoff.
cutoff="${prefix}/$(date -u -d "${retention_days} days ago" +%Y%m%d%H%M%S)"
expired=0
while read -r old; do
    [[ -n "${old}" && "${old}" != "None" && "${old}" < "${cutoff}" ]] || continue
    aws s3api delete-object --bucket "${bucket}" --key "${old}" </dev/null >/dev/null
    expired=$((expired + 1))
done < <(aws s3api list-objects-v2 --bucket "${bucket}" --prefix "${prefix}/" --query 'Contents[].Key' --output text </dev/null | tr '\t' '\n')

mkdir -p "${evidence_dir}"
printf '{"schema":"foundation-platform.identity-backup.v1","target":"%s","key":"%s","bytes":%s,"expired":%s}\n' \
    "${name}" "${key}" "${size}" "${expired}" >"${evidence_dir}/identity-backup-${name}.json"
printf 'identity-backup-ok target=%s key=%s bytes=%s expired=%s\n' "${name}" "${key}" "${size}" "${expired}"
