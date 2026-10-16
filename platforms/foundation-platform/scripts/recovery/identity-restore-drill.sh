#!/usr/bin/env bash
# Weekly drill for the identity database backups (run-identity-backup.sh): with the recovery READER
# key, restore each database's newest dump into a throwaway container of its live image, compare it
# with the live database, then start the live Zitadel's own image and masterkey on the restored
# database and ask it for its signing keys. Rows alone do not prove a login: Zitadel's encryption
# keys (system.encryption_keys) are sealed with the masterkey, which lives outside the database.
# Everything runs on an internal network (no route out: a restored Zitadel must not send mail).
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
evidence_dir="${FOUNDATION_RECOVERY_EVIDENCE_DIR:?set FOUNDATION_RECOVERY_EVIDENCE_DIR}"
export AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY:?set FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY}"
# shellcheck source=identity-backup-lib.sh
source "${script_dir}/identity-backup-lib.sh"
# The pinned references tools/technology-versions.contract.json lists; the drill refuses to run when
# a live database or Zitadel runs another one (a dump restores into the version it came from).
PG_IMAGE="postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
SOCAT_IMAGE="alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e"
ZITADEL_IMAGE="ghcr.io/zitadel/zitadel:v2.65.1@sha256:013d23b69aa681f03d36a7fd61e4837a7b049a7e22bd7215eb3e98e9dbf5543c"
drill="identity-restore-drill-$(date -u +%Y%m%d%H%M%S)"

cleanup() {
    docker ps -aq --filter "label=${drill}" | xargs -r docker rm -f >/dev/null 2>&1 || true
    docker network rm "${drill}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create --internal --label "${drill}" "${drill}" >/dev/null

psql_in() {
    docker exec "$1" sh -c 'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' _ "$2"
}
tables_sql="SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"

results=()
for target in "${IDENTITY_BACKUP_TARGETS[@]}"; do
    read -r project service name <<<"${target}"
    live="$(service_container "${project}" "${service}")"
    key="$(dump_keys "${name}" | tail -n 1)"
    [[ -n "${key}" ]] || { printf 'no backup under postgres/%s/\n' "${name}" >&2; exit 1; }

    # A dump restores into the server version it came from: the drill runs the live database's image.
    live_image="$(docker inspect --format '{{.Config.Image}}' "${live}")"
    [[ "${live_image}" == "${PG_IMAGE}" ]] || { printf '%s runs %s, the drill %s\n' "${service}" "${live_image}" "${PG_IMAGE}" >&2; exit 1; }
    restored="${drill}-${name}"
    # The live service name is its network alias, so the restored Zitadel finds it where the live one does.
    docker run -d --name "${restored}" --label "${drill}" --network "${drill}" --network-alias="${service}" \
        --memory 1g -e POSTGRES_USER="$(container_env "${live}" POSTGRES_USER)" \
        -e POSTGRES_DB="$(container_env "${live}" POSTGRES_DB)" -e POSTGRES_HOST_AUTH_METHOD=trust \
        "${PG_IMAGE}" >/dev/null
    for _ in $(seq 60); do
        # shellcheck disable=SC2016 # the container's env
        docker exec "${restored}" sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1' >/dev/null 2>&1 && break
        sleep 1
    done
    # Roles are not in a database dump; the identity platform's bootstrap (bootstrap-identity.sql)
    # creates them before a real restore. The drill restores the objects under the superuser.
    # shellcheck disable=SC2016 # the container's env
    aws s3 cp --only-show-errors "s3://${bucket}/${key}" - </dev/null \
        | decrypt \
        | docker exec -i "${restored}" sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-privileges --exit-on-error'

    live_tables="$(psql_in "${live}" "${tables_sql}")"
    restored_tables="$(psql_in "${restored}" "${tables_sql}")"
    [[ "${restored_tables}" == "${live_tables}" ]] \
        || { printf '%s: restored %s tables, live has %s\n' "${name}" "${restored_tables}" "${live_tables}" >&2; exit 1; }
    case "${name}" in
        zitadel) witness="SELECT count(*) FROM eventstore.events2" ;;
        identity) witness="SELECT count(*) FROM identity.staff" ;;
    esac
    restored_rows="$(psql_in "${restored}" "${witness}")"
    live_rows="$(psql_in "${live}" "${witness}")"
    [[ "${restored_rows}" -gt 0 ]] || { printf '%s: the restore holds no rows of %s\n' "${name}" "${witness}" >&2; exit 1; }
    results+=("{\"target\":\"${name}\",\"key\":\"${key}\",\"tables\":${restored_tables},\"witness_rows\":${restored_rows},\"live_witness_rows\":${live_rows}}")
    printf 'identity-restore-ok target=%s key=%s tables=%s rows=%s live=%s\n' \
        "${name}" "${key}" "${restored_tables}" "${restored_rows}" "${live_rows}"
done

# Zitadel on the restored database: the live container's image, settings and masterkey. The values
# reach docker on a file descriptor (`--env-file /dev/fd/3`), never a command line or a file.
zitadel="$(service_container zitadel-platform zitadel)"
zitadel_image="$(docker inspect --format '{{.Config.Image}}' "${zitadel}")"
[[ "${zitadel_image}" == "${ZITADEL_IMAGE}" ]] || { printf 'Zitadel runs %s, the drill %s\n' "${zitadel_image}" "${ZITADEL_IMAGE}" >&2; exit 1; }
settings="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${zitadel}" \
    | grep -E '^ZITADEL_' | grep -vE '^ZITADEL_FIRSTINSTANCE_')"
while IFS="=" read -r var value; do
    printf -v "${var}" '%s' "${value}"
done <<<"${settings}"
[[ -n "${ZITADEL_MASTERKEY:-}" ]] || { printf 'the live Zitadel has no ZITADEL_MASTERKEY\n' >&2; exit 1; }
docker run -d --name "${drill}-zitadel-app" --label "${drill}" --network "${drill}" --memory 1g --env-file /dev/fd/3 \
    "${ZITADEL_IMAGE}" start --masterkeyFromEnv --tlsMode disabled >/dev/null 3<<<"${settings}"

# Zitadel finds its instance by the Host a client sends, the external domain and port. The probe
# keeps its side open (shut-none): Go's server cancels a request whose client half-closes.
host="${ZITADEL_EXTERNALDOMAIN}"
[[ "${ZITADEL_EXTERNALPORT:-}" =~ ^(|80|443)$ ]] || host="${host}:${ZITADEL_EXTERNALPORT}"
ask_zitadel() { # <path> [header line]; the token stays off every command line (printf is a builtin)
    printf 'GET %s HTTP/1.1\r\nHost: %s\r\n%sConnection: close\r\n\r\n' "$1" "${host}" "${2:+$2$'\r\n'}" \
        | docker run --rm -i --network "${drill}" "${SOCAT_IMAGE}" -t 30 - \
            "TCP:${drill}-zitadel-app:${ZITADEL_PORT:-8080},shut-none" 2>/dev/null || true
}
keys=""
for _ in $(seq 90); do
    keys="$(ask_zitadel /oauth/v2/keys)"
    [[ "${keys}" == *'"keys":[{'* ]] && break
    [[ "$(docker inspect --format '{{.State.Running}}' "${drill}-zitadel-app")" == true ]] || break
    sleep 2
done
# The public keys come back under any masterkey (proven 2026-10-12 with a wrong one): only a request
# Zitadel must decrypt something for proves the key. An access token is sealed with an encryption key
# that the masterkey seals, so a live token must read its own user back. The unit hands the token in
# as a systemd credential (LoadCredential=); a run by hand names the file.
token_file="${ZITADEL_DRILL_TOKEN_FILE:-${CREDENTIALS_DIRECTORY:?run from its unit or set ZITADEL_DRILL_TOKEN_FILE}/zitadel-token}"
pat="$(<"${token_file}")"
me="$(ask_zitadel /auth/v1/users/me "Authorization: Bearer ${pat}")"
if [[ "${keys}" != *'"keys":[{'* || "${me}" != 'HTTP/1.1 200 '* ]]; then
    printf 'the restored Zitadel did not serve its keys, or refused the live token (%s); its last log lines:\n' \
        "$(head -n 1 <<<"${me}" | tr -d '\r')" >&2
    docker logs --tail 20 "${drill}-zitadel-app" 2>&1 | sed 's/^/  /' >&2
    exit 1
fi
key_count="$(grep -o '"kid"' <<<"${keys}" | wc -l)"
# Ties this drill to the masterkey kept off the host without writing the key down.
fingerprint="$(printf '%s' "${ZITADEL_MASTERKEY}" | sha256sum | cut -c1-12)"

mkdir -p "${evidence_dir}"
printf '{"schema":"foundation-platform.identity-restore-drill.v1","restored":[%s],"zitadel_image":"%s","signing_keys":%s,"masterkey_sha256_prefix":"%s","result":"pass"}\n' \
    "$(IFS=,; printf '%s' "${results[*]}")" "${zitadel_image}" "${key_count}" "${fingerprint}" \
    >"${evidence_dir}/identity-restore-drill.json"
printf 'identity-restore-drill-ok zitadel_signing_keys=%s masterkey_sha256_prefix=%s\n' "${key_count}" "${fingerprint}"
