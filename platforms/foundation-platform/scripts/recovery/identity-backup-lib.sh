# Shared by the identity database backup, its freshness check and its restore drill (sourced, not run).
# The two databases login needs, each a compose service whose container names its own superuser and
# database in POSTGRES_USER / POSTGRES_DB:
#   <compose project> <compose service> <name under postgres/ in the recovery bucket>
IDENTITY_BACKUP_TARGETS=(
    "zitadel-platform zitadel-db zitadel"
    "identity-platform-runtime identity-db identity"
)
# The pinned reference tools/technology-versions.contract.json lists.
AWS_IMAGE="amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
# Object keys are postgres/<name>/<UTC yyyymmddHHMMSS>.dump.gpg; an upload is .partial until it is whole.
DUMP_KEY_PATTERN='/[0-9]{14}\.dump\.gpg$'

: "${FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS:?set FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS}"
bucket="${FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET:?set FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET}"
endpoint="https://${FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST:?set FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST}"

aws() {
    docker run --rm -i --user "$(id -u):$(id -g)" -e HOME=/tmp -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        -e AWS_DEFAULT_REGION=auto "${AWS_IMAGE}" --endpoint-url "${endpoint}" "$@"
}

# gpg's symmetric mode authenticates what it decrypts (a truncated or altered dump fails), which a
# bare CBC cipher does not. The pass reaches gpg on a file descriptor, never argv.
gpg_with_pass() {
    local home status=0
    home="$(mktemp -d)"
    gpg --homedir "${home}" --batch --quiet --pinentry-mode loopback --passphrase-fd 3 "$@" \
        3<<<"${FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS}" || status=$?
    rm -rf "${home}"
    return "${status}"
}
encrypt() { gpg_with_pass --symmetric --cipher-algo AES256 --compress-algo none --output - ; }
decrypt() { gpg_with_pass --decrypt --output - ; }

# The one running container of a compose service.
service_container() {
    local found
    mapfile -t found < <(docker ps -q --filter "label=com.docker.compose.project=$1" \
        --filter "label=com.docker.compose.service=$2")
    [[ "${#found[@]}" -eq 1 ]] || { printf 'expected one running %s of %s, found %s\n' "$2" "$1" "${#found[@]}" >&2; return 1; }
    printf '%s\n' "${found[0]}"
}

container_env() {
    docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$1" | sed -n "s/^$2=//p"
}

# Whole dumps of postgres/<name>/, oldest first.
dump_keys() {
    aws s3api list-objects-v2 --bucket "${bucket}" --prefix "postgres/$1/" --query 'Contents[].Key' --output text \
        </dev/null | tr '\t' '\n' | grep -E "${DUMP_KEY_PATTERN}" | sort || true
}
