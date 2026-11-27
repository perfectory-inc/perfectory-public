#!/usr/bin/env bash
# The staging smoke (root ADR-0177): a new release runs a few real inputs before production switches
# to it. foundation-release.sh `staging-smoke <sha>` starts this from the installed, not yet current,
# release as a transient unit (User=foundation-platform, the runtime-secrets contract's run
# `staging-smoke`); foundation-deploy.sh calls that between the admission build and the switch, and
# a failure here refuses the deploy.
#
# Staging is production's host, bucket, database server and credentials, isolated by name:
#   - FOUNDATION_PLATFORM_RUNTIME_ENV=staging, set here, puts every R2 key the publisher touches under
#     staging/ and refuses any other (R2KeyNamespace in crates/foundation-outbox);
#   - the same variable makes database-url.sh name foundation_staging, which this run drops and
#     creates from empty with the release's own bootstrap, migrations, grants and finalize.
#
# Steps, each bounded by config/staging-gate.contract.json; the first that fails stops the run:
#   clear     empty staging/ (the previous run's objects), through the release's own R2 client and its
#             staging namespace guard
#   image     build the release's runtime image (its migrator) under a staging image name
#   database  foundation_staging from empty through the release's schema chain
#
# The smoke judges the release and asks no provider (root ADR-0363, amended 2026-11-27). Until then it
# also read the hub and VWorld plans, the VWorld inventory and a sample download against the live sites,
# and a provider answer the release did not recognise refused every release: 2026-11-27 09:35-09:44Z
# VWorld's scheduled maintenance redirected its dataset page to a service notice, the inventory parse
# failed and five releases in a row were not deployed, the fixes among them. How the release reads a
# provider is judged before merge, against recorded responses, by the collectors' own tests in CI
# (collection-infrastructure's VWorld and hub clients, the publisher's VWorld file ingest, the R2
# client's single and multipart writes); whether a provider answers today is the collector's own run
# check and alert in production (source_sweep, root ADR-0260).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

required_env=(
  FOUNDATION_ADMIN_PASSWORD
  FOUNDATION_MIGRATOR_PASSWORD
  FOUNDATION_API_PASSWORD
  FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER
  FOUNDATION_PLATFORM_EXECUTION_CONTEXT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "staging-smoke: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

# The one switch. Whatever the loaded environment files say, this run is staging.
export FOUNDATION_PLATFORM_RUNTIME_ENV=staging
staging_database="$(foundation_database_name)"
if [[ "${staging_database}" == "$(FOUNDATION_PLATFORM_RUNTIME_ENV=production foundation_database_name)" ]]; then
  echo "staging-smoke: refused: staging names the production database ${staging_database}" >&2
  exit 78
fi
DATABASE_URL="$(foundation_database_url)"
export DATABASE_URL

CONTRACT="${RELEASE_ROOT}/config/staging-gate.contract.json"
STATE_ROOT="${FOUNDATION_STAGING_SMOKE_STATE_ROOT:-/data/foundation-platform/staging-smoke}"
# The Docker CLI keeps its state under DOCKER_CONFIG, and the service account's home is
# /nonexistent: the first real smoke (2026-10-10) failed its image step with "mkdir /nonexistent:
# permission denied". Same answer as raon-large-files.sh; set after admitted-writer-runtime.sh,
# which unsets any DOCKER_CONFIG a caller passed.
export DOCKER_CONFIG="${STATE_ROOT}/docker-config"
POSTGRES_PROJECT="${FOUNDATION_PLATFORM_COMPOSE_PROJECT:-foundation-platform-runtime}"
run_log="${STATE_ROOT}/run.log"
contract() {
  python3 -I - "${CONTRACT}" "$@" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for key in sys.argv[2:]:
    value = value[key]
print(value)
PY
}

postgres_container() {
  local ids
  ids="$(docker ps -q --filter "label=com.docker.compose.project=${POSTGRES_PROJECT}" \
    --filter label=com.docker.compose.service=postgres)"
  [[ "$(wc -w <<<"${ids}")" == 1 ]] || { echo "expected one running postgres of ${POSTGRES_PROJECT}, found: ${ids:-none}" >&2; return 1; }
  printf '%s\n' "${ids}"
}

# psql inside the runtime's postgres container, on the staging database, reading SQL from stdin.
staging_psql() {
  local role="$1" password="$2" database="$3"
  shift 3
  PGPASSWORD="${password}" docker exec -i -e PGPASSWORD -e FOUNDATION_MIGRATOR_PASSWORD -e FOUNDATION_API_PASSWORD \
    "$(postgres_container)" psql -X -q -v ON_ERROR_STOP=1 -U "${role}" -d "${database}" "$@"
}

step_clear() {
  "${PUBLISHER_BIN}" clear-staging-namespace
}

# Its own image name: the production runtime's foundation-platform-runtime:local is not moved
# before the switch. The container policy wants both the context and the image spelled literally.
step_image() {
  cd "${RELEASE_ROOT}"
  docker build --quiet --file services/foundation-api/Dockerfile --tag foundation-platform-staging-runtime:local .
}

step_database() {
  # DROP/CREATE DATABASE cannot run inside a transaction; psql runs each -c on its own.
  staging_psql foundation_admin "${FOUNDATION_ADMIN_PASSWORD}" postgres \
    -c "DROP DATABASE IF EXISTS ${staging_database} WITH (FORCE)" -c "CREATE DATABASE ${staging_database}"
  staging_psql foundation_admin "${FOUNDATION_ADMIN_PASSWORD}" "${staging_database}" -f - \
    <"${RELEASE_ROOT}/infra/compose/bootstrap-foundation.sql"
  FOUNDATION_MIGRATOR_DATABASE_URL="$(foundation_database_url migrator)" \
    docker run --rm --network host --memory 256m -e FOUNDATION_MIGRATOR_DATABASE_URL \
    --entrypoint /usr/local/bin/foundation-migrate foundation-platform-staging-runtime:local
  staging_psql foundation_migrator "${FOUNDATION_MIGRATOR_PASSWORD}" "${staging_database}" -f - \
    <"${RELEASE_ROOT}/infra/compose/grant-foundation-runtime.sql"
  staging_psql foundation_admin "${FOUNDATION_ADMIN_PASSWORD}" "${staging_database}" -f - \
    <"${RELEASE_ROOT}/infra/compose/finalize-foundation.sql"
}

# One step in its own process, so `timeout` can bound it: `staging-smoke.sh --step <name>`.
if [[ "${1:-}" == --step ]]; then
  "step_${2:?}"
  exit 0
fi

mkdir -p "${STATE_ROOT}" "${DOCKER_CONFIG}"
: >"${run_log}"
durations=()
run_step() {
  local name="$1" limit started status=0
  limit="$(contract steps "${name}" timeout_seconds)"
  started="${SECONDS}"
  timeout --kill-after=30 "${limit}" bash "${BASH_SOURCE[0]}" --step "${name}" >>"${run_log}" 2>&1 || status=$?
  if [[ "${status}" != 0 ]]; then
    local why="exit ${status}"
    [[ "${status}" != 124 ]] || why="over its ${limit}s limit"
    echo "staging-smoke: ${name} FAILED (${why}) for release ${RELEASE_ID}; the release is not deployed (root ADR-0177)" >&2
    job_run_log_tail "${run_log}"
    exit 1
  fi
  durations+=("${name}=$((SECONDS - started))s")
  echo "staging-smoke: ${name} ok in $((SECONDS - started))s"
}

run_step clear
run_step image
run_step database
echo "staging-smoke: passed release=${RELEASE_ID} ${durations[*]}"
