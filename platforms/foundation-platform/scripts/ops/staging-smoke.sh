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
#   clear     empty staging/ (the previous run's sample)
#   image     build the release's runtime image (its migrator) under a staging image name
#   database  foundation_staging from empty through the release's schema chain
#   plan      hub and VWorld plans and the VWorld file inventory, read-only, against the real providers
#   ingest    a sample of the inventory through the daily sweep's own VWorld lane settings: the two
#             smallest files (single PUT) and one large enough for the multipart path
#   measure   bronze-object-members on what landed, with the read-only key pair
#
# Every check counts what happened: a lane that lands nothing, or a measure that measures nothing,
# fails the smoke rather than passing it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/vworld-login.sh"
source "$(dirname "${BASH_SOURCE[0]}")/vworld-sweep-lane.sh"
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

required_env=(
  FOUNDATION_ADMIN_PASSWORD
  FOUNDATION_MIGRATOR_PASSWORD
  FOUNDATION_API_PASSWORD
  FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER
  FOUNDATION_PLATFORM_EXECUTION_CONTEXT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
for name in $(vworld_login_missing "${RELEASE_ROOT}/config/environment-variable-naming.contract.json"); do
  missing+=("${name}")
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
CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
STATE_ROOT="${FOUNDATION_STAGING_SMOKE_STATE_ROOT:-/data/foundation-platform/staging-smoke}"
SPOOL_DIR="${STATE_ROOT}/spool"
POSTGRES_PROJECT="${FOUNDATION_PLATFORM_COMPOSE_PROJECT:-foundation-platform-runtime}"
hub_plan="${STATE_ROOT}/hub-plan.json"
vworld_plan="${STATE_ROOT}/vworld-plan.json"
inventory="${STATE_ROOT}/vworld-inventory.json"
sample="${STATE_ROOT}/vworld-sample-inventory.json"
evidence="${STATE_ROOT}/vworld-evidence.json"
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
  find "${SPOOL_DIR}" -mindepth 1 -maxdepth 1 -name '.provider-*.part' -delete 2>/dev/null || true
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

step_plan() {
  export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH="${hub_plan}"
  "${PUBLISHER_BIN}" plan-building-hub-bulk-collection
  vworld_sweep_lane_env "${CATALOG}" "${vworld_plan}" "${inventory}" "${evidence}" "${SPOOL_DIR}"
  "${PUBLISHER_BIN}" plan-vworld-dataset-collection
  "${PUBLISHER_BIN}" inventory-vworld-dataset-files
  python3 -I - "${hub_plan}" "${vworld_plan}" "${inventory}" <<'PY'
import json, sys
hub, plan, inventory = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:4])
files = sum(len(job.get("files", [])) for job in inventory.get("jobs", []))
print(f"plan hub_jobs={len(hub.get('jobs', []))} vworld_jobs={len(plan.get('jobs', []))} inventory_files={files}")
if not hub.get("jobs") or not plan.get("jobs") or not files:
    sys.exit("a provider listed nothing: a plan or the inventory is empty")
PY
}

# The sample: the smallest files and the smallest one large enough for the multipart path, each a
# file the sweep would take (a RAON selection archive never is). Written as an inventory of its own.
write_sample() {
  python3 -I - "${CONTRACT}" "${inventory}" "${sample}" <<'PY'
import json, sys
contract, inventory_path, sample_path = sys.argv[1:4]
rules = json.load(open(contract, encoding="utf-8"))["sample"]
inventory = json.load(open(inventory_path, encoding="utf-8"))
files = [(job_index, item) for job_index, job in enumerate(inventory["jobs"]) for item in job.get("files", [])
         if item.get("download_kind") != "selection_archive"]
size = lambda pair: int(pair[1].get("size_kib") or 0) * 1024
by_size = sorted(files, key=size)
small = by_size[: rules["smallest_files"]]
large = [pair for pair in by_size
         if rules["multipart_threshold_bytes"] <= size(pair) <= rules["multipart_max_bytes"]
         and pair not in small][: rules["multipart_files"]]
if len(small) < rules["smallest_files"] or len(large) < rules["multipart_files"]:
    sys.exit(f"the inventory has no sample: {len(small)} small files and {len(large)} between "
             f"{rules['multipart_threshold_bytes']} and {rules['multipart_max_bytes']} bytes")
chosen = small + large
jobs = []
for job_index, job in enumerate(inventory["jobs"]):
    picked = [item for index, item in chosen if index == job_index]
    if picked:
        jobs.append({**job, "files": picked})
json.dump({**inventory, "jobs": jobs}, open(sample_path, "w", encoding="utf-8"), ensure_ascii=False)
print("sample " + " ".join(f"{item['download_ds_id']}-{item['file_no']}:{size((i, item))}" for i, item in chosen))
PY
}

step_ingest() {
  vworld_sweep_lane_env "${CATALOG}" "${vworld_plan}" "${sample}" "${evidence}" "${SPOOL_DIR}"
  FOUNDATION_PLATFORM_R2_STAGING_MULTIPART_THRESHOLD_BYTES="$(contract sample multipart_threshold_bytes)" \
    "${PUBLISHER_BIN}" ingest-vworld-dataset-files
  python3 -I - "${sample}" "${evidence}" <<'PY'
import json, sys
sample, evidence = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:3])
wanted = sum(len(job["files"]) for job in sample["jobs"])
landed = evidence.get("succeeded_file_count")
print(f"ingest wanted={wanted} landed={landed} failed={evidence.get('failed_file_count')} status={evidence.get('status')}")
if evidence.get("status") != "ready" or evidence.get("failed_file_count") or landed != wanted:
    sys.exit("the sample did not land whole")
PY
}

step_measure() {
  # Read-only, as the measure runs in production (root ADR-0152): the writer pair goes first.
  unset FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  local sources output
  sources="$(python3 -I - "${sample}" <<'PY'
import json, sys
print(",".join(sorted({job["source_slug"] for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"]})))
PY
)"
  output="$(FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES="${sources}" FOUNDATION_PLATFORM_RELEASE_ID="${RELEASE_ID}" \
    "${PUBLISHER_BIN}" measure-bronze-object-members)"
  printf '%s\n' "${output}"
  python3 -I - "${sample}" "${output}" <<'PY'
import json, sys
wanted = sum(len(job["files"]) for job in json.load(open(sys.argv[1], encoding="utf-8"))["jobs"])
line = [l for l in sys.argv[2].splitlines() if l.startswith("bronze-object-members-json ")]
summary = json.loads(line[-1].split(" ", 1)[1]) if line else {}
print(f"measure wanted={wanted} measured={summary.get('measured')} failed={summary.get('failed')}")
if summary.get("measured") != wanted or summary.get("failed"):
    sys.exit("the landed sample was not measured whole")
PY
}

# One step in its own process, so `timeout` can bound it: `staging-smoke.sh --step <name>`.
if [[ "${1:-}" == --step ]]; then
  "step_${2:?}"
  exit 0
fi

mkdir -p "${STATE_ROOT}" "${SPOOL_DIR}"
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
run_step plan
write_sample >>"${run_log}" 2>&1 || {
  echo "staging-smoke: sample FAILED for release ${RELEASE_ID}; the release is not deployed (root ADR-0177)" >&2
  job_run_log_tail "${run_log}"
  exit 1
}
run_step ingest
run_step measure
grep -E '^(plan|sample|ingest|measure) ' "${run_log}" || true
echo "staging-smoke: passed release=${RELEASE_ID} ${durations[*]}"
