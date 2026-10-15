#!/usr/bin/env bash
# The scheduled lakehouse maintenance (root ADR-0238): compaction and snapshot expiry on every table
# of the lakehouse contract artifact, under the retention of root ADR-0237.
#
#   lakehouse-maintenance.sh [--dry-run]
#   lakehouse-maintenance.sh cleanup        (the unit's ExecStopPost)
#
# One Spark run (infra/lakehouse/spark/jobs/lakehouse_maintenance.py --all-tables) takes the tables
# the catalog holds, the least recently maintained first, and starts none after the maintenance
# contract's `scheduled.deadline_minutes`. Which table was maintained when is kept in this unit's
# state directory, so a table one run does not reach is first at the next. The steps it skips and
# the Spark size are the contract's `scheduled`; per-table rules are its `tables`. Expiry never
# removes a snapshot a loader or the Gold rebuild reads back (lakehouse_maintenance.protected_snapshots).
#
# The run ends with `foundation-job-outcome unchanged`: compaction rewrites files, not rows, so no
# downstream job has new data to read (root ADR-0171).
#
# Spark runs in the Compose project foundation-lakehouse-maintenance-<INVOCATION_ID>. A timeout kills
# this script and the compose client but not the daemon-owned container, which would keep its 20g
# while Airflow frees the pool. The unit's ExecStopPost (`cleanup`) has the release's publisher
# remove that project's one-off containers and networks (root ADR-0128), and nothing else.
set -euo pipefail

if [[ "${1:-}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo "lakehouse-maintenance: cleanup takes no arguments" >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo "lakehouse-maintenance: cleanup needs the systemd INVOCATION_ID" >&2; exit 64; }
  # Any installed release: `current` may have moved since this invocation started.
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-lakehouse-maintenance
fi
DRY_RUN=()
while (($#)); do
  case "$1" in
    --dry-run) DRY_RUN=(--dry-run); shift ;;
    *) echo "lakehouse-maintenance: expected --dry-run or cleanup, got '$1'" >&2; exit 64 ;;
  esac
done
# A run outside systemd (a supervised manual run) names its own project; ExecStopPost is not there
# to clean it, so the operator is told how.
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "lakehouse-maintenance: no systemd invocation; this run's Compose project is foundation-lakehouse-maintenance-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo "lakehouse-maintenance: INVOCATION_ID is not a systemd invocation id" >&2; exit 64; }
export INVOCATION_ID

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
STATE_ROOT="${FOUNDATION_LAKEHOUSE_MAINTENANCE_STATE_ROOT:-/var/lib/foundation-lakehouse-maintenance}"
SCRATCH="${FOUNDATION_LAKEHOUSE_MAINTENANCE_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"
log() { printf 'lakehouse-maintenance: %s\n' "$*"; }

# One run at a time, whoever started it. Taken without waiting.
lock="${STATE_ROOT}/maintenance.lock"
mkdir -p "${STATE_ROOT}"
[[ -e "${lock}" ]] || : >>"${lock}"
exec 8<"${lock}"
if ! flock -n 8; then
  log "refused: another maintenance run holds ${lock}"
  exit 75
fi

read -r master driver_memory < <(python3 -I - "${RELEASE_ROOT}/infra/lakehouse/spark/jobs" <<'PY'
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
import lakehouse_maintenance
spark = lakehouse_maintenance.load_maintenance_contract()["scheduled"]["spark"]
print(spark["master"], spark["driver_memory"])
PY
)
[[ -n "${driver_memory:-}" ]] || { log "refused: the maintenance contract names no scheduled Spark size"; exit 65; }

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
# The Spark container runs as uid 185: its init step checks it can write the state root it mounts,
# and the rotation file lives there.
chmod 0777 "${STATE_ROOT}" "${work}"
[[ ! -e "${STATE_ROOT}/rotation.json" ]] || chmod 0666 "${STATE_ROOT}/rotation.json"

docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
status=0
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p "foundation-lakehouse-maintenance-${INVOCATION_ID}" --profile lakehouse-batch run --rm -v "${SCRATCH}:/scratch" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master "${master}" --driver-memory "${driver_memory}" \
  --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/lakehouse_maintenance.py --all-tables \
  --state-file /workspace/target/lakehouse/rotation.json ${DRY_RUN[@]+"${DRY_RUN[@]}"} \
  >"${work}/lakehouse_maintenance.log" 2>&1 || status=$?
grep -a -E '^maintenance-(done|floor|failed|deferred|summary)|^foundation-job-outcome' "${work}/lakehouse_maintenance.log" || true
if ((status)); then
  grep -a -E 'Error|Exception' "${work}/lakehouse_maintenance.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  log "FAILED (exit ${status}; log ${work}/lakehouse_maintenance.log)"
fi
exit "${status}"
