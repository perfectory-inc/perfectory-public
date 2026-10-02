#!/usr/bin/env bash
# The deploy's lakehouse migration (root ADR-0124): runs lakehouse_schema_migrate.py in apply mode
# from the release, the way `foundation-migrate` applies the database migrations. Started by
# `foundation-release.sh migrate` through its systemd service, which supplies the lakehouse
# catalog settings (map-edit-fold.env). Spark's scratch space is the bulk disk, not the root one.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

STATE_ROOT="${FOUNDATION_LAKEHOUSE_MIGRATE_STATE_ROOT:-/var/lib/foundation-platform/lakehouse-migrate}"
SCRATCH="${FOUNDATION_LAKEHOUSE_MIGRATE_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # the Spark container writes as uid 185

docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
status=0
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${work}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm -v "${SCRATCH}:/scratch" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[8,8]' --driver-memory 16g \
  --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/lakehouse_schema_migrate.py --mode apply \
  --summary-output /workspace/target/lakehouse/summary.json > "${work}/run.log" 2>&1 || status=$?

# One line per table and the totals reach the journal (and the deploy's output); the full Spark
# log stays in the run directory.
grep -a '^lakehouse-migrate ' "${work}/run.log" || true
if [[ "${status}" != 0 ]]; then
  grep -a -E 'Traceback|Error|Exception' "${work}/run.log" | grep -v '^\s*at ' | tail -5 || true
  printf 'lakehouse-migrate FAILED status=%s log=%s\n' "${status}" "${work}/run.log"
fi
exit "${status}"
