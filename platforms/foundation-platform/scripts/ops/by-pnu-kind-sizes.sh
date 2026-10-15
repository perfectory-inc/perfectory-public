#!/usr/bin/env bash
# Measure what each data kind of a by-PNU panel weighs as its own lane (root ADR-0193 §6-1):
#
#   by-pnu-kind-sizes.sh <parcel|building>
#
# Runs infra/lakehouse/spark/jobs/by_pnu_kind_sizes.py on the panel Gold's current snapshot in the
# measured small Spark (compose `spark-small`, the by-PNU change set's size, root ADR-0138) and
# keeps its summary under the bake root: kind-sizes/<unit>-<UTC time>.json, with the Spark log
# beside it. Read-only: it reads every Gold data file once and writes nothing to the lakehouse or R2.
# The operator starts it (by-pnu-pack-operator.sh <unit> kind-sizes), never beside a Gold rebuild
# or a bake.
set -euo pipefail

UNIT="${1:-}"
[[ "$#" == 1 && ( "${UNIT}" == parcel || "${UNIT}" == building ) ]] \
  || { echo "usage: by-pnu-kind-sizes.sh <parcel|building>" >&2; exit 64; }
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
STATE_ROOT="${FOUNDATION_BY_PNU_BAKE_STATE_ROOT:-/data/foundation-platform/by-pnu-bake}/kind-sizes"
run_id="${UNIT}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${STATE_ROOT}/${run_id}"
# The Spark container runs as uid 185: its init step checks it can write the state root it mounts.
chmod 0777 "${STATE_ROOT}" "${STATE_ROOT}/${run_id}"
docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
rc=0
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark-small spark-submit --master 'local[4]' --driver-memory 1500m --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/by_pnu_kind_sizes.py --unit "${UNIT}" \
  --summary-output "/workspace/target/lakehouse/${run_id}/kind-sizes.json" \
  >"${STATE_ROOT}/${run_id}/spark.log" 2>&1 || rc=$?
if ((rc)) || [[ ! -s "${STATE_ROOT}/${run_id}/kind-sizes.json" ]]; then
  grep -a -E 'Error|Exception' "${STATE_ROOT}/${run_id}/spark.log" | grep -v '^\s*at ' | tail -5 >&2 || true
  echo "by-pnu-kind-sizes ${UNIT}: FAILED (exit ${rc}); log ${STATE_ROOT}/${run_id}/spark.log" >&2
  exit 1
fi
echo "by-pnu-kind-sizes ${UNIT}: ${STATE_ROOT}/${run_id}/kind-sizes.json"
