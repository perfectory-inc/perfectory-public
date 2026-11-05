#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/batch-containers.yml (`opengrep scan --test`). Not run.

# ok: lakehouse-compose-starts-through-lakehouse-spark-run
# The bake's write check (compose.lakehouse.yml lakehouse-target-init) runs through the helper.
# ruleid: lakehouse-compose-starts-through-lakehouse-spark-run
docker compose -f "${RELEASE_ROOT}/compose.lakehouse.yml" run --rm lakehouse-batch spark-submit job.py
# ruleid: lakehouse-compose-starts-through-lakehouse-spark-run
local compose=(docker compose -f "${RELEASE_ROOT}/compose.lakehouse.yml" up lakehouse-target-init)

# ruleid: lakehouse-spark-run-creates-no-dependency-container
lakehouse_spark_run() {
  # ruleid: lakehouse-compose-starts-through-lakehouse-spark-run
  local compose=(docker compose -f "${RELEASE_ROOT}/compose.lakehouse.yml" -p "$1" run --rm)
  shift
  "${compose[@]}" lakehouse-target-init && "${compose[@]}" "$@"
}

# ruleid: host-jobs-remove-no-container-by-a-fixed-name
docker rm -f foundation-platform-lakehouse-target-init || true
# ruleid: host-jobs-remove-no-container-by-a-fixed-name
docker container rm foundation-platform-lakehouse-target-init
# ok: host-jobs-remove-no-container-by-a-fixed-name
docker rm -f "${container_id}"

# ruleid: host-jobs-start-no-container-under-a-fixed-name
docker run --rm --name parcel-forest-ledger image:local
# ruleid: host-jobs-start-no-container-under-a-fixed-name
docker create --name=parcel-forest-ledger image:local
# ok: host-jobs-start-no-container-under-a-fixed-name
docker run --rm --name "parcel-forest-ledger-${RUN_ID}" --memory 2g image:local
# ok: host-jobs-start-no-container-under-a-fixed-name
wrangler versions deploy "$1@100%" --name "${WORKER}" --yes
