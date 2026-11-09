#!/usr/bin/env bash
# Runs the Dagster observer's tests (platforms/foundation-platform/orchestration/tests/test_dagster_*.py)
# inside the image production runs it from (orchestration/dagster/Dockerfile, root ADR-0333 stage 0), then
# the proxied run's Airflow side inside the release's Airflow image (orchestration/airflow/Dockerfile).
#
# The default CI runner has no Dagster, so those tests skip there ("requires dagster ..."). Here the
# image is built from the locked, hashed requirements and the repository is mounted read-only, the way
# the host mounts the release; the parity test replays a recorded production day of Airflow runs
# through dagster-airlift's own sensor. A test that still skips for want of Dagster fails the run.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "foundation-dagster-observe: Docker Engine is required" >&2
  exit 1
fi

work="$(mktemp -d)"
log="$work/tests.log"
trap 'rm -rf "$work" 2>/dev/null || true' EXIT

# The tag compose names it by (compose.orchestration.yml), built from the same Dockerfile and context.
SECONDS=0
docker build --quiet --tag foundation-platform-dagster:local --file platforms/foundation-platform/orchestration/dagster/Dockerfile ./platforms/foundation-platform
echo "foundation-dagster-observe: image ready in ${SECONDS}s"
status=0
docker run --rm --network none -v "$ROOT:/repo:ro" -w /repo/platforms/foundation-platform \
  -e PYTHONDONTWRITEBYTECODE=1 -e HOME=/tmp foundation-platform-dagster:local \
  python -m unittest discover -v -s orchestration/tests -p 'test_dagster_*.py' >"$log" 2>&1 || status=$?
grep -v -E ' - dagster - (INFO|DEBUG) - ' "$log" | grep -v -E '^\s*$' || true
if ((status)); then
  echo "foundation-dagster-observe: FAILED (exit $status)" >&2
  exit "$status"
fi
if grep -E "skipped '(requires|.*dagster)" "$log"; then
  echo "foundation-dagster-observe: FAILED: the tests above skipped for want of the runtime this lane provides" >&2
  exit 1
fi

# The release's Airflow image (orchestration/airflow/Dockerfile, root ADR-0431 §5): a proxied job's `run`
# task against apache/airflow and the dagster-airlift wheel it installs, and the DAG file against Airflow
# itself (test_job_dags.py WithAirflow), which the default runner, having no Airflow, skips.
SECONDS=0
docker build --quiet --tag foundation-platform-airflow:local --file platforms/foundation-platform/orchestration/airflow/Dockerfile ./platforms/foundation-platform
echo "foundation-dagster-observe: Airflow image ready in ${SECONDS}s"
airflow_log="$work/airflow-tests.log"
status=0
docker run --rm --network none -v "$ROOT:/repo:ro" -w /repo/platforms/foundation-platform/orchestration/tests \
  -e PYTHONDONTWRITEBYTECODE=1 --entrypoint python foundation-platform-airflow:local \
  -m unittest -v test_airflow_proxied_run test_job_dags >"$airflow_log" 2>&1 || status=$?
grep -v -E '^\s*$' "$airflow_log" || true
if ((status)); then
  echo "foundation-dagster-observe: Airflow FAILED (exit $status)" >&2
  exit "$status"
fi
if grep -E "skipped '(requires|Airflow is not installed)" "$airflow_log"; then
  echo "foundation-dagster-observe: FAILED: the Airflow tests above skipped in the Airflow image" >&2
  exit 1
fi
echo "foundation-dagster-observe: OK"
