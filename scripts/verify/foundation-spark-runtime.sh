#!/usr/bin/env bash
# Runs Foundation's Spark tests (platforms/foundation-platform/infra/lakehouse/spark/tests) inside the
# pinned Spark image with the pinned Iceberg runtime, so the tests that skip without them run.
#
# The default CI runner has no PySpark, so those tests skip there. This is where they run:
#   - the container is the compose `spark` service (compose.lakehouse.yml), the image production runs;
#   - the Iceberg artifacts and version are lakehouse-engine.contract.json's, the ones production
#     submits with. Neither is restated here.
# The tests here are the files that skip without that runtime: their skip reason says what they
# require ("requires pyspark", "requires the pinned Spark/Iceberg runtime"), so a new one joins by
# saying so. foundation-spark-runtime-files.sh runs them, each file in its own interpreter, several
# at once. A test that still skips for want of a runtime fails the run: a lane whose tests quietly
# skip proves nothing.
#
# Speed (measured in this script's log, "durations, slowest first"): the Iceberg artifacts are
# resolved once, before the files start, and the files then load the resolved jars, so concurrent
# JVMs never share an Ivy resolution. Spark settings stay each session's own: a shared default
# (fewer shuffle partitions, say) would change what a test exercises. Each test JVM compiles with C1
# only, collects with the serial collector and sizes its pools for two CPUs: the files are short
# and run side by side, so C2 compiler and parallel GC threads only take CPU from the other file.
# Measured 2026-10-13 on ubuntu-24.04 (4 CPUs), one commit, four configurations side by side:
# 2 files at a time 534s; with these JVM options 357s at 2, 418s at 3, 420s at 4 at a time
# (those four also defaulted shuffle partitions to 2, dropped since; serial on main: 488s-774s).
# None of the options changes what a query computes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
AREA="$ROOT/platforms/foundation-platform"
TESTS="infra/lakehouse/spark/tests"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "foundation-spark-runtime: Docker Engine is required" >&2
  exit 1
fi

packages="$(python3 - "$AREA/infra/lakehouse/contracts/lakehouse-engine.contract.json" <<'PY'
import json, sys
iceberg = json.load(open(sys.argv[1], encoding="utf-8"))["iceberg"]
print(",".join(f"{artifact}:{iceberg['version']}" for artifact in iceberg["artifacts"]))
PY
)"
echo "foundation-spark-runtime: compose service spark with $packages"

work="$(mktemp -d)"
log="$work/tests.log"
project="foundation-spark-runtime-$$"
compose() {
  # The service's state, Ivy and /tmp mounts point into this run's temporary directory, not the tree.
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="$work/state" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="$work/ivy" \
    FOUNDATION_PLATFORM_SPARK_TMP="$work/tmp" docker compose -p "$project" --project-directory "$AREA" -f "$AREA/compose.lakehouse.yml" \
    --profile lakehouse-batch "$@"
}
cleanup() {
  compose down --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT
mkdir -p "$work/state" "$work/ivy" "$work/tmp"
chmod 0777 "$work/state" "$work/ivy"
chmod 1777 "$work/tmp"
# Read-only source; Ivy, Spark's scratch and the tests' temporary tables live in the container's /tmp.
SECONDS=0
compose pull --quiet spark
echo "foundation-spark-runtime: image ready in ${SECONDS}s"
status=0
compose run --rm --no-deps -v "$ROOT:/repo:ro" -w "/repo/platforms/foundation-platform" \
  -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 -e RUN_SPARK_TESTS=1 -e RUN_ICEBERG_TESTS=1 \
  -e SPARK_RUNTIME_JOBS -e "SPARK_RUNTIME_PACKAGES=$packages" \
  --entrypoint /bin/bash spark -c '
    set -euo pipefail
    py4j=(/opt/spark/python/lib/py4j-*-src.zip)
    export PYTHONPATH="/opt/spark/python:${py4j[0]}"
    defaults="--conf spark.ui.enabled=false"
    defaults+=" --driver-java-options \"-XX:TieredStopAtLevel=1 -XX:+UseSerialGC -XX:ActiveProcessorCount=2\""
    # Resolve the Iceberg artifacts once; the files then load the jars instead of each resolving.
    PYSPARK_SUBMIT_ARGS="--packages $SPARK_RUNTIME_PACKAGES --conf spark.jars.ivy=/tmp/ivy $defaults pyspark-shell" \
      python3 -c "from pyspark.sql import SparkSession; SparkSession.builder.master(\"local[1]\").getOrCreate().stop()"
    jars="$(printf "%s," /tmp/ivy/jars/*.jar)"
    export PYSPARK_SUBMIT_ARGS="--jars ${jars%,} $defaults pyspark-shell"
    bash /repo/scripts/verify/foundation-spark-runtime-files.sh "$0"' "$TESTS" >"$log" 2>&1 || status=$?
cat "$log"
if ((status)); then
  echo "foundation-spark-runtime: FAILED (exit $status)" >&2
  exit "$status"
fi
if grep -E "skipped '(requires|.*runtime)" "$log"; then
  echo "foundation-spark-runtime: FAILED: the tests above skipped for want of the runtime this lane provides" >&2
  exit 1
fi
echo "foundation-spark-runtime: OK"
