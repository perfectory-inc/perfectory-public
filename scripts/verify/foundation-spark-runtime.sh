#!/usr/bin/env bash
# Runs Foundation's Spark tests (platforms/foundation-platform/infra/lakehouse/spark/tests) inside the
# pinned Spark image with the pinned Iceberg runtime, so the tests that skip without them run.
#
# The default CI runner has no PySpark, so those tests skip there. This is where they run:
#   - the image is the compose `spark` service's (compose.lakehouse.yml), the one production runs;
#   - the Iceberg artifacts and version are lakehouse-engine.contract.json's, the ones production
#     submits with. Neither is restated here.
# The tests here are the files that skip without that runtime: their skip reason says what they
# require ("requires pyspark", "requires the pinned Spark/Iceberg runtime"), so a new one joins by
# saying so. Each file runs in its own interpreter: the pure-logic tests beside them install a
# stand-in `pyspark` module, which would shadow the real one in a shared process. A test that still
# skips for want of a runtime fails the run: a lane whose tests quietly skip proves nothing.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
AREA="$ROOT/platforms/foundation-platform"
TESTS="infra/lakehouse/spark/tests"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "foundation-spark-runtime: Docker Engine is required" >&2
  exit 1
fi

image="$(docker compose --project-directory "$AREA" -f "$AREA/compose.lakehouse.yml" \
  --profile lakehouse-batch config --format json --no-interpolate \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["spark"]["image"])')"
packages="$(python3 - "$AREA/infra/lakehouse/contracts/lakehouse-engine.contract.json" <<'PY'
import json, sys
iceberg = json.load(open(sys.argv[1], encoding="utf-8"))["iceberg"]
print(",".join(f"{artifact}:{iceberg['version']}" for artifact in iceberg["artifacts"]))
PY
)"
[[ "$image" == *@sha256:* ]] || { echo "foundation-spark-runtime: compose spark image is not digest-pinned: $image" >&2; exit 1; }
echo "foundation-spark-runtime: $image with $packages"

log="$(mktemp)"
trap 'rm -f "$log"' EXIT
# Read-only source; Ivy, Spark's scratch and the tests' temporary tables live in the container's /tmp.
status=0
docker run --rm --network bridge -v "$ROOT:/repo:ro" -w "/repo/platforms/foundation-platform" \
  -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 -e RUN_SPARK_TESTS=1 -e RUN_ICEBERG_TESTS=1 \
  -e "PYSPARK_SUBMIT_ARGS=--packages $packages --conf spark.jars.ivy=/tmp/ivy --conf spark.ui.enabled=false pyspark-shell" \
  --entrypoint /bin/bash "$image" -c '
    set -euo pipefail
    py4j=(/opt/spark/python/lib/py4j-*-src.zip)
    export PYTHONPATH="/opt/spark/python:${py4j[0]}"
    cd "$0"
    mapfile -t files < <(grep -l -E "skip(Unless|If)\(.*\"requires (pyspark|the pinned)" test_*.py)
    ((${#files[@]})) || { echo "no runtime-gated test file found" >&2; exit 1; }
    failed=0
    for file in "${files[@]}"; do
      echo "== ${file}"
      python3 -m unittest -v "${file%.py}" || failed=1
    done
    exit "${failed}"' "$TESTS" >"$log" 2>&1 || status=$?
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
