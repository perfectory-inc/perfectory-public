#!/usr/bin/env bash
# Proves batch-containers-are-run-scoped refuses what it exists to refuse: the 2026-10-29 tree (a
# batch service with a fixed container_name, a job that removes that name and starts compose
# itself), a container started under a fixed --name, and a helper that drops --no-deps; and
# accepts a standing named service, a run-built name and the helper.
set -euo pipefail

guard="$(cd "$(dirname "$0")" && pwd)/batch-containers-are-run-scoped.sh"
name="batch-containers-are-run-scoped-self-test"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
ops="$fixture/platforms/foundation-platform/scripts/ops"

reset() {
  rm -rf "$fixture/platforms" "$fixture/tools"
  mkdir -p "$ops" "$fixture/tools"
  printf '{"projects": [{"files": ["platforms/foundation-platform/compose.lakehouse.yml"]}]}\n' \
    >"$fixture/tools/host-memory-budget.contract.json"
  cat >"$fixture/platforms/foundation-platform/compose.lakehouse.yml" <<'YAML'
services:
  trino:
    image: fixture/trino:1
    container_name: fixture-trino
    restart: unless-stopped
  lakehouse-target-init:
    image: fixture/busybox:1
    # container_name: in a comment is not one
  spark:
    image: fixture/spark:1
    restart: "no"
YAML
  cat >"$ops/admitted-writer-runtime.sh" <<'SH'
lakehouse_spark_run() {
  local compose=(docker compose -f "${RELEASE_ROOT}/compose.lakehouse.yml" -p "$1" run --rm --no-deps)
  shift
  "${compose[@]}" lakehouse-target-init && "${compose[@]}" "$@"
}
SH
  cat >"$ops/job.sh" <<'SH'
# docker rm foundation-platform-old is only a comment
docker run --rm --name "fixture-${INVOCATION_ID}" fixture/tool:local
lakehouse_spark_run foundation-platform-compute spark spark-submit job.py
SH
}

expect() {
  local want=$1 label=$2 got
  if bash "$guard" "$fixture" >"$fixture/out" 2>&1; then got=pass; else got=fail; fi
  if [ "$got" != "$want" ]; then
    cat "$fixture/out" >&2
    echo "FAIL $name: $label: expected $want, got $got" >&2
    exit 1
  fi
}

reset
expect pass "a standing named service, a run-built name and the helper"

reset
sed -i 's|^  lakehouse-target-init:$|&\n    container_name: fixture-lakehouse-target-init|' \
  "$fixture/platforms/foundation-platform/compose.lakehouse.yml"
expect fail "a batch service with a fixed container_name (2026-10-29)"

reset
sed -i 's|    restart: unless-stopped||' "$fixture/platforms/foundation-platform/compose.lakehouse.yml"
expect fail "a named service that does not stay up"

reset
printf 'docker rm fixture-lakehouse-target-init >/dev/null 2>&1 || true\n' >>"$ops/job.sh"
expect fail "a job that removes a container by a fixed name"

reset
printf 'docker compose -f "${RELEASE_ROOT}/compose.lakehouse.yml" run --rm spark\n' >>"$ops/job.sh"
expect fail "a job that starts compose.lakehouse.yml itself"

reset
cat >>"$ops/job.sh" <<'SH'
docker run -d --name fixture-relay fixture/relay:local
SH
expect fail "a job that starts a container under a fixed name"

reset
sed -i 's| --no-deps||' "$ops/admitted-writer-runtime.sh"
expect fail "a helper that lets run start the check as a dependency"

echo "OK $name"
