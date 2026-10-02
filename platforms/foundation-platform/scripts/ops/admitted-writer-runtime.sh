#!/usr/bin/env bash
# Sourced by registered production jobs. Admission is the root systemd ExecStartPre;
# this binds their execution inputs to that same physical release (root ADR-0126).
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ "${RELEASE_ROOT}" != "$(readlink -f /opt/foundation-platform/current)" ||
      ! "${RELEASE_ROOT}" =~ ^/opt/foundation-platform/releases/[0-9a-f]{40}$ ]]; then
  printf 'writer must execute the admitted current release\n' >&2
  exit 65
fi
readonly RELEASE_ROOT
readonly artifact_root="/opt/foundation-platform/artifacts/${RELEASE_ROOT##*/}"
readonly PUBLISHER_BIN="${artifact_root}/foundation-outbox-publisher"
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
unset PYTHONPATH PYTHONHOME BASH_ENV ENV LD_PRELOAD LD_LIBRARY_PATH
unset FOUNDATION_PLATFORM_LAKEHOUSE_ENGINE_CONTRACT_PATH COMPOSE_FILE COMPOSE_PATH_SEPARATOR
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_CONFIG
export FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${artifact_root}/jars"
export FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro
# Spark receives only the frozen jars, never --packages or a writable Ivy lookup.
SPARK_RELEASE_JARS="$(python3 -I - "${artifact_root}/build.json" <<'PY'
import json, sys
files = json.load(open(sys.argv[1]))["files"]
print(",".join("/home/spark/.ivy2/" + name.removeprefix("jars/")
               for name in sorted(files) if name.startswith("jars/")))
PY
)"
[[ -x "${PUBLISHER_BIN}" && -n "${SPARK_RELEASE_JARS}" ]] || {
  printf 'trusted release build output is missing\n' >&2
  exit 65
}
readonly SPARK_RELEASE_JARS
