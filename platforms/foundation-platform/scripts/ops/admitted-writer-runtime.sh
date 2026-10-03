#!/usr/bin/env bash
# Sourced by every registered job that runs the publisher (root ADR-0134 §3). Admission itself is
# the root `ExecStartPre` drop-in; this binds the job's inputs to that same physical release:
#
#   <base>/releases/<sha>/scripts/ops/<this file>   the admitted, read-only source
#   <base>/artifacts/<sha>/                         its trusted build output (build.json)
#
# Nothing here comes from the environment. The base is where this file physically is, so a
# caller cannot point a job at another binary, another source tree or a mutable Ivy cache; the
# per-job binary/release overrides and the shared state-directory binary are gone.
#
#   source admitted-writer-runtime.sh --current     the release must be `current`
#   source admitted-writer-runtime.sh --installed   any installed release (FLOOR's ExecStopPost
#                                                   cleans up after `current` has moved on)
#
# The mode is required: `source` without arguments hands this file the caller's own arguments.
#
# Sets RELEASE_ROOT, RELEASE_ID, ARTIFACT_ROOT, PUBLISHER_BIN, RELEASE_JARS_DIR and
# SPARK_RELEASE_JARS (the frozen jars, as paths inside the Spark container's Ivy mount), and exports
# FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_TIPPECANOE_IMAGE (the release build's tippecanoe image ID).
admitted_writer_refuse() {
  printf 'admitted-writer-runtime: refused: %s\n' "$1" >&2
  exit 65
}
case "$#:${1:-}" in
  1:--current) admitted_writer_require_current=yes ;;
  1:--installed) admitted_writer_require_current=no ;;
  *) admitted_writer_refuse 'source with exactly one of --current or --installed' ;;
esac
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)" ||
  admitted_writer_refuse 'cannot resolve the physical release'
[[ "${RELEASE_ROOT}" =~ ^(/.+)/releases/([0-9a-f]{40})$ ]] ||
  admitted_writer_refuse "not inside an installed release: ${RELEASE_ROOT}"
admitted_writer_base="${BASH_REMATCH[1]}"
RELEASE_ID="${BASH_REMATCH[2]}"
if [[ "${admitted_writer_require_current}" == yes &&
      "$(readlink -e "${admitted_writer_base}/current" 2>/dev/null)" != "${RELEASE_ROOT}" ]]; then
  admitted_writer_refuse 'writer must execute the admitted current release'
fi
ARTIFACT_ROOT="${admitted_writer_base}/artifacts/${RELEASE_ID}"
PUBLISHER_BIN="${ARTIFACT_ROOT}/foundation-outbox-publisher"
RELEASE_JARS_DIR="${ARTIFACT_ROOT}/jars"
unset PYTHONPATH PYTHONHOME BASH_ENV ENV LD_PRELOAD LD_LIBRARY_PATH
unset FOUNDATION_PLATFORM_LAKEHOUSE_ENGINE_CONTRACT_PATH COMPOSE_FILE COMPOSE_PATH_SEPARATOR
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_CONFIG
# The trusted build wrote build.json; admission re-verifies every byte as root before start.
# This repeats the one check a job can make for itself: the binary it is about to run is the
# one that manifest names for this release, and the frozen jar list comes from the same file.
admitted_writer_manifest="$(python3 -I - "${ARTIFACT_ROOT}" "${RELEASE_ID}" <<'PY'
import hashlib, json, os, pathlib, stat, sys
root, release = pathlib.Path(sys.argv[1]), sys.argv[2]
def refuse(reason):
    sys.exit("admitted-writer-runtime: refused: " + reason)
try:
    manifest = json.loads((root / "build.json").read_text(encoding="utf-8"))
except (OSError, ValueError):
    refuse("trusted release build output is missing: " + str(root))
if manifest.get("source") != release:
    refuse("build output belongs to another release")
files = manifest.get("files")
if not isinstance(files, dict):
    refuse("build manifest lists no files")
publisher = root / "foundation-outbox-publisher"
info = os.lstat(publisher) if os.path.lexists(publisher) else None
if info is None or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o222:
    refuse("publisher must be a read-only regular file in the release artifacts")
if hashlib.sha256(publisher.read_bytes()).hexdigest() != files.get("foundation-outbox-publisher"):
    refuse("publisher sha256 differs from the release build manifest")
# FLOOR's native image (its configuration names it) must be the image this build produced.
image = os.environ.get("FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE")
if image is not None and image != manifest.get("publisher_image"):
    refuse("FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE is not this release's publisher_image")
jars = sorted(name for name in files if name.startswith("jars/"))
if not jars:
    refuse("build manifest lists no frozen jars")
tippecanoe = manifest.get("tippecanoe_image")
if not isinstance(tippecanoe, str) or not tippecanoe.startswith("sha256:"):
    refuse("build manifest records no tippecanoe image")
print(",".join("/home/spark/.ivy2/" + name.removeprefix("jars/") for name in jars))
print(tippecanoe)
PY
)" || exit 65
SPARK_RELEASE_JARS="${admitted_writer_manifest%%$'\n'*}"
# The tile bake runs the tippecanoe image this release built, not a tag built by hand.
export FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_TIPPECANOE_IMAGE="${admitted_writer_manifest##*$'\n'}"
readonly RELEASE_ROOT RELEASE_ID ARTIFACT_ROOT PUBLISHER_BIN RELEASE_JARS_DIR SPARK_RELEASE_JARS
unset admitted_writer_base admitted_writer_require_current admitted_writer_manifest
