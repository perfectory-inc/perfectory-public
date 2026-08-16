#!/usr/bin/env bash
# Monorepo-wide internal-link check (docs recurrence gate, Phase D) via lychee.
#
# Runs the official lycheeverse/lychee Docker image against the root lychee.toml
# SSOT, pinned by digest in tools/container-images.env. CI and local verification
# therefore execute the same repository-owned path without a wrapper Action that
# downloads an unverified release binary.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/container-images.env

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  if [ "${CI:-}" = "true" ]; then
    echo "lychee-docs: Docker is required in CI" >&2
    exit 1
  fi
  echo "lychee-docs: Docker unavailable -- skipping local convenience check (CI is authoritative)."
  exit 0
fi

if ! command -v timeout >/dev/null 2>&1; then
  echo "lychee-docs: coreutils \`timeout\` is required to bound the container run" >&2
  exit 1
fi

# The check itself takes about sixteen seconds. It has been observed printing its
# full summary -- 1078 links, 935 OK, 0 errors -- and then never returning: the
# container sat at "Up 56 minutes" with no processes inside it while the client
# below stayed attached to it, and the pre-push hook reported 3492 seconds for
# this one command. Killing the container by hand turned the hook into an exit 1
# and rejected the push, so the hour bought nothing.
#
# (The prose above deliberately does not spell out the two-word client
# invocation: container-runtime-policy reads this file and parses any occurrence
# of it as a real command whose image it must resolve.)
#
# A run that has stopped producing output is not a run in progress. Bound it, and
# name the container so a run the runtime refuses to release can still be removed
# rather than left behind holding the repository bind mount.
timeout_seconds="${LYCHEE_DOCS_TIMEOUT_SECONDS:-300}"
container="lychee-docs-$$"

remove_container() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap remove_container EXIT

repo="$(pwd -W 2>/dev/null || pwd)"
status=0
MSYS_NO_PATHCONV=1 timeout --signal=TERM --kill-after=30s "$timeout_seconds" \
  docker run --rm \
  --name "$container" \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  -v "$repo":/input:ro \
  -w /input \
  "$LYCHEE_IMAGE" \
  --config lychee.toml --offline --no-progress './**/*.md' || status=$?

# 124 is `timeout` giving up after TERM; 137 is the container being killed after
# it ignored TERM. Either way the runtime, not the link check, is what failed --
# so say that instead of reporting broken links.
if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
  remove_container
  echo "lychee-docs: TIMEOUT -- the container did not return within ${timeout_seconds}s and was removed." >&2
  echo "lychee-docs: the link check completes in ~16s, so this is the container runtime failing to release a finished run, not a slow scan." >&2
  echo "lychee-docs: restart the container runtime, or raise LYCHEE_DOCS_TIMEOUT_SECONDS if this repository has genuinely grown that much." >&2
  exit 1
fi

exit "$status"
