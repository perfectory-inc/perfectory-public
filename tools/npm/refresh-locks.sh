#!/usr/bin/env bash
# Regenerates every tracked pnpm-lock.yaml with the toolchain the trees pin (root ADR-0158).
#
# Node, pnpm and the container digest are read from tools/technology-versions.contract.json
# through security-overrides.mjs; the trees are every directory holding a tracked
# pnpm-lock.yaml. Each lock is resolved inside node:<pin>-bookworm@<digest> by
# `corepack pnpm@<pin> install --lockfile-only`, so the same bytes come out on a laptop and
# on a CI runner, and no install scripts run. The overrides check runs at the end: a refresh
# that leaves a tree below a floor fails here, not in review.
#
# usage: bash tools/npm/refresh-locks.sh [tree ...]   (default: every tracked tree)
set -euo pipefail

for command_name in docker git node; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL refresh-locks: missing command '$command_name'" >&2
    exit 1
  }
done

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
tool="$root/tools/npm/security-overrides.mjs"

contract_image=""
pnpm_version=""
while IFS='=' read -r key value; do
  case "$key" in
    NODE_IMAGE) contract_image="$value" ;;
    PNPM_VERSION) pnpm_version="$value" ;;
  esac
done < <(node "$tool" toolchain --root "$root" | tr -d '\r')
if [ -z "$contract_image" ] || [ -z "$pnpm_version" ]; then
  echo "FAIL refresh-locks: the technology contract did not yield a toolchain" >&2
  exit 1
fi
# The container reference comes from tools/container-images.env, the checked projection of
# the same contract that the container runtime policy can read; it must be the image of the
# Node the trees pin, or a pin bump left the image behind.
# shellcheck source=../container-images.env
. "$root/tools/container-images.env"
if [ "$NODE_VERIFY_IMAGE" != "$contract_image" ]; then
  echo "FAIL refresh-locks: NODE_VERIFY_IMAGE is $NODE_VERIFY_IMAGE but the pinned Node needs $contract_image" >&2
  exit 1
fi

if [ "$#" -gt 0 ]; then
  trees=("$@")
else
  mapfile -t trees < <(node "$tool" trees --root "$root" | tr -d '\r')
fi
if [ "${#trees[@]}" -eq 0 ]; then
  echo "FAIL refresh-locks: no tracked pnpm-lock.yaml" >&2
  exit 1
fi

# Git Bash rewrites anything that looks like a POSIX path in arguments to a Windows program;
# docker needs the Windows form of the bind source and the untouched container path, so the
# rewrite is switched off for the docker call alone (node below still needs it). On Linux the
# container runs as the caller so the regenerated lock keeps its owner; Docker Desktop maps
# bind-mount ownership itself.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    host_path() { cygpath -w "$1"; }
    run_as=root
    ;;
  *)
    host_path() { printf '%s\n' "$1"; }
    run_as="$(id -u):$(id -g)"
    ;;
esac

for tree in "${trees[@]}"; do
  if [ ! -f "$root/$tree/pnpm-lock.yaml" ] || [ ! -f "$root/$tree/package.json" ]; then
    echo "FAIL refresh-locks: $tree has no package.json and pnpm-lock.yaml" >&2
    exit 1
  fi
  echo "refresh-locks: $tree (pnpm $pnpm_version in $NODE_VERIFY_IMAGE)"
  MSYS_NO_PATHCONV=1 docker run --rm --user "$run_as" \
    --volume "$(host_path "$root/$tree"):/work" \
    --workdir /work \
    --env HOME=/tmp \
    --env COREPACK_HOME=/tmp/corepack \
    --env COREPACK_ENABLE_DOWNLOAD_PROMPT=0 \
    --env CI=true \
    "$NODE_VERIFY_IMAGE" \
    corepack "pnpm@$pnpm_version" install --lockfile-only --ignore-scripts --config.confirmModulesPurge=false
done

node "$tool" check --root "$root"
