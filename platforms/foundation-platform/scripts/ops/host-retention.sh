#!/usr/bin/env bash
# Bounds what fills the root disk between deploys (root ADR-0198).
#
# Docker lives on the root disk. On 2026-10-12 it held 52 GB of BuildKit cache, all of it
# reclaimable, and 43 GB of images, which together put the disk at 86%; nothing ever pruned
# either. Every release build adds cache, so this keeps the newest 20 GB of it (enough for the
# next release build to reuse its layers) and removes dangling images, the untagged layers each
# rebuilt tag leaves behind.
#
# It never runs `docker image prune --all`, `system prune` or `volume prune`: a tagged image with
# no container is how the bake images wait between runs, and volumes hold state.
set -euo pipefail

BUILD_CACHE_KEEP=20GB

root_free() { df --output=avail -B1 / | tail -n 1 | tr -d ' '; }

before="$(root_free)"
docker builder prune --force --max-used-space "${BUILD_CACHE_KEEP}"
docker image prune --force
after="$(root_free)"
printf 'host-retention-ok build_cache_keep=%s root_free_before_bytes=%s root_free_after_bytes=%s freed_gib=%s\n' \
  "${BUILD_CACHE_KEEP}" "${before}" "${after}" "$(( (after - before) / 1073741824 ))"
