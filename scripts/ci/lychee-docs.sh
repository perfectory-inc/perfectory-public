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

repo="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 docker run --rm \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  -v "$repo":/input:ro \
  -w /input \
  "$LYCHEE_IMAGE" \
  --config lychee.toml --offline --no-progress './**/*.md'
