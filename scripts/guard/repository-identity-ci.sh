#!/usr/bin/env bash
# Canonical public CI requires a pinned identity matching GitHub's immutable
# runtime IDs. Private repositories, forks, and local development lint only.
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "usage: $0" >&2
  exit 2
fi

control_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
validator="$control_root/scripts/github/validate-public-repository-identity.sh"
json_helper="$control_root/scripts/github/github-policy-json.py"
repository_identity="$control_root/tools/github/repository-identity.json"

if [ "${GITHUB_REPOSITORY:-}" = "perfectory-inc/perfectory-public" ]; then
  bash "$validator"
  if [ -z "${GITHUB_REPOSITORY_ID:-}" ] \
    || [ -z "${GITHUB_REPOSITORY_OWNER_ID:-}" ]; then
    echo "FAIL repository-identity-ci: canonical CI requires immutable runtime repository and owner IDs" >&2
    exit 1
  fi
  exec python3 "$json_helper" validate-repository-runtime-identity \
    "$repository_identity" \
    "$GITHUB_REPOSITORY_ID" "$GITHUB_REPOSITORY_OWNER_ID"
fi
exec bash "$validator" --allow-unset
