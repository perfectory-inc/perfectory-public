#!/usr/bin/env bash
# Canonical public CI enforces the strict legal self-attestation. Private
# repositories, forks, and local development perform structural lint only.
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "usage: $0" >&2
  exit 2
fi

control_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
validator="$control_root/scripts/github/validate-legal-publication.sh"

if [ "${GITHUB_REPOSITORY:-}" = "perfectory-inc/perfectory-public" ]; then
  exec bash "$validator"
fi
exec bash "$validator" --allow-unconfirmed
