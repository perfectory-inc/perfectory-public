#!/usr/bin/env bash
# Repository entry point for immutable container images and loopback-only
# development port publication.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec bash scripts/guard/check-container-runtime-policy.sh .
