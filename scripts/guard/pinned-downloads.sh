#!/usr/bin/env bash
# Prevents: a pinned release download that bypasses the cache, the retry and the checksum, or a CI
# job that downloads one without restoring the cache first (root ADR-0197).
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 scripts/guard/check-pinned-downloads.py "$@"
