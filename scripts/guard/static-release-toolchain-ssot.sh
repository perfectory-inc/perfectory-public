#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
exec python3 "$root/scripts/guard/static-release-toolchain-ssot.py" --root "$root"
