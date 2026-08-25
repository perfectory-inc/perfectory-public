#!/usr/bin/env bash
# Proves the heavy Foundation job selector and its workflow wiring fail closed.
set -euo pipefail
cd "$(dirname "$0")/../.."

python3 -m unittest discover \
  -s scripts/ci \
  -p 'test_foundation_ci_scope.py' \
  -v
