#!/usr/bin/env bash
# Proves the CI time budget's grouping, statistics and verdict on every pull request;
# the weekly workflow that applies them to real runs gates no pull request (root ADR-0203).
# The per-crate build-time summary verify prints is proved beside it (root ADR-0269).
set -euo pipefail
cd "$(dirname "$0")/../.."

for suite in test_ci_time_budget.py test_cargo_timings.py; do
  python3 -m unittest discover \
    -s scripts/ci \
    -p "$suite" \
    -v
done
