#!/usr/bin/env bash
# Proves the CI time budget's grouping, statistics and verdict on every pull request;
# the weekly workflow that applies them to real runs gates no pull request (root ADR-0203).
set -euo pipefail
cd "$(dirname "$0")/../.."

python3 -m unittest discover \
  -s scripts/ci \
  -p 'test_ci_time_budget.py' \
  -v
