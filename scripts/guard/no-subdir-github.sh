#!/usr/bin/env bash
# Prevents: silently-inert CI. GitHub only reads root .github/workflows; on 2026-07-19 all
# four areas' pipelines were dead because they lived in subdirectories.
set -euo pipefail
cd "$(dirname "$0")/../.."
found=$(git ls-files | grep -E '^(products|platforms)/.*/\.github/workflows/' || true)
if [ -n "$found" ]; then
  echo "FAIL no-subdir-github: workflow files outside root .github/ are never executed:" >&2
  echo "$found" >&2
  exit 1
fi
echo "OK no-subdir-github"
