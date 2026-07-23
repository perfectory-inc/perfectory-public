#!/usr/bin/env bash
# Prevents maintained docs and scripts from pointing at a repository that used to be a sibling
# before the monorepo became the source of truth.
set -euo pipefail
cd "$(dirname "$0")/../.."
pattern='(^|[^.[:alnum:]_/-])(\.\./)+[[:alnum:]_][[:alnum:]_.-]*/(AGENTS\.md|Cargo\.toml|package\.json|docs/|crates/|services/|apps/)|[A-Za-z]:[\\/][^[:space:]]*[\\/]Desktop[\\/]'
# Historical plans and operational evidence are outside the public code tree
# (ADR-0007), so there is no archive path to exclude from this live-tree guard.
hits=$(git grep -nE "$pattern" -- \
  ':!scripts/guard' ':!docs/adr/0001*' \
  ':!*/docs/superpowers/**' ':!*/docs/adr/*' ':!*/docs/migration/*' ':!*/docs/research/*' \
  || true)
if [ -n "$hits" ]; then
  echo "FAIL no-stale-sibling-paths: pre-merge sibling paths in live docs/config/code:" >&2
  echo "$hits" >&2
  exit 1
fi
echo "OK no-stale-sibling-paths"
