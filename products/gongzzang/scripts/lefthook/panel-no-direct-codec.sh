#!/usr/bin/env bash
# SP10 spec § 5.2: apps/web/lib/panel/codec.ts 외부에서 ad-hoc split('>') 금지.
# URL grammar 우회 방지. 위반 시 commit 차단.
set -euo pipefail

# Staged files by default (pre-commit); `--all` scans every tracked file (CI). `--relative`
# matters: lefthook runs this from products/gongzzang/, and without it git prints repository-
# root paths that the `^apps/web/` filter below can never match.
if [ "${1:-}" = "--all" ]; then
  candidates=$(git ls-files)
else
  candidates=$(git diff --cached --name-only --relative --diff-filter=ACM)
fi
staged=$(echo "$candidates" | grep -E '^apps/web/.*\.(ts|tsx)$' | grep -v '^apps/web/lib/panel/codec\.' || true)
if [ -z "$staged" ]; then
  exit 0
fi

# Pattern: split('>') or split(">"). Use grep -F-friendly alternation via -E.
# shellcheck disable=SC2086
bad=$(echo "$staged" | xargs -r grep -lE 'split\(("\>"|'"'"'>'"'"')\)' || true)
if [ -n "$bad" ]; then
  echo "ERROR: ad-hoc split('>') outside lib/panel/codec.ts (spec § 5.2)"
  echo "  offending: $bad"
  exit 1
fi
exit 0
