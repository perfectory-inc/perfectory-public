#!/usr/bin/env bash
# SP10 spec § 9 #5: apps/web/lib/panel/** 는 apps/web/components/panels/** 에 의존 금지.
# 프레임워크 → kind 단방향. 위반 시 commit 차단.
set -euo pipefail

# Staged files by default (pre-commit); `--all` scans every tracked file (CI). `--relative`
# matters: lefthook runs this from products/gongzzang/, and without it git prints repository-
# root paths that the `^apps/web/` filter below can never match.
if [ "${1:-}" = "--all" ]; then
  candidates=$(git ls-files)
else
  candidates=$(git diff --cached --name-only --relative --diff-filter=ACM)
fi
staged=$(echo "$candidates" | grep -E '^apps/web/lib/panel/.*\.(ts|tsx)$' || true)
if [ -z "$staged" ]; then
  exit 0
fi

# Match either `from "@/components/panels/"` (named/default imports) or
# `import "@/components/panels/"` (side-effect imports).
# shellcheck disable=SC2086
bad=$(echo "$staged" | xargs -r grep -lE "(from|import)[[:space:]]+['\"]@/components/panels/" || true)
if [ -n "$bad" ]; then
  echo "ERROR: lib/panel/** must not import components/panels/** (spec § 9 #5)"
  echo "  offending: $bad"
  exit 1
fi
exit 0
