#!/usr/bin/env bash
# Prevents: a shared-verification change slipping past an area CI. `cargo xtask
# verify` (ADR-0004) is the single verification tool every area CI runs, yet its
# source lives at tools/xtask — outside every area's own path filter. If a
# workflow gates on `paths:` but does not also watch tools/xtask, an xtask change
# (which alters how that area is verified) never retriggers it. That exact gap
# once left an intelligence-platform fix untested. So: any workflow that runs
# `cargo xtask verify` AND restricts itself with a `paths:` filter MUST watch
# tools/xtask, making the drift structurally impossible instead of a thing to
# remember for each new area CI.
set -euo pipefail
cd "$(dirname "$0")/../.."
rc=0
for wf in .github/workflows/*.yml; do
  [ -e "$wf" ] || continue
  grep -qE 'cargo[[:space:]]+xtask[[:space:]]+verify' "$wf" || continue # not a verify workflow
  grep -qE '^[[:space:]]*paths:' "$wf" || continue                      # no path filter -> always runs -> covered
  if ! grep -qE 'tools/xtask' "$wf"; then
    echo "FAIL xtask-path-coverage: $wf runs 'cargo xtask verify' behind a paths filter but does not watch tools/xtask/** — an xtask change would not retrigger it (ADR-0004)." >&2
    rc=1
  fi
done
if [ "$rc" -eq 0 ]; then
  echo "OK xtask-path-coverage"
fi
exit "$rc"
