#!/usr/bin/env bash
# Prevents: a pull-request workflow that runs whatever changed, or a scope that
# silently skips a gate (root ADR-0201). The unit suite plants each violation —
# an unscoped new workflow, a leftover `paths:` filter, a job the scope does not
# gate, a stale input glob, a missing xtask input — and its last test checks the
# repository's own workflows against tools/ci/change-scope.json.
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 scripts/ci/test_change_scope.py
