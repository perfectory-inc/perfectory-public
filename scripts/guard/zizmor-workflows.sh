#!/usr/bin/env bash
# GitHub Actions security audits by zizmor (tools/zizmor.env, config .github/zizmor.yml), pedantic
# persona, failing on any finding of medium severity or above. It owns the generic checks the
# hand-written parser in check-workflow-policy.sh used to carry:
#   - every Action and docker:// action pinned to a full commit SHA or digest (unpinned-uses):
#     a moved tag runs someone else's code with our token (tj-actions/changed-files, 2025-03-14);
#   - every service and container image pinned to a digest (unpinned-images);
#   - actions/checkout with persist-credentials: false (artipacked): a persisted token in
#     .git/config leaks into uploaded artifacts and caches;
# plus zizmor's other audits (dangerous triggers, template injection, cache poisoning).
# Repository contracts (required contexts, the Action allowlist, area working directories) stay
# in check-workflow-policy.sh.
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
workflows="${2:-$root/.github/workflows}"
cd "$root"
if ! bash scripts/ci/zizmor.sh --offline --persona pedantic --min-severity medium \
  --config "$root/.github/zizmor.yml" --format plain "$workflows"; then
  echo "FAIL zizmor-workflows: zizmor reported the findings above" >&2
  exit 1
fi
echo "OK zizmor-workflows"
