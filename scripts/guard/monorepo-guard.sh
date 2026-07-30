#!/usr/bin/env bash
# Runs every monorepo guard. Each guard documents the incident it prevents.
set -euo pipefail
# Git for Windows exports drive-letter paths to hooks, while the repository
# guards run under Git Bash/WSL.  Normalize that one path before any guard uses
# `git -C`; otherwise linked worktrees resolve `.git` as a literal `C:/...`
# child path and the guard either fails or can target the wrong index.
normalize_git_path() {
  local value="${1:-}"
  if [[ "$value" =~ ^([A-Za-z]):[\\/](.*)$ ]]; then
    if command -v wslpath >/dev/null 2>&1; then
      wslpath -u "$value"
      return
    fi
    if command -v cygpath >/dev/null 2>&1; then
      cygpath -u "$value"
      return
    fi
    local drive="${BASH_REMATCH[1],,}"
    local rest="${BASH_REMATCH[2]//\\//}"
    rest="${rest#/}"
    printf '/mnt/%s/%s' "$drive" "$rest"
  else
    printf '%s' "$value"
  fi
}
if [ -n "${GIT_DIR:-}" ]; then
  export GIT_DIR="$(normalize_git_path "$GIT_DIR")"
fi
if [ -n "${GIT_WORK_TREE:-}" ]; then
  export GIT_WORK_TREE="$(normalize_git_path "$GIT_WORK_TREE")"
fi
if [ -n "${GIT_COMMON_DIR:-}" ]; then
  export GIT_COMMON_DIR="$(normalize_git_path "$GIT_COMMON_DIR")"
fi
if [ -n "${GIT_INDEX_FILE:-}" ]; then
  export GIT_INDEX_FILE="$(normalize_git_path "$GIT_INDEX_FILE")"
fi
if [ -n "${GIT_OBJECT_DIRECTORY:-}" ]; then
  export GIT_OBJECT_DIRECTORY="$(normalize_git_path "$GIT_OBJECT_DIRECTORY")"
fi
dir="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$dir/../.." && pwd -P)"
repository_identity_gate="$root/scripts/guard/repository-identity-ci.sh"
legal_gate="$root/scripts/guard/legal-publication-ci.sh"
bash "$repository_identity_gate"
bash "$legal_gate"
rc=0
for g in no-subdir-github toolchain-consistency technology-version-consistency \
         foundation-kafka-contract \
         backend-profile-consistency backend-profile-consistency-self-test \
         r2-env-namespace-consistency r2-env-namespace-consistency-self-test \
         technology-version-consistency-self-test migration-naming \
         unique-package-names no-stale-sibling-paths health-route-conformance \
         no-adhoc-cargo-lint foundation-parcel-current-selector-self-test \
         foundation-parcel-current-selector xtask-path-coverage \
         lefthook-advisory-policy-self-test lefthook-advisory-policy \
         package-publication-policy-self-test public-fixture-safety-self-test \
         public-doc-boundary-self-test \
         tracked-blob-sizes-self-test public-repository-safety \
         container-runtime-policy-self-test container-runtime-policy \
         workflow-policy-self-test github-policy-json-self-test \
         repository-identity-policy-self-test \
         legal-publication-self-test \
         third-party-artifact-policy-self-test \
         gitleaks-policy-self-test \
         actions-cache-controls-self-test billing-budgets-self-test \
         tiles-slice-proof-env-self-test \
         live-lane-completeness-self-test live-lane-completeness \
         build-coupling-baseline-self-test build-coupling-baseline \
         no-silent-test-skip-self-test no-silent-test-skip \
         no-env-access-in-domain-layers-self-test no-env-access-in-domain-layers \
         publication-authority-self-test \
         public-github-policy public-root-builder public-root-publisher-self-test \
         import-private-feature-diff-self-test; do
  bash "$dir/$g.sh" || rc=1
done
exit "$rc"
