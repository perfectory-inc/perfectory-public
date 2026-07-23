#!/usr/bin/env bash
# Runs every monorepo guard. Each guard documents the incident it prevents.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$dir/../.." && pwd -P)"
repository_identity_gate="$root/scripts/guard/repository-identity-ci.sh"
legal_gate="$root/scripts/guard/legal-publication-ci.sh"
bash "$repository_identity_gate"
bash "$legal_gate"
rc=0
for g in no-subdir-github toolchain-consistency migration-naming \
         unique-package-names no-stale-sibling-paths health-route-conformance \
         no-adhoc-cargo-lint xtask-path-coverage \
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
         publication-authority-self-test \
         public-github-policy public-root-builder public-root-publisher-self-test \
         import-private-feature-diff-self-test; do
  bash "$dir/$g.sh" || rc=1
done
exit "$rc"
