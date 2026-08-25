#!/usr/bin/env bash
# Runs every monorepo guard. Each guard documents the incident it prevents.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd)"
# Git starts a hook with GIT_DIR pointing at the real repository and
# GIT_WORK_TREE unset. A guard that builds a temporary git fixture inherits that
# binding, and every `git -C "$tmp" ...` it runs then addresses this checkout
# with $tmp as the working tree: on 2026-08-16 that put five `fixture` commits on
# the branch being pushed, one of which deleted 2,169 files -- including
# .github/workflows, so the pull request matched no workflow and reported no CI
# runs at all.
#
# This prologue used to normalize those variables and re-export them, which
# handed every guard a *usable* pointer at the real repository. Nothing under
# scripts/ reads them (guards address the repository by path), so the runner
# releases them instead. scripts/guard/lib/fixture-repo.sh holds the single
# definition of that release; hook-isolation-self-test.sh proves the children of
# this script inherit no binding, and that an unprotected fixture builder would
# still be corrupted by one.
. "$dir/lib/fixture-repo.sh"
# Lets a guard tell "run by the runner" from "run by hand" without handing it a
# repository binding to do it with.
export PERFECTORY_GUARD_RUNNER=monorepo-guard
root="$(cd "$dir/../.." && pwd -P)"
repository_identity_gate="$root/scripts/guard/repository-identity-ci.sh"
legal_gate="$root/scripts/guard/legal-publication-ci.sh"
bash "$repository_identity_gate"
bash "$legal_gate"
rc=0
for g in hook-isolation-self-test \
         no-subdir-github toolchain-consistency technology-version-consistency \
         static-release-toolchain-ssot-self-test static-release-toolchain-ssot \
         foundation-kafka-contract \
         backend-profile-consistency backend-profile-consistency-self-test \
         r2-env-namespace-consistency r2-env-namespace-consistency-self-test \
         technology-version-consistency-self-test migration-naming \
         unique-package-names no-stale-sibling-paths health-route-conformance \
         utility-library-policy-self-test utility-library-policy \
         hand-rolled-error-boundary-self-test hand-rolled-error-boundary \
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
         osv-vulnerability-ratchet-self-test \
         actions-cache-controls-self-test billing-budgets-self-test \
         tiles-slice-proof-env-self-test \
         foundation-ci-scope-self-test \
         live-lane-completeness-self-test live-lane-completeness \
         build-coupling-baseline-self-test build-coupling-baseline \
         no-silent-test-skip-self-test no-silent-test-skip \
         no-env-access-in-domain-layers-self-test no-env-access-in-domain-layers \
         area-ci-input-coverage-self-test area-ci-input-coverage \
         document-contract-markers-self-test document-contract-markers \
         roadmap-owns-recorded-debt-self-test roadmap-owns-recorded-debt \
         judgment-position-exit-codes-self-test judgment-position-exit-codes \
         publication-authority-self-test \
         public-github-policy public-root-builder public-root-publisher-self-test \
         import-private-feature-diff-self-test; do
  bash "$dir/$g.sh" || rc=1
done
exit "$rc"
