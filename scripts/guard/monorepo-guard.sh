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
# The ordered list of guards, and what each one reads, lives in
# tools/ci/change-scope.json (root ADR-0207). On a pull request the selector keeps
# the guards whose inputs changed; every other run is the whole list.
guards="$(python3 "$root/scripts/ci/guard_scope.py" select)" || exit 1
rc=0
for g in $guards; do
  bash "$dir/$g.sh" || rc=1
done
exit "$rc"
