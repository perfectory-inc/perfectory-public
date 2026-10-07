#!/usr/bin/env bash
# Plants every refusal of tools/npm/advisory-fix-guards.sh (root ADR-0158): a patch from any run but
# this repository's main, and a patch that touches anything but the overrides contract, package
# manifests and lockfiles. A fake `gh` answers the run's origin; patches are real git diffs.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
# shellcheck source=tools/npm/advisory-fix-guards.sh
source "$root/tools/npm/advisory-fix-guards.sh"

work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
repository="perfectory-inc/perfectory-public"
failures=0
expect() {
  local want="$1" label="$2"
  shift 2
  if "$@" 2>/dev/null; then got=accepted; else got=refused; fi
  if [ "$got" != "$want" ]; then
    echo "FAIL npm-advisory-fix-guards-self-test: $label was $got, expected $want" >&2
    failures=$((failures + 1))
  fi
}

# The run's origin, as `gh run view --jq` would print it.
gh() { printf '%s\n' "$FAKE_RUN_ORIGIN"; }
for origin in "schedule main $repository" "push main $repository" "workflow_dispatch main $repository"; do
  FAKE_RUN_ORIGIN="$origin" expect accepted "a run '$origin'" advisory_fix_require_main_run 1 "$repository"
done
for origin in "pull_request main $repository" "pull_request feature someone/fork" \
              "schedule feature $repository" "push main someone/fork" "merge_group main $repository" " "; do
  FAKE_RUN_ORIGIN="$origin" expect refused "a run '$origin'" advisory_fix_require_main_run 1 "$repository"
done
unset -f gh

# Patches made by git from a scratch repository.
git -C "$work" init -q
mkdir -p "$work/tools/npm" "$work/products/app" "$work/.github/workflows"
for path in tools/npm/security-overrides.contract.json products/app/package.json \
            products/app/pnpm-lock.yaml package.json .github/workflows/ci.yml products/app/index.ts; do
  printf 'before\n' > "$work/$path"
done
git -C "$work" add -A
git -C "$work" -c user.name=t -c user.email=t@t commit -qm base
patch_of() {
  local name="$1"
  shift
  for path in "$@"; do printf 'after\n' > "$work/$path"; done
  git -C "$work" diff > "$work/$name.patch"
  git -C "$work" checkout -q -- .
}
patch_of allowed tools/npm/security-overrides.contract.json products/app/package.json products/app/pnpm-lock.yaml package.json
patch_of workflow products/app/package.json .github/workflows/ci.yml
patch_of source products/app/pnpm-lock.yaml products/app/index.ts
cd "$work"
expect accepted "a contract, manifest and lockfile patch" advisory_fix_require_patch_paths "$work/allowed.patch"
expect refused "a patch touching a workflow" advisory_fix_require_patch_paths "$work/workflow.patch"
expect refused "a patch touching source" advisory_fix_require_patch_paths "$work/source.patch"
cd "$root"

if [ "$failures" -ne 0 ]; then
  exit 1
fi
echo "OK npm-advisory-fix-guards-self-test"
