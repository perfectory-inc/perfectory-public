#!/usr/bin/env bash
# The two fences open-advisory-fix-pr.sh puts around a CI-prepared patch before it is applied
# and pushed under the maintainer's credentials (root ADR-0158). Sourced; the self-test plants
# each refusal (scripts/guard/npm-advisory-fix-guards-self-test.sh).

# A patch may come only from this repository's own main: a pull request (a fork's included)
# chooses its artifact's contents. Args: <run-id> <owner/repo>.
advisory_fix_require_main_run() {
  local run="$1" repository="$2" origin_of_run
  origin_of_run="$(gh run view "$run" --json event,headBranch,headRepository \
    --jq '[.event, .headBranch, (.headRepository.nameWithOwner // "")] | join(" ")')"
  case "$origin_of_run" in
    "schedule main $repository" | "push main $repository" | "workflow_dispatch main $repository") return 0 ;;
  esac
  echo "FAIL open-advisory-fix-pr: run $run is '$origin_of_run'; only a schedule, push or dispatch run on main of $repository may supply the patch" >&2
  return 1
}

# The patch may touch the overrides contract, package manifests and lockfiles, nothing else.
# Args: <patch-file>.
advisory_fix_require_patch_paths() {
  local patch="$1" outside
  outside="$(git apply --numstat "$patch" | awk -F'\t' '{print $3}' |
    grep -vE '^(tools/npm/security-overrides\.contract\.json|(.+/)?package\.json|(.+/)?pnpm-lock\.yaml)$' || true)"
  [ -z "$outside" ] && return 0
  echo "FAIL open-advisory-fix-pr: the patch touches files outside the contract, manifests and lockfiles:" >&2
  printf '  %s\n' $outside >&2
  return 1
}
