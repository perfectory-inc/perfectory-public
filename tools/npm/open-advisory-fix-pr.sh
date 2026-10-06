#!/usr/bin/env bash
# Opens the pull request for a new npm advisory in one command (root ADR-0158).
#
# With a run id it applies the patch the failed repository guard run prepared (artifact
# `repository-supply-chain`, path npm-advisory-fix/); without one it scans and prepares here.
# Either way the branch starts from origin/main, the overrides check must pass, and the push
# and the pull request use the caller's own GitHub credentials: the non-main branch firewall
# admits the maintainer, and no workflow holds a write token (tools/github/workflow-permissions.json).
#
# usage: bash tools/npm/open-advisory-fix-pr.sh [<run-id>]
set -euo pipefail

for command_name in gh git node; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL open-advisory-fix-pr: missing command '$command_name'" >&2
    exit 1
  }
done
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
if [ -n "$(git status --porcelain=v1 --untracked-files=no)" ]; then
  echo "FAIL open-advisory-fix-pr: the working tree has changes" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT

git fetch --quiet origin main
login="$(gh api user --jq .login)"
branch="$login/npm-advisory-fix-$(date -u +%Y%m%d-%H%M%S)"
git switch --quiet --create "$branch" origin/main

if [ "$#" -eq 1 ]; then
  gh run download "$1" --name repository-supply-chain --dir "$work"
  fix="$work/npm-advisory-fix"
  if [ ! -s "$fix/npm-advisory-fix.patch" ]; then
    cat "$fix/summary.md" 2>/dev/null || true
    echo "FAIL open-advisory-fix-pr: run $1 prepared no patch" >&2
    exit 1
  fi
  git apply --index "$fix/npm-advisory-fix.patch"
else
  report="$work/osv-report.json"
  bash scripts/ci/osv-vulnerability-gate.sh --report-path "$report" "$root" || true
  fix="$work/npm-advisory-fix"
  bash tools/npm/prepare-advisory-fix.sh "$report" "$fix"
  if [ ! -s "$fix/npm-advisory-fix.patch" ]; then
    echo "FAIL open-advisory-fix-pr: nothing an override can fix; see the summary above" >&2
    exit 1
  fi
  git add --update
fi

node tools/npm/security-overrides.mjs check
advisories="$(git diff --cached --unified=0 -- tools/npm/security-overrides.contract.json | grep -E '^\+ ' | grep -oE 'GHSA(-[23456789cfghjmpqrvwx]{4}){3}' | sort -u | paste -sd, - || true)"
title="fix(deps): raise npm advisory floors (${advisories:-see contract})"
git commit --quiet --message "$title" --message "$(cat "$fix/summary.md")"
git push --quiet --set-upstream origin "$branch"
gh pr create --base main --head "$branch" --title "$title" --body-file "$fix/summary.md"
