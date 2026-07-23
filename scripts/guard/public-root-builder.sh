#!/usr/bin/env bash
# Proves that the publication helper copies only the selected tree into a new
# parentless commit, even when the source repository has sensitive ancestry.
set -euo pipefail
cd "$(dirname "$0")/../.."

builder="scripts/github/build-public-root.sh"
if [ ! -x "$builder" ]; then
  echo "FAIL public-root-builder: missing executable $builder" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

source_repo="$test_root/source"
snapshot_repo="$test_root/public.git"
git init -q --initial-branch=main "$source_repo"
git -C "$source_repo" config user.name "Synthetic Test Author"
git -C "$source_repo" config user.email "author@example.invalid"

printf '%s\n' 'synthetic-private-history-sentinel' >"$source_repo/internal-evidence.txt"
git -C "$source_repo" add internal-evidence.txt
git -C "$source_repo" commit -q -m "synthetic private ancestor"
private_commit="$(git -C "$source_repo" rev-parse HEAD)"

rm "$source_repo/internal-evidence.txt"
printf '%s\n' 'publication-safe tree' >"$source_repo/README.md"
git -C "$source_repo" add -A
git -C "$source_repo" commit -q -m "synthetic publication source"
source_commit="$(git -C "$source_repo" rev-parse HEAD)"
source_tree="$(git -C "$source_repo" rev-parse 'HEAD^{tree}')"

GIT_AUTHOR_NAME=Hostile \
GIT_AUTHOR_EMAIL=hostile@example.invalid \
GIT_AUTHOR_DATE='@1 +0900' \
GIT_COMMITTER_NAME=Hostile \
GIT_COMMITTER_EMAIL=hostile@example.invalid \
GIT_COMMITTER_DATE='@1 +0900' \
GIT_CONFIG_COUNT=1 \
GIT_CONFIG_KEY_0=user.name \
GIT_CONFIG_VALUE_0=Hostile \
  "$builder" "$source_repo" "$source_commit" "$snapshot_repo" >/dev/null

snapshot_commit="$(git --git-dir="$snapshot_repo" rev-parse refs/heads/main)"
snapshot_tree="$(git --git-dir="$snapshot_repo" rev-parse 'refs/heads/main^{tree}')"
commit_count="$(git --git-dir="$snapshot_repo" rev-list --count refs/heads/main)"
parent_count="$(git --git-dir="$snapshot_repo" cat-file -p "$snapshot_commit" | grep -c '^parent ' || true)"
ref_count="$(git --git-dir="$snapshot_repo" for-each-ref --format='%(refname)' | wc -l | tr -d ' ')"

if [ "$snapshot_tree" != "$source_tree" ] \
  || [ "$commit_count" -ne 1 ] \
  || [ "$parent_count" -ne 0 ] \
  || [ "$ref_count" -ne 1 ]; then
  echo "FAIL public-root-builder: snapshot tree or ancestry invariant failed" >&2
  exit 1
fi
if git --git-dir="$snapshot_repo" cat-file -e "$source_commit^{commit}" 2>/dev/null \
  || git --git-dir="$snapshot_repo" cat-file -e "$private_commit^{commit}" 2>/dev/null \
  || git --git-dir="$snapshot_repo" grep -q 'synthetic-private-history-sentinel' refs/heads/main; then
  echo "FAIL public-root-builder: source history leaked into the snapshot" >&2
  exit 1
fi
if [ "$(git --git-dir="$snapshot_repo" for-each-ref --format='%(refname)')" != "refs/heads/main" ]; then
  echo "FAIL public-root-builder: snapshot contains an unexpected ref" >&2
  exit 1
fi

echo "OK public-root-builder"
