#!/usr/bin/env bash
# Proves tracked-paths-are-portable refuses the names that leaked in with PR #478
# and the other names Windows cannot check out, and accepts ordinary ones.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"

guard="$(cd "$(dirname "$0")" && pwd)/tracked-paths-are-portable.sh"
fixture_root
test_root="$FIXTURE_ROOT"
repo="$test_root/repo"
mkdir -p "$repo"
fixture_git "$repo" init -q --initial-branch=main
# Git itself refuses `:` and `\` in an index entry by default; a clone or a
# checkout with this off does not, so the guard must not lean on it.
fixture_git "$repo" config core.protectNTFS false
blob="$(printf 'synthetic\n' | fixture_git "$repo" hash-object -w --stdin)"

# Index entries only: some of these names cannot exist on this file system, and
# the guard reads the index, which is what a commit would carry.
stage() { fixture_git "$repo" update-index --add --cacheinfo "100644,$blob,$1"; }
unstage() { fixture_git "$repo" update-index --force-remove -- "$1"; }

stage "area/docs/readme.md"
stage "area/.github-like/config.v1.json"
if ! bash "$guard" "$repo" >"$test_root/clean.out" 2>&1; then
  cat "$test_root/clean.out" >&2
  echo "FAIL tracked-paths-are-portable-self-test: refused ordinary paths" >&2
  exit 1
fi

colon=$'\xef\x80\xba'  # U+F03A, how Git for Windows stores ':'
for bad in \
  "area/C${colon}UsersexampleTempautodeploy-0000state/lock" \
  'area/C:Usersexample/lock' \
  'area/a\b.txt' \
  'area/what?.txt' \
  'area/trailing-dot./x' \
  'area/nul.txt' \
  $'area/tab\there'; do
  stage "$bad"
  if bash "$guard" "$repo" >"$test_root/bad.out" 2>&1; then
    echo "FAIL tracked-paths-are-portable-self-test: accepted ${bad@Q}" >&2
    exit 1
  fi
  grep -q 'FAIL tracked-paths-are-portable' "$test_root/bad.out" || {
    cat "$test_root/bad.out" >&2
    echo "FAIL tracked-paths-are-portable-self-test: refusal of ${bad@Q} did not name the guard" >&2
    exit 1
  }
  unstage "$bad"
done

echo "OK tracked-paths-are-portable-self-test"
