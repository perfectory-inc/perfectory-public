#!/usr/bin/env bash
# Proves area-registry.sh passes a clean tree and refuses a hand-written area list and an area no
# change-scope workflow covers.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"

guard="$(cd "$(dirname "$0")" && pwd)/area-registry.sh"
fixture_root
test_root="$FIXTURE_ROOT"
trap 'rm -rf -- "$test_root"' EXIT

fail() {
  echo "FAIL area-registry-self-test: $*" >&2
  exit 1
}

# Builds a clean fixture tree in $1: five synthetic areas, all covered by change-scope.
build() {
  local repo="$1" dir
  mkdir -p "$repo/tools/ci"
  fixture_git "$repo" init -q --initial-branch=main
  {
    echo '{ "areas": ['
    echo '  { "slug": "a1", "dir": "zone/a1", "title": "A1" },'
    echo '  { "slug": "a2", "dir": "zone/a2", "title": "A2" },'
    echo '  { "slug": "a3", "dir": "zone/a3", "title": "A3" },'
    echo '  { "slug": "a4", "dir": "zone/a4", "title": "A4" },'
    echo '  { "slug": "a5", "dir": "zone/a5", "title": "A5" }'
    echo '] }'
  } >"$repo/tools/areas.contract.json"
  for dir in a1 a2 a3 a4 a5; do
    mkdir -p "$repo/zone/$dir"
    printf 'synthetic\n' >"$repo/zone/$dir/README.txt"
  done
  printf '{ "workflows": { "ci.yml": { "paths": ["zone/a1/**", "zone/a2/**", "zone/a3/**", "zone/a4/**", "zone/a5/**"] } } }\n' \
    >"$repo/tools/ci/change-scope.json"
}

expect_refused() {
  local repo="$1" needle="$2" label="$3"
  fixture_git "$repo" add -A
  if bash "$guard" "$repo" >"$test_root/$label.out" 2>&1; then
    fail "$label: accepted"
  fi
  grep -qF "$needle" "$test_root/$label.out" || fail "$label: refused for the wrong reason: $(cat "$test_root/$label.out")"
}

# (c) the clean tree passes.
clean="$test_root/clean"
build "$clean"
fixture_git "$clean" add -A
bash "$guard" "$clean" >/dev/null || fail "clean fixture refused"

# (a) a non-markdown file listing four of five areas by hand is refused.
listed="$test_root/listed"
build "$listed"
printf 'AREAS = ["zone/a1", "zone/a2", "zone/a3", "zone/a4"]\n' >"$listed/areas.py"
expect_refused "$listed" "areas.py names 4 of 5 areas by hand" listed

# (b) an area no change-scope workflow watches or owns is refused.
uncovered="$test_root/uncovered"
build "$uncovered"
printf '{ "workflows": { "ci.yml": { "paths": ["zone/a1/**", "zone/a2/**", "zone/a3/**", "zone/a4/**"] } } }\n' \
  >"$uncovered/tools/ci/change-scope.json"
expect_refused "$uncovered" "no workflow watches or owns area zone/a5/**" uncovered

echo "OK area-registry-self-test"
