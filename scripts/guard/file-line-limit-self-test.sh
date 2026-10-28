#!/usr/bin/env bash
# Proves the line-limit ratchet rejects an unrecorded long file, a recorded one that grew, and an
# exception with no reason, and admits generated files and a recorded file under its ceiling.
set -euo pipefail
# The guard lists files with `git ls-files`, so each fixture is a real repository (see the
# incident in lib/fixture-repo.sh for why the git binding is pinned to the fixture).
. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/file-line-limit.sh"

fixture_root
test_root="$FIXTURE_ROOT"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

lines() { awk -v n="$1" 'BEGIN { for (i = 0; i < n; i++) print "x" }'; }

# make_fixture <name> <file> <line count> <exceptions json>
make_fixture() {
  local dir="$test_root/$1"
  mkdir -p "$dir/src" "$dir/tools/ci"
  lines "$3" >"$dir/$2"
  lines 10 >"$dir/src/short.rs"
  printf '%s\n' "$4" >"$dir/tools/ci/file-line-limit.exceptions.json"
  fixture_git "$dir" init -q
  fixture_git "$dir" add -A
  printf '%s' "$dir"
}

none='{"limit": 1500, "generated": ["*.generated.ts"], "exceptions": {}}'
ceiling() { printf '{"limit": 1500, "generated": [], "exceptions": {"src/long.rs": {"lines": 1600, "reason": "%s"}}}' "$1"; }

expect() {
  local verdict="$1" label="$2" dir="$3"
  local err
  # A rejection must be the guard's verdict, not a crash.
  if err="$(bash "$checker" "$dir" 2>&1 >/dev/null)"; then got=accepted
  elif printf '%s' "$err" | grep -q '^FAIL file-line-limit:'; then got=rejected
  else got="a crash: $err"; fi
  if [ "$got" != "$verdict" ]; then
    echo "FAIL file-line-limit-self-test: $label was $got" >&2
    exit 1
  fi
}

expect accepted "the repository" "$root"
expect accepted "a file at the limit" "$(make_fixture at src/long.rs 1500 "$none")"
expect rejected "an unrecorded file over the limit" "$(make_fixture over src/long.rs 1501 "$none")"
expect accepted "a generated file over the limit" "$(make_fixture generated src/api.generated.ts 4000 "$none")"
expect accepted "a recorded file under its ceiling" "$(make_fixture under src/long.rs 1550 "$(ceiling split)")"
expect rejected "a recorded file past its ceiling" "$(make_fixture grew src/long.rs 1601 "$(ceiling split)")"
expect rejected "an exception with no reason" "$(make_fixture reasonless src/long.rs 1550 "$(ceiling ' ')")"

echo "OK file-line-limit-self-test"
