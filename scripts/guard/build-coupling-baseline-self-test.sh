#!/usr/bin/env bash
# Proves the build-coupling ratchet fails in both directions.
#
# A ceiling that only rejects growth is half a ratchet: once the real count drops,
# a stale-high baseline silently re-authorises everything between the two numbers.
# So "fell below the baseline" has to fail as loudly as "rose above" it, and both
# paths need a fixture — the real repository sits at exactly the baseline by
# construction, so it can only ever exercise the passing case.
set -euo pipefail
# This test builds real Git repositories. Under a pre-push hook the inherited
# GIT_DIR made every `git -C "$dir"` below address the real checkout instead,
# and the five fixture commits landed on the branch being pushed. `fixture_git`
# pins the binding to $dir and refuses any target outside the directory
# `fixture_root` created, so that outcome is no longer expressible here.
. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/build-coupling-baseline.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL build-coupling-baseline-self-test: missing $checker" >&2
  exit 1
fi

fixture_root
test_root="$FIXTURE_ROOT"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

# The guard discovers through `git ls-files`/`git grep`, so a fixture has to be a
# real repository with the files committed. That is deliberate: it is exactly what
# keeps an ignored or generated `build.rs` out of the count.
make_fixture() {
  local dir="$1" build_scripts="$2" reads="$3"
  mkdir -p "$dir/crate/src"
  fixture_git "$dir" init -q
  fixture_git "$dir" config user.email guard@example.invalid
  fixture_git "$dir" config user.name guard

  local index=0
  while [ "$index" -lt "$build_scripts" ]; do
    mkdir -p "$dir/crate$index"
    printf 'fn main() {}\n' >"$dir/crate$index/build.rs"
    index=$((index + 1))
  done

  {
    printf 'pub fn main() {}\n'
    index=0
    while [ "$index" -lt "$reads" ]; do
      printf 'const D%s: &str = include_str!("d%s.txt");\n' "$index" "$index"
      index=$((index + 1))
    done
  } >"$dir/crate/src/lib.rs"

  fixture_git "$dir" add -A
  fixture_git "$dir" -c commit.gpgsign=false commit -q -m fixture
}

expect_accepted() {
  local label="$1" dir="$2" build_baseline="$3" read_baseline="$4"
  if ! bash "$checker" "$dir" "$build_baseline" "$read_baseline" >/dev/null 2>&1; then
    echo "FAIL build-coupling-baseline-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1" dir="$2" build_baseline="$3" read_baseline="$4"
  if bash "$checker" "$dir" "$build_baseline" "$read_baseline" >/dev/null 2>&1; then
    echo "FAIL build-coupling-baseline-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

# On the baseline exactly.
at_baseline="$test_root/at-baseline"
make_fixture "$at_baseline" 1 3
expect_accepted "counts equal to the baseline" "$at_baseline" 1 3

# Grew: one more build script than declared.
extra_script="$test_root/extra-script"
make_fixture "$extra_script" 2 3
expect_rejected "an undeclared build script" "$extra_script" 1 3

# Grew: one more compile-time read than declared. Counted as occurrences, so a
# single file gaining a second `include_str!` still trips it.
extra_read="$test_root/extra-read"
make_fixture "$extra_read" 1 4
expect_rejected "an undeclared compile-time file read" "$extra_read" 1 3

# Shrank: the baseline outlived the coupling it described. Left alone it would
# re-authorise everything between the two numbers without anyone noticing.
shrunk="$test_root/shrunk"
make_fixture "$shrunk" 1 2
expect_rejected "a baseline left high after the real count dropped" "$shrunk" 1 3

# A repository with no coupling at all is a legitimate state and must be
# expressible, or the guard would force a floor it never intended to set.
none="$test_root/none"
make_fixture "$none" 0 0
expect_accepted "no build scripts and no compile-time reads" "$none" 0 0

echo "OK build-coupling-baseline-self-test"
