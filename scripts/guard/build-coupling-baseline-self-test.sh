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
  local output
  if output="$(bash "$checker" "$dir" "$build_baseline" "$read_baseline" 2>&1)"; then
    echo "FAIL build-coupling-baseline-self-test: $label should have been rejected" >&2
    exit 1
  fi
  # A crash is not a rejection: it would pass here and fail every real run.
  case "$output" in
    *Traceback*)
      printf 'FAIL build-coupling-baseline-self-test: %s crashed the checker:\n%s\n' "$label" "$output" >&2
      exit 1
      ;;
  esac
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

# Every read an image compiles must be in that image (build-coupling-baseline.py). 2026-10-29 the
# publisher image lacked contracts/: each release build on main failed for 11 hours, and the pull
# requests, compiled from the checkout, all passed. A workspace whose `app` is built by a Dockerfile
# that compose names; `lib` compiles in contracts/c.json, a test module and a feature-gated module
# read files the image never gets.
make_image_fixture() {
  local dir="$1" copy_contracts="$2" ignore="$3" lib_features="$4"
  mkdir -p "$dir/ws/app/src" "$dir/ws/lib/src" "$dir/ws/contracts" "$dir/ws/outside"
  fixture_git "$dir" init -q
  fixture_git "$dir" config user.email guard@example.invalid
  fixture_git "$dir" config user.name guard
  printf '[workspace]\nmembers = ["app", "lib"]\n' >"$dir/ws/Cargo.toml"
  printf '[package]\nname = "app"\n\n[dependencies]\nlib = { path = "../lib"%s }\n\n[dev-dependencies]\nlib = { path = "../lib", features = ["fixtures"] }\n' \
    "$lib_features" >"$dir/ws/app/Cargo.toml"
  printf 'fn main() {}\n' >"$dir/ws/app/src/main.rs"
  printf '[package]\nname = "lib"\n\n[features]\nfixtures = []\n' >"$dir/ws/lib/Cargo.toml"
  {
    printf '// A comment naming include_str!("../../outside/x.json") reads nothing.\n'
    printf 'pub const C: &str = include_str!(\n    "../../contracts/c.json"\n);\n'
    printf '#[cfg(feature = "fixtures")]\npub mod fixtures;\n'
    printf '#[cfg(test)]\nmod tests;\n'
    printf '#[cfg(test)]\nmod inline {\n    const X: &str = include_str!("../../outside/x.json");\n}\n'
  } >"$dir/ws/lib/src/lib.rs"
  printf 'const Y: &str = include_str!("../../outside/y.json");\n' >"$dir/ws/lib/src/fixtures.rs"
  printf 'const X: &str = include_str!("../../outside/x.json");\n' >"$dir/ws/lib/src/tests.rs"
  printf '{}\n' >"$dir/ws/contracts/c.json"
  printf '{}\n' >"$dir/ws/outside/x.json"
  printf '{}\n' >"$dir/ws/outside/y.json"
  {
    printf 'FROM rust:1 AS chef\nWORKDIR /w\n'
    printf 'FROM chef AS build\nCOPY Cargo.toml /w/\nCOPY app /w/app\nCOPY lib /w/lib\n'
    [ "$copy_contracts" = yes ] && printf 'COPY contracts /w/contracts\n'
    printf 'RUN cargo build --locked --release \\\n    -p app\n'
  } >"$dir/ws/Dockerfile"
  printf 'services:\n  app:\n    build:\n      context: .\n      dockerfile: Dockerfile\n' >"$dir/ws/compose.yml"
  [ -z "$ignore" ] || printf '%s\n' "$ignore" >"$dir/ws/.dockerignore"
  fixture_git "$dir" add -A
  fixture_git "$dir" -c commit.gpgsign=false commit -q -m fixture
}

# Five lines the count sees (it counts the comment too); the image check reads none of the comment.
image_holds="$test_root/image-holds"
make_image_fixture "$image_holds" yes "" ""
expect_accepted "an image that holds what it compiles in; test and unused-feature reads left out" "$image_holds" 0 5

image_lacks="$test_root/image-lacks"
make_image_fixture "$image_lacks" no "" ""
expect_rejected "an image whose build stage never copies a file it compiles in (2026-10-29)" "$image_lacks" 0 5

image_ignores="$test_root/image-ignores"
make_image_fixture "$image_ignores" yes "**/contracts" ""
expect_rejected "a compiled-in file the .dockerignore keeps out of the context" "$image_ignores" 0 5

image_feature="$test_root/image-feature"
make_image_fixture "$image_feature" yes "" ', features = ["fixtures"]'
expect_rejected "a feature a normal dependency switches on compiles a read the image lacks" "$image_feature" 0 5

echo "OK build-coupling-baseline-self-test"
