#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/area-ci-input-coverage.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL area-ci-input-coverage-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Builds a synthetic repository: one area CI, its change-scope contract entry,
# one area source file holding the given literal, and whichever files should
# exist. Extra inputs are trailing arguments rather than one word-split string —
# a glob like `docs/architecture/**` left unquoted is pathname-expanded against
# the real repository, which silently replaced the intended entry with a list of files.
fixture() {
  local root="$1"
  local literal="$2"
  local place="$3" # area | repo | nowhere
  shift 3
  local area="platforms/example-platform"
  mkdir -p "$root/.github/workflows" "$root/$area/src" "$root/tools/ci"
  {
    printf 'name: Example CI
on:
  pull_request:
    branches: [main]
'
    printf 'jobs:
  verify:
    runs-on: ubuntu-latest
'
    printf '    steps:
      - run: cargo xtask verify %s
' "$area"
  } >"$root/.github/workflows/example-ci.yml"
  scope_contract "$root" "$area/**" "$@"
  printf 'const INPUT: &str = "%s";
' "$literal" >"$root/$area/src/lib.rs"
  case "$place" in
    area) mkdir -p "$(dirname "$root/$area/$literal")" && printf 'x
' >"$root/$area/$literal" ;;
    repo) mkdir -p "$(dirname "$root/$literal")" && printf 'x
' >"$root/$literal" ;;
    nowhere) ;;
  esac
}
# Writes tools/ci/change-scope.json scoping example-ci.yml to the given globs;
# no globs leaves the workflow unscoped.
scope_contract() {
  local root="$1"
  shift
  python3 -c 'import json, sys
globs = sys.argv[2:]
workflows = {"example-ci.yml": {"merge_group": "scoped", "paths": globs}} if globs else {}
open(sys.argv[1], "w").write(json.dumps({"workflows": workflows, "unscoped": {}}))'     "$root/tools/ci/change-scope.json" "$@"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL area-ci-input-coverage-self-test: rejected allowed fixture $1" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL area-ci-input-coverage-self-test: accepted forbidden fixture $1" >&2
    exit 1
  fi
}

# The defect this guard exists for: a repository-root input the filter omits.
uncovered="$test_root/uncovered"
fixture "$uncovered" "docs/architecture/contract.md" repo
expect_rejected "$uncovered"

covered_by_glob="$test_root/covered-by-glob"
fixture "$covered_by_glob" "docs/architecture/contract.md" repo "docs/architecture/**"
expect_allowed "$covered_by_glob"

covered_exactly="$test_root/covered-exactly"
fixture "$covered_exactly" "docs/technology-stack.md" repo "docs/technology-stack.md"
expect_allowed "$covered_exactly"

# A glob for a sibling directory must not be read as covering this one.
neighbouring_glob="$test_root/neighbouring-glob"
fixture "$neighbouring_glob" "docs/architecture/contract.md" repo "docs/adr/**"
expect_rejected "$neighbouring_glob"

# A prefix that is not a path boundary must not count as coverage.
prefix_not_boundary="$test_root/prefix-not-boundary"
fixture "$prefix_not_boundary" "docs/architecture-notes/contract.md" repo "docs/architecture/**"
expect_rejected "$prefix_not_boundary"

# Area-local reads need no filter entry: the area's own glob already reruns the
# workflow. Without this case the guard could demand entries for every file the
# area reads from itself.
area_local="$test_root/area-local"
fixture "$area_local" "docs/architecture/contract.md" area
expect_allowed "$area_local"

# A literal that is not a path at either root is assertion text, not an input.
not_a_path="$test_root/not-a-path"
fixture "$not_a_path" "docs/architecture/contract.md" nowhere
expect_allowed "$not_a_path"

# A traversal-shaped assertion can resolve to a real file outside the synthetic
# repository. It is not a CI input, and the guard must never leave repo_root while
# deciding whether a quoted string is one.
outside_assertion="$test_root/escape-root/a/b"
fixture "$outside_assertion" "../../../etc/passwd" nowhere
mkdir -p "$test_root/etc"
printf 'synthetic\n' >"$test_root/etc/passwd"
expect_allowed "$outside_assertion"

# An unscoped workflow always runs, so it cannot miss an input.
unscoped="$test_root/unscoped"
fixture "$unscoped" "docs/architecture/contract.md" repo
scope_contract "$unscoped"
expect_allowed "$unscoped"

# An unscoped workflow that still filters its pushes with `paths:` can miss an
# input on push; that filter is read in place of the contract.
push_filtered="$test_root/push-filtered"
fixture "$push_filtered" "docs/architecture/contract.md" repo
scope_contract "$push_filtered"
{
  printf 'name: Example CI
on:
  push:
    branches: [main]
    paths:
'
  printf '      - "platforms/example-platform/**"
'
  printf 'jobs:
  verify:
    runs-on: ubuntu-latest
'
  printf '    steps:
      - run: cargo xtask verify platforms/example-platform
'
} >"$push_filtered/.github/workflows/example-ci.yml"
expect_rejected "$push_filtered"

# A scoped workflow that takes the area but does not run its tests cannot miss one. The real
# repository has two such workflows on `products/gongzzang/**`; demanding coverage from them would
# be a failure with no defect behind it.
does_not_run_tests="$test_root/does-not-run-tests"
fixture "$does_not_run_tests" "docs/architecture/contract.md" repo
{
  printf 'name: Example Prepare
on:
  pull_request:
    branches: [main]
'
  printf 'jobs:
  prepare:
    runs-on: ubuntu-latest
'
  printf '    steps:
      - run: cargo sqlx prepare
'
} >"$does_not_run_tests/.github/workflows/example-ci.yml"
expect_allowed "$does_not_run_tests"

echo "OK area-ci-input-coverage-self-test"
