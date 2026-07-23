#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
release_script="${repo_root}/scripts/deploy/foundation-release.sh"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT

release_root="${test_root}/opt/foundation-platform"
state_root="${test_root}/var/lib/foundation-platform"
release_a="1111111111111111111111111111111111111111"
release_b="2222222222222222222222222222222222222222"

mkdir -p "${test_root}/source-a" "${test_root}/source-b"
printf 'release-a\n' >"${test_root}/source-a/version.txt"
printf 'release-b\n' >"${test_root}/source-b/version.txt"
tar -C "${test_root}/source-a" -czf "${test_root}/release-a.tar.gz" .
tar -C "${test_root}/source-b" -czf "${test_root}/release-b.tar.gz" .

run_release() {
  FOUNDATION_PLATFORM_RELEASE_ROOT="${release_root}" \
  FOUNDATION_PLATFORM_STATE_ROOT="${state_root}" \
    "${release_script}" "$@"
}

assert_link() {
  local link_path="$1"
  local expected="$2"
  local actual
  actual="$(readlink "${link_path}")"
  [[ "${actual}" == "${expected}" ]] || {
    printf 'expected %s -> %s, got %s\n' "${link_path}" "${expected}" "${actual}" >&2
    exit 1
  }
}

run_release install "${release_a}" "${test_root}/release-a.tar.gz"
assert_link "${release_root}/current" "releases/${release_a}"
[[ "$(cat "${release_root}/current/version.txt")" == "release-a" ]]
[[ "$(stat -c '%a' "${release_root}/releases/${release_a}")" == "755" ]]
[[ -d "${state_root}/recovery" ]]

run_release install "${release_a}" "${test_root}/release-a.tar.gz"
assert_link "${release_root}/current" "releases/${release_a}"

run_release install "${release_b}" "${test_root}/release-b.tar.gz"
assert_link "${release_root}/current" "releases/${release_b}"
assert_link "${release_root}/previous" "releases/${release_a}"
[[ "$(cat "${release_root}/current/version.txt")" == "release-b" ]]

[[ -w "${state_root}/lakehouse" ]]
[[ -w "${state_root}/remote-lakehouse" ]]
rm -rf "${state_root}/lakehouse" "${state_root}/remote-lakehouse"

run_release rollback
assert_link "${release_root}/current" "releases/${release_a}"
assert_link "${release_root}/previous" "releases/${release_b}"
[[ "$(cat "${release_root}/current/version.txt")" == "release-a" ]]
[[ -w "${state_root}/lakehouse" ]]
[[ -w "${state_root}/remote-lakehouse" ]]

run_release activate "${release_b}"
assert_link "${release_root}/current" "releases/${release_b}"

if run_release install invalid-sha "${test_root}/release-a.tar.gz"; then
  printf 'invalid release id was accepted\n' >&2
  exit 1
fi

cp "${test_root}/release-b.tar.gz" "${test_root}/release-a-mutated.tar.gz"
if run_release install "${release_a}" "${test_root}/release-a-mutated.tar.gz"; then
  printf 'release id reuse with different archive was accepted\n' >&2
  exit 1
fi

printf 'foundation-release-test=pass\n'
