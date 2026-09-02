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

# A release carries its own deploy scripts, and `install` now ends by asking the running
# database whether it has what this release ships (root ADR-0071). A fixture holding one text
# file is not a release, and rehearsing against one only proves the parts that were already
# there. These carry what a real archive carries, plus a stand-in for the two things a
# rehearsal cannot have: a compose wrapper and a database.
build_source() {
  local dir="$1" label="$2" migrations="$3" version
  mkdir -p "${dir}/scripts/deploy" "${dir}/migrations"
  printf '%s\n' "${label}" >"${dir}/version.txt"
  cp "${repo_root}/scripts/deploy/assert-runtime-migrations.sh" "${dir}/scripts/deploy/"
  cp "${repo_root}/scripts/deploy/assert-runtime-environment.sh" "${dir}/scripts/deploy/"
  chmod +x "${dir}/scripts/deploy/assert-runtime-migrations.sh"
  chmod +x "${dir}/scripts/deploy/assert-runtime-environment.sh"
  # `verify` asks the environment file too, so the rehearsal carries the compose files that
  # check reads and an environment holding every variable they declare required. Neither list is
  # typed here — not the variables, and not the files. Which files the runtime loads is the
  # wrapper's `-f` arguments, so they are read from there; naming them here would go stale the
  # next time a compose file joins the runtime, and the rehearsal would keep passing while
  # covering a set it no longer covers.
  local -a compose_files=()
  local file
  while read -r file; do
    [[ -n "${file}" ]] && compose_files+=("${file}")
  done < <(
    sed -nE 's|^[[:space:]]*-f "\$\{root_dir\}/([^"]+)".*|\1|p' \
      "${repo_root}/scripts/deploy/foundation-runtime.sh"
  )
  [[ "${#compose_files[@]}" -gt 0 ]] || {
    printf 'foundation-release-test: read no compose files out of the runtime wrapper\n' >&2
    return 1
  }
  for file in "${compose_files[@]}"; do
    cp "${repo_root}/${file}" "${dir}/${file}"
  done
  grep -ohE '[$][{][A-Z_][A-Z0-9_]*:[?]' "${compose_files[@]/#/${dir}/}" \
    | tr -d '${:?' | sort -u | sed 's/$/=rehearsal/' >"${dir}/rehearsal.env"
  for version in ${migrations}; do
    printf -- '-- rehearsal\n' >"${dir}/migrations/${version}_rehearsal.sql"
  done
  # Stands in for the compose wrapper: the rehearsal has no runtime to ask. It carries the same
  # `-f` arguments as the real one, because the environment check reads that list out of the
  # wrapper rather than holding a copy of it. A stub without them made the check refuse — which
  # is the check behaving correctly, on a rehearsal that was not shaped like a release.
  {
    printf '#!/usr/bin/env bash\n'
    for file in "${compose_files[@]}"; do
      printf -- '  -f "${root_dir}/%s"\n' "${file}"
    done
    printf "printf 'rehearsal-container\\\\n'\n"
  } >"${dir}/scripts/deploy/foundation-runtime.sh"
  chmod +x "${dir}/scripts/deploy/foundation-runtime.sh"
}

# A `docker` that reports what the rehearsal decided the database holds.
mkdir -p "${test_root}/bin"
cat >"${test_root}/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
if [[ "${1:-}" == "exec" ]]; then
  cat "${REHEARSAL_APPLIED_FILE}"
  exit 0
fi
exit 1
DOCKER
chmod +x "${test_root}/bin/docker"
printf '20260719000001\n20260719000002\n' >"${test_root}/applied.txt"
export REHEARSAL_APPLIED_FILE="${test_root}/applied.txt"
export PATH="${test_root}/bin:${PATH}"

build_source "${test_root}/source-a" release-a "20260719000001 20260719000002"
build_source "${test_root}/source-b" release-b "20260719000001 20260719000002"
# The release the running database is not ready for.
build_source "${test_root}/source-ahead" release-ahead \
  "20260719000001 20260719000002 20260901000001"
tar -C "${test_root}/source-a" -czf "${test_root}/release-a.tar.gz" .
tar -C "${test_root}/source-b" -czf "${test_root}/release-b.tar.gz" .
tar -C "${test_root}/source-ahead" -czf "${test_root}/release-ahead.tar.gz" .

run_release() {
  FOUNDATION_PLATFORM_RELEASE_ROOT="${release_root}" \
  FOUNDATION_PLATFORM_STATE_ROOT="${state_root}" \
  FOUNDATION_PLATFORM_ENV_FILE="${release_root}/current/rehearsal.env" \
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

# A release the running database is not ready for must not be reported as installed, and the
# release must still be on disk afterwards: the check refuses to call the deploy finished, it
# does not undo it (root ADR-0071).
release_ahead="3333333333333333333333333333333333333333"
if run_release install "${release_ahead}" "${test_root}/release-ahead.tar.gz"; then
  printf 'a deploy that left the schema behind reported success\n' >&2
  exit 1
fi
assert_link "${release_root}/current" "releases/${release_ahead}"
[[ "$(cat "${release_root}/current/version.txt")" == "release-ahead" ]]

# And once the database has it, the same release installs clean.
printf '20260719000001\n20260719000002\n20260901000001\n' >"${test_root}/applied.txt"
run_release install "${release_ahead}" "${test_root}/release-ahead.tar.gz"
assert_link "${release_root}/current" "releases/${release_ahead}"

printf 'foundation-release-test=pass\n'
