#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
test_root="$(mktemp -d)"
trap 'chmod -R u+w "${test_root}"; rm -rf "${test_root}"' EXIT

release_root="${test_root}/opt/foundation-platform"
state_root="${test_root}/var/lib/foundation-platform"
canonical="${test_root}/canonical"
mkdir -p "${canonical}/platforms/foundation-platform"
git -C "${canonical}" init -q -b main
git -C "${canonical}" config user.name 'Release rehearsal'
git -C "${canonical}" config user.email 'release-rehearsal@example.invalid'

# Substitute only the privileged host/network connector, in a test copy of the installer.
# Production has no verifier-path or repository override. The real admission implementation
# still compares real Git ancestry and every file; this is not a marker-accepting stub.
release_script="${test_root}/foundation-release.sh"
sed "s|^admission=.*|admission=\"${test_root}/admission\"|" \
  "${repo_root}/scripts/deploy/foundation-release.sh" >"${release_script}"
chmod +x "${release_script}"
cat >"${test_root}/admission" <<'ADMISSION'
#!/usr/bin/env bash
exec python3 - "$@" <<'PY'
import importlib.util, os, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("admission", os.environ["REHEARSAL_ADMISSION_SOURCE"])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
def git(*args):
    return subprocess.check_output(["git", "-C", os.environ["REHEARSAL_CANONICAL"], *args])
try:
    if sys.argv[1] == "prepare":
        sha, archive, target = sys.argv[2:]
        module.prepare_release(module.release_files(git, sha), sha, pathlib.Path(archive), pathlib.Path(target))
    elif sys.argv[1] in ("verify", "build"):
        target = pathlib.Path(sys.argv[2])
        module.verify_release(module.release_files(git, target.name), target.name, target, os.getuid())
        artifacts = target.parent.parent / "artifacts" / target.name
        if sys.argv[1] == "build" and not artifacts.exists():
            artifacts.mkdir(parents=True)
            (artifacts / "jars").mkdir()
            (artifacts / "jars/fixture.jar").write_bytes(b"credential-free fixture dependency")
            (artifacts / "foundation-outbox-publisher").write_bytes(b"credential-free fixture binary")
            module.seal_artifacts(target.name, artifacts, "sha256:" + "a" * 64)
        module.verify_artifacts(target.name, artifacts, os.getuid())
    else:
        raise ValueError("unknown rehearsal admission command")
except (ValueError, OSError, subprocess.CalledProcessError) as error:
    print(str(error), file=sys.stderr)
    sys.exit(65)
PY
ADMISSION
chmod +x "${test_root}/admission"
export REHEARSAL_ADMISSION_SOURCE="${repo_root}/../../scripts/deploy/foundation-release-admission.py"
export REHEARSAL_CANONICAL="${canonical}"

register_release() {
  local source="$1" target="${canonical}/platforms/foundation-platform"
  rm -rf "${target}"
  mkdir -p "${target}"
  cp -r "${source}/." "${target}/"
  git -C "${canonical}" add -A
  git -C "${canonical}" commit -qm 'Canonical rehearsal release'
  git -C "${canonical}" rev-parse HEAD
}

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
release_a="$(register_release "${test_root}/source-a")"
release_b="$(register_release "${test_root}/source-b")"
release_ahead="$(register_release "${test_root}/source-ahead")"
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
[[ "$(stat -c '%a' "${release_root}/releases/${release_a}")" == "555" ]]
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

# A valid archive large enough that tar is still listing entries after the release marker has
# matched. The gate's first version piped `tar -tzf` into `grep -q`; grep exited on its hit,
# tar died of SIGPIPE, and under pipefail the gate refused every real deployment while passing
# this rehearsal's single-file fixtures (2026-09-03). Thousands of entries sorting after
# `scripts/` reproduce that timing; a fixture too small to trigger it proves nothing.
current_before_large="$(readlink "${release_root}/current")"
mkdir -p "${test_root}/source-large/src"
cp -r "${test_root}/source-a/." "${test_root}/source-large/"
for i in $(seq 1 3000); do
  : >"${test_root}/source-large/src/zz-padding-${i}.txt"
done
tar -C "${test_root}/source-large" -czf "${test_root}/release-large.tar.gz" .
release_large="$(register_release "${test_root}/source-large")"
run_release install "${release_large}" \
  "${test_root}/release-large.tar.gz" || {
  printf 'a valid release archive with many entries after the marker was refused\n' >&2
  exit 1
}
run_release activate "$(basename "${current_before_large}")"

# An archive of the monorepo root, not the release subtree. On 2026-09-03 one of these
# installed, activated, and only then failed the schema check — sealing its release id to the
# wrong bytes. It must be refused before anything is staged, and `current` must not move.
current_before_monorepo="$(readlink "${release_root}/current")"
mkdir -p "${test_root}/source-monorepo/platforms/foundation-platform"
cp -r "${test_root}/source-a/." "${test_root}/source-monorepo/platforms/foundation-platform/"
tar -C "${test_root}/source-monorepo" -czf "${test_root}/release-monorepo.tar.gz" .
if run_release install "3333333333333333333333333333333333333333" \
  "${test_root}/release-monorepo.tar.gz"; then
  printf 'a monorepo-rooted archive was accepted as a release\n' >&2
  exit 1
fi
assert_link "${release_root}/current" "${current_before_monorepo}"
if [[ -e "${release_root}/releases/3333333333333333333333333333333333333333" ]]; then
  printf 'a refused monorepo archive still left a release directory behind\n' >&2
  exit 1
fi

# A release the running database is not ready for must not be reported as installed, and the
# release must still be on disk afterwards: the check refuses to call the deploy finished, it
# does not undo it (root ADR-0071).
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

# Neither a caller's real-but-unmerged SHA nor an already installed release's markers are
# authority. Activation/rollback must read the source bytes again, not a prior green flag.
git -C "${canonical}" checkout -qb private-feature
printf 'private\n' >"${canonical}/platforms/foundation-platform/version.txt"
git -C "${canonical}" commit -qam 'Unmerged rehearsal'
unmerged="$(git -C "${canonical}" rev-parse HEAD)"
git -C "${canonical}" archive --format=tar.gz -o "${test_root}/unmerged.tar.gz" \
  "${unmerged}:platforms/foundation-platform"
git -C "${canonical}" checkout -q main
if run_release install "${unmerged}" "${test_root}/unmerged.tar.gz"; then
  printf 'unmerged source was accepted\n' >&2
  exit 1
fi
assert_link "${release_root}/current" "releases/${release_ahead}"

tampered="${release_root}/releases/${release_b}/version.txt"
chmod u+w "${tampered}"
if run_release activate "${release_b}"; then
  printf 'a writable release was activated\n' >&2
  exit 1
fi
printf 'private\n' >"${tampered}"
chmod 0444 "${tampered}"
if run_release activate "${release_b}"; then
  printf 'a tampered release was activated\n' >&2
  exit 1
fi
ln -sfn "releases/${release_b}" "${release_root}/previous"
if run_release rollback; then
  printf 'a tampered release was rolled back to\n' >&2
  exit 1
fi
assert_link "${release_root}/current" "releases/${release_ahead}"

# A modified active tree cannot install host units. The stand-in records any attempted host
# mutation, so "failed later because the test user cannot write /etc" cannot masquerade as a gate.
export REHEARSAL_INSTALL_CALLED="${test_root}/install-called"
cat >"${test_root}/bin/install" <<'INSTALL'
#!/usr/bin/env bash
: >"${REHEARSAL_INSTALL_CALLED}"
exit 1
INSTALL
chmod +x "${test_root}/bin/install"
ln -sfn "releases/${release_b}" "${release_root}/current"
for command in timers migrate; do
  if run_release "${command}" >"${test_root}/${command}.log" 2>&1; then
    printf 'tampered current release entered %s\n' "${command}" >&2
    exit 1
  fi
  [[ ! -e "${REHEARSAL_INSTALL_CALLED}" ]]
  grep -q 'installed release contents differ' "${test_root}/${command}.log"
done

printf 'foundation-release-test=pass\n'
