#!/usr/bin/env bash
set -Eeuo pipefail

# Ordinary CI can run as root without the production service account. Keep its rehearsal
# unprivileged; only the explicit disposable-container state rehearsal creates that account.
if [[ "$(id -u)" == 0 && "${FOUNDATION_RELEASE_STATE_REHEARSAL:-}" != 1 ]]; then
  exec python3 - "${BASH_SOURCE[0]}" "$@" <<'PY'
import subprocess, sys
sys.exit(subprocess.run(['bash', *sys.argv[1:]], user=65534, group=65534,
                        extra_groups=[]).returncode)
PY
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
umask 022
test_root="$(mktemp -d)"
# Git must not read the invoking account's configuration (CI runs this as nobody under root's HOME).
export HOME="${test_root}"
# Admitted releases and artifacts are read-only; the owner can still lift that to clean up.
# A stand-in build still holding the release build lock is stopped with it.
trap '[[ -z "${locker:-}" ]] || kill "${locker}" 2>/dev/null; chmod -R u+w "${test_root}"; rm -rf "${test_root}"' EXIT

release_root="${test_root}/opt/foundation-platform"
state_root="${test_root}/var/lib/foundation-platform"

# Canonical main is a real Git repository here. Every release id below is a real commit on its
# `main`, and admission compares the real ancestry and every archive byte (root ADR-0134).
canonical="${test_root}/canonical"
mkdir -p "${canonical}/platforms/foundation-platform"
git -C "${canonical}" init -q -b main
git -C "${canonical}" config user.name 'Release rehearsal'
git -C "${canonical}" config user.email 'release-rehearsal@example.invalid'

# Substitute only the privileged host/network connector, in a test copy of the installer.
# Production has no verifier-path or repository override. The real admission implementation
# still compares real Git ancestry and every file; this is not a marker-accepting stub.
release_script="${test_root}/foundation-release.sh"
retention_contract="${test_root}/release-retention.contract.json"
control_releases="${test_root}/opt/perfectory-control/releases"
sed -e "s|^admission=.*|admission=\"${test_root}/admission\"|" \
  -e "s|^retention_contract=.*|retention_contract=\"${retention_contract}\"|" \
  -e "s|^control_releases_dir=.*|control_releases_dir=\"${control_releases}\"|" \
  "${repo_root}/scripts/deploy/foundation-release.sh" >"${release_script}"
# Every activation ends with `prune`. The release flows below install fewer releases than this
# keeps, so prune removes nothing there; its own section further down plants what it must remove.
write_retention_contract() {
  printf '{"emergency_release":"%s","keep_newest":%s}\n' "$1" "$2" >"${retention_contract}"
}
write_retention_contract "$(printf 'e%039d' 0)" 20
chmod +x "${release_script}"
# `prune` imports this file for admission's release build lock, so it is a module as well as a
# command: the lock code is admission's own, only its path is moved to where the rehearsal can write.
cat >"${test_root}/admission" <<'ADMISSION'
#!/usr/bin/env python3
import importlib.util, os, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location("admission", os.environ["REHEARSAL_ADMISSION_SOURCE"])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.BUILD_LOCK = pathlib.Path(os.environ["REHEARSAL_BUILD_LOCK"])
release_build_lock = module.release_build_lock
def git(*args):
    return subprocess.check_output(["git", "-C", os.environ["REHEARSAL_CANONICAL"], *args])
def main():
    if sys.argv[1] == "prepare":
        sha, archive, target = sys.argv[2:]
        module.check_control_commit(git, sha, pathlib.Path(os.environ["REHEARSAL_CONTROL_ROOT"]))
        module.prepare_release(module.release_files(git, sha), sha, pathlib.Path(archive), pathlib.Path(target))
    elif sys.argv[1] in ("verify", "build", "verify-current"):
        # verify-current resolves `current` under the rehearsal's release root; the checks are real.
        target = (pathlib.Path(os.path.realpath(pathlib.Path(os.environ["FOUNDATION_PLATFORM_RELEASE_ROOT"]) / "current"))
                  if sys.argv[1] == "verify-current" else pathlib.Path(sys.argv[2]))
        module.verify_release(module.release_files(git, target.name), target.name, target, os.getuid())
        artifacts = target.parent.parent / "artifacts" / target.name
        if sys.argv[1] == "build" and not artifacts.exists():
            # Stands in for the Buildx/Spark build only; sealing and verification are the real code.
            artifacts.mkdir(parents=True)
            (artifacts / "jars").mkdir()
            (artifacts / "jars/fixture.jar").write_bytes(b"credential-free fixture dependency")
            (artifacts / "foundation-outbox-publisher").write_bytes(b"credential-free fixture binary")
            module.seal_artifacts(target.name, artifacts, {"publisher": "sha256:" + "a" * 64, "tippecanoe": "sha256:" + "d" * 64})
        module.verify_artifacts(target.name, artifacts, os.getuid())
    else:
        raise ValueError("unknown rehearsal admission command")
if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print("admission refused: " + str(error), file=sys.stderr)
        sys.exit(65)
ADMISSION
chmod +x "${test_root}/admission"
export REHEARSAL_ADMISSION_SOURCE="${repo_root}/../../scripts/deploy/foundation-release-admission.py"
export REHEARSAL_BUILD_LOCK="${test_root}/release-build.lock"
export REHEARSAL_CANONICAL="${canonical}"
# The control checkout records the canonical commit it was installed from; the first commit on
# main is an ancestor of every rehearsal release.
export REHEARSAL_CONTROL_ROOT="${test_root}/control"
mkdir -p "${REHEARSAL_CONTROL_ROOT}"

# Commits a source tree as the next canonical main and prints its id.
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
  mkdir -p "${dir}/infra/systemd"
  cp "${repo_root}/infra/systemd/building-register-floor.env.example" "${dir}/infra/systemd/"
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
# It also answers the few calls `prune` makes. Running containers are lines
# `<id> /<name> <image id> <image ref> <mount source>...` in REHEARSAL_CONTAINERS; images are files
# named `<repository>:<tag>` holding `<id> <size>` in REHEARSAL_IMAGES.
cat >"${test_root}/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
case "$*" in
  exec*) cat "${REHEARSAL_APPLIED_FILE}" ;;
  "ps -q --no-trunc") cut -d' ' -f1 "${REHEARSAL_CONTAINERS}" ;;
  "inspect --format "*)
    shift 3
    for id in "$@"; do grep "^${id} " "${REHEARSAL_CONTAINERS}" | cut -d' ' -f2-; done ;;
  "image inspect --format "*)
    if [[ -f "${REHEARSAL_IMAGES}/$5" ]]; then
      cat "${REHEARSAL_IMAGES}/$5"
    else
      # By ID: one answer however many tags share it, as Docker gives.
      awk -v id="$5" '$1 == id { print; found = 1; exit } END { exit !found }' "${REHEARSAL_IMAGES}"/*
    fi ;;
  "image rm "*) rm "${REHEARSAL_IMAGES}/$3" 2>/dev/null || { echo "No such image: $3" >&2; exit 1; } ;;
  *) exit 1 ;;
esac
DOCKER
chmod +x "${test_root}/bin/docker"
export REHEARSAL_CONTAINERS="${test_root}/containers.txt" REHEARSAL_IMAGES="${test_root}/images"
: >"${REHEARSAL_CONTAINERS}"
mkdir -p "${REHEARSAL_IMAGES}"
printf '20260719000001\n20260719000002\n' >"${test_root}/applied.txt"
export REHEARSAL_APPLIED_FILE="${test_root}/applied.txt"
export PATH="${test_root}/bin:${PATH}"


build_source "${test_root}/source-a" release-a "20260719000001 20260719000002"
build_source "${test_root}/source-b" release-b "20260719000001 20260719000002"
# The release the running database is not ready for.
build_source "${test_root}/source-ahead" release-ahead \
  "20260719000001 20260719000002 20260901000001"
build_source "${test_root}/source-prepared" release-prepared "20260719000001 20260719000002"
release_a="$(register_release "${test_root}/source-a")"
printf '%s\n' "${release_a}" >"${REHEARSAL_CONTROL_ROOT}/.perfectory-control-commit"
release_b="$(register_release "${test_root}/source-b")"
release_ahead="$(register_release "${test_root}/source-ahead")"
prepared="$(register_release "${test_root}/source-prepared")"
for name in a b ahead prepared; do
  tar -C "${test_root}/source-${name}" -czf "${test_root}/release-${name}.tar.gz" .
done

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

# Lifts the read-only bit only for the length of one planted change.
with_writable() {
  local path="$1"; shift
  local mode
  mode="$(stat -c '%a' "${path}")"
  chmod u+w "${path}"
  "$@"
  chmod "${mode}" "${path}"
}

run_release prepare "${release_a}" "${test_root}/release-a.tar.gz"
[[ ! -e "${release_root}/current" && ! -L "${release_root}/current" ]]
[[ ! -e "${release_root}/previous" && ! -L "${release_root}/previous" ]]
run_release install "${release_a}" "${test_root}/release-a.tar.gz"
assert_link "${release_root}/current" "releases/${release_a}"
[[ "$(cat "${release_root}/current/version.txt")" == "release-a" ]]
# Installed from canonical Git, read-only; the trusted build output sits outside it.
[[ "$(stat -c '%a' "${release_root}/releases/${release_a}")" == "555" ]]
[[ -x "${release_root}/artifacts/${release_a}/foundation-outbox-publisher" ]]
[[ ! -e "${release_root}/releases/${release_a}/bin" ]]
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
  printf '%s\n' "${i}" >"${test_root}/source-large/src/zz-padding-${i}.txt"
done
tar -C "${test_root}/source-large" -czf "${test_root}/release-large.tar.gz" .
release_large="$(register_release "${test_root}/source-large")"
run_release install "${release_large}" "${test_root}/release-large.tar.gz" || {
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

# --- Only canonical main installs (root ADR-0134 §1) -------------------------------------------
# A real commit that never reached main, with its own honest archive, is still refused.
git -C "${canonical}" checkout -qb private-feature
printf 'private\n' >"${canonical}/platforms/foundation-platform/version.txt"
git -C "${canonical}" commit -qam 'Unmerged rehearsal'
unmerged="$(git -C "${canonical}" rev-parse HEAD)"
git -C "${canonical}" archive --format=tar.gz -o "${test_root}/unmerged.tar.gz" \
  "${unmerged}:platforms/foundation-platform"
git -C "${canonical}" checkout -q main
if run_release install "${unmerged}" "${test_root}/unmerged.tar.gz" 2>"${test_root}/unmerged.log"; then
  printf 'unmerged source was accepted\n' >&2
  exit 1
fi
grep -q 'not in independently fetched canonical main' "${test_root}/unmerged.log"
[[ ! -e "${release_root}/releases/${unmerged}" ]]
[[ ! -e "${release_root}/artifacts/${unmerged}" ]]
assert_link "${release_root}/current" "releases/${release_ahead}"
printf 'refused-unmerged-sha=pass\n'

# A control checkout from another line of history cannot admit anything.
printf '%s\n' "${unmerged}" >"${REHEARSAL_CONTROL_ROOT}/.perfectory-control-commit"
if run_release prepare "${prepared}" "${test_root}/release-prepared.tar.gz" 2>"${test_root}/control.log"; then
  printf 'a control checkout off canonical main admitted a release\n' >&2
  exit 1
fi
grep -q 'not an ancestor of release' "${test_root}/control.log"
# Nor can a control checkout newer than the release being installed.
printf '%s\n' "${release_large}" >"${REHEARSAL_CONTROL_ROOT}/.perfectory-control-commit"
if run_release prepare "${prepared}" "${test_root}/release-prepared.tar.gz" 2>"${test_root}/control.log"; then
  printf 'a release older than the control checkout was admitted\n' >&2
  exit 1
fi
grep -q 'not an ancestor of release' "${test_root}/control.log"
[[ ! -e "${release_root}/releases/${prepared}" ]]
printf '%s\n' "${release_a}" >"${REHEARSAL_CONTROL_ROOT}/.perfectory-control-commit"
printf 'refused-control-drift=pass\n'

# A merged id cannot label other bytes either.
if run_release prepare "${prepared}" "${test_root}/unmerged.tar.gz"; then
  printf 'a merged id was installed from unmerged bytes\n' >&2
  exit 1
fi
[[ ! -e "${release_root}/releases/${prepared}" ]]

# --- An installed release holds exactly its canonical files (root ADR-0134 §2) ---------------
# The layout production had before this change — the publisher in bin/ and FLOOR's config in
# .foundation-floor.env, both inside releases/<sha> — is exactly an extra file now.
legacy="${release_root}/releases/${release_b}"
for extra in bin/foundation-outbox-publisher .foundation-floor.env; do
  plant_extra() {
    mkdir -p "$(dirname "${legacy}/${extra}")"
    printf 'planted\n' >"${legacy}/${extra}"
    chmod 0444 "${legacy}/${extra}"
    [[ "${extra}" != bin/* ]] || chmod 0555 "${legacy}/bin"
  }
  with_writable "${legacy}" plant_extra
  if run_release activate "${release_b}" 2>"${test_root}/extra.log"; then
    printf 'a release with an extra file was activated: %s\n' "${extra}" >&2
    exit 1
  fi
  grep -q 'file set differs' "${test_root}/extra.log"
  assert_link "${release_root}/current" "releases/${release_ahead}"
  [[ ! -d "${legacy}/bin" ]] || chmod u+w "${legacy}/bin"
  with_writable "${legacy}" rm -rf "${legacy:?}/bin" "${legacy}/.foundation-floor.env"
done
run_release activate "${release_b}"
run_release activate "${release_ahead}"
printf 'refused-extra-file=pass\n'

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
chmod u+w "${tampered}"
printf 'release-b\n' >"${tampered}"
chmod 0444 "${tampered}"
run_release activate "${release_b}"
run_release activate "${release_ahead}"

# --- The trusted build output is bound by its sha256 (root ADR-0134 §3) ------------------------
publisher="${release_root}/artifacts/${release_b}/foundation-outbox-publisher"
cp "${publisher}" "${test_root}/publisher.saved"
with_writable "${publisher}" sh -c 'printf "#!/bin/sh\nexit 0\n" >"$1"' _ "${publisher}"
if run_release activate "${release_b}" 2>"${test_root}/artifact.log"; then
  printf 'a publisher with the wrong sha256 was activated\n' >&2
  exit 1
fi
grep -q 'artifact contents differ' "${test_root}/artifact.log"
assert_link "${release_root}/current" "releases/${release_ahead}"
with_writable "${publisher}" cp "${test_root}/publisher.saved" "${publisher}"
run_release activate "${release_b}"
run_release activate "${release_ahead}"
printf 'refused-wrong-publisher-sha256=pass\n'

# --- prepare stages a release without activating it ------------------------------------------
current_before_prepare="$(readlink "${release_root}/current")"
previous_before_prepare="$(readlink "${release_root}/previous")"
assert_inactive() {
  assert_link "${release_root}/current" "${current_before_prepare}"
  assert_link "${release_root}/previous" "${previous_before_prepare}"
}
refuse() {
  if run_release "$@"; then
    printf 'unexpected acceptance: %s\n' "$1" >&2
    exit 1
  fi
  assert_inactive
}
run_release prepare "${prepared}" "${test_root}/release-prepared.tar.gz"
assert_inactive
[[ -d "${release_root}/artifacts/${prepared}" ]]
run_release prepare "${prepared}" "${test_root}/release-prepared.tar.gz"
assert_inactive
refuse prepare "${prepared}" "${test_root}/release-b.tar.gz"
refuse prepare invalid "${test_root}/release-a.tar.gz"
# There is no command that accepts a caller's publisher binary.
refuse publisher "${prepared}" "${test_root}/publisher.saved" "$(printf '0%.0s' {1..64})"

# --- FLOOR's configuration lives outside the release ------------------------------------------
config_source="${test_root}/floor.env"
config_target="${release_root}/config/${prepared}/building-register-floor.env"
history_witness="${test_root}/floor-history.json"
printf '{}\n' >"${history_witness}"
chmod 0444 "${history_witness}"
if [[ "$(id -u)" == 0 ]]; then
  if ! id foundation-platform >/dev/null 2>&1; then
    [[ "${FOUNDATION_RELEASE_STATE_REHEARSAL:-}" == 1 && -f /.dockerenv ]]
    useradd --system foundation-platform
  fi
  service_uid="$(id -u foundation-platform)"
  service_gid="$(getent group foundation-platform | cut -d: -f3)"
  chmod 0711 "${test_root}"
  chown "root:${service_gid}" "${history_witness}"
  chmod 0440 "${history_witness}"
fi
python3 - "${release_root}/releases/${prepared}" "${history_witness}" >"${config_source}" <<'PY'
import pathlib, re, sys
release = pathlib.Path(sys.argv[1])
for line in (release / 'infra/systemd/building-register-floor.env.example').read_text().splitlines():
    if not line.strip() or line.lstrip().startswith('#'):
        continue
    name = line.split('=', 1)[0]
    assert re.fullmatch(r'[A-Z][A-Z0-9_]*', name)
    value = str(release) if name == 'FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT' else 'fixture'
    if name == 'FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE':
        # The image ID this release's build recorded (the rehearsal build's fixed ID).
        value = 'sha256:' + 'a' * 64
    if name == 'FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH':
        value = sys.argv[2]
    print(f'{name}={value}')
PY
release_listing_before="$(cd "${release_root}/releases/${prepared}" && find . | sort | sha256sum)"
run_release floor-config "${prepared}" "${config_source}"
assert_inactive
cmp "${config_source}" "${config_target}"
[[ "$(stat -c '%a' "${config_target}")" == 644 ]]
[[ "$(cd "${release_root}/releases/${prepared}" && find . | sort | sha256sum)" == "${release_listing_before}" ]]
[[ ! -e "${release_root}/releases/${prepared}/.foundation-floor.env" ]]
# The configured release is still admitted: nothing was written inside it.
run_release activate "${prepared}"
run_release activate "$(basename "${current_before_prepare}")"
ln -sfn "${previous_before_prepare}" "${release_root}/previous"
assert_inactive
config_before="$(stat -c '%i:%Y' "${config_target}")"
run_release floor-config "${prepared}" "${config_source}"
[[ "$(stat -c '%i:%Y' "${config_target}")" == "${config_before}" ]]
mv "${config_target}" "${config_target}.saved"
if [[ "$(id -u)" == 0 ]]; then
  chmod 0400 "${history_witness}"
  refuse floor-config "${prepared}" "${config_source}"
  chmod 0440 "${history_witness}"
  chown root:root "${history_witness}"
  refuse floor-config "${prepared}" "${config_source}"
  chown "root:${service_gid}" "${history_witness}"
  chmod 0700 "${test_root}"
  refuse floor-config "${prepared}" "${config_source}"
  chmod 0711 "${test_root}"
  [[ ! -e "${config_target}" ]]
  printf 'floor-witness-readability=pass\n'
fi
chmod 0644 "${history_witness}"
refuse floor-config "${prepared}" "${config_source}"
chmod 0444 "${history_witness}"
ln -s "${history_witness}" "${test_root}/history-link"
for invalid_history in relative/history "${test_root}/missing-history" "${test_root}/history-link" "${test_root}"; do
  sed "s|^FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=.*|FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=${invalid_history}|" \
    "${config_source}" >"${test_root}/invalid-history.env"
  refuse floor-config "${prepared}" "${test_root}/invalid-history.env"
done
# A canonical file of the release itself is still not an operator-held witness.
sed "s|^FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=.*|FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=${release_root}/releases/${prepared}/version.txt|" \
  "${config_source}" >"${test_root}/internal-history.env"
refuse floor-config "${prepared}" "${test_root}/internal-history.env"
cp "${config_source}" "${test_root}/secret.env"
printf 'DATABASE_URL=postgres://fixture\n' >>"${test_root}/secret.env"
refuse floor-config "${prepared}" "${test_root}/secret.env"
sed '/^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=/d' "${config_source}" >"${test_root}/missing.env"
refuse floor-config "${prepared}" "${test_root}/missing.env"
sed 's|^FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=.*|FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=${NETWORK}|' "${config_source}" >"${test_root}/interpolation.env"
refuse floor-config "${prepared}" "${test_root}/interpolation.env"
for invalid_value in '' '"quoted"' 'back\slash' 'two words'; do
  FIXTURE_VALUE="${invalid_value}" awk '
    /^FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=/ { print "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=" ENVIRON["FIXTURE_VALUE"]; next }
    { print }
  ' "${config_source}" >"${test_root}/unsafe.env"
  refuse floor-config "${prepared}" "${test_root}/unsafe.env"
done
# A well-formed, pinned digest that is not this release's build output (root ADR-0134 §3).
sed 's|^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=.*|FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=sha256:'"$(printf 'b%.0s' {1..64})"'|' \
  "${config_source}" >"${test_root}/foreign-image.env"
if run_release floor-config "${prepared}" "${test_root}/foreign-image.env" 2>"${test_root}/foreign-image.log"; then
  printf 'a FLOOR image other than the release build output was accepted\n' >&2
  exit 1
fi
grep -q 'must be publisher_image' "${test_root}/foreign-image.log"
assert_inactive
printf 'refused-foreign-floor-image=pass\n'
sed 's|^FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=.*|FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=/other/release|' "${config_source}" >"${test_root}/wrong-root.env"
refuse floor-config "${prepared}" "${test_root}/wrong-root.env"
cp "${config_source}" "${test_root}/duplicate.env"
head -1 "${config_source}" >>"${test_root}/duplicate.env"
refuse floor-config "${prepared}" "${test_root}/duplicate.env"
ln -s "${config_source}" "${test_root}/config-link"
refuse floor-config "${prepared}" "${test_root}/config-link"
[[ ! -e "${config_target}" && ! -L "${config_target}" ]]
ln -s "${config_target}.saved" "${config_target}"
refuse floor-config "${prepared}" "${config_source}"
rm "${config_target}"
mv "${config_target}.saved" "${config_target}"
mkdir "${test_root}/outside"
mv "${release_root}/config/${prepared}" "${test_root}/outside/${prepared}"
ln -s "${test_root}/outside/${prepared}" "${release_root}/config/${prepared}"
refuse floor-config "${prepared}" "${config_source}"
rm "${release_root}/config/${prepared}"
mv "${test_root}/outside/${prepared}" "${release_root}/config/${prepared}"
sed 's|^FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=.*|FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK=changed|' "${config_source}" >"${test_root}/conflict.env"
refuse floor-config "${prepared}" "${test_root}/conflict.env"
cmp "${config_source}" "${config_target}"
assert_inactive
if [[ "${FOUNDATION_RELEASE_STATE_REHEARSAL:-}" == 1 ]]; then
  # Only a disposable root container may exercise real host namespaces.
  [[ -f /.dockerenv && "$(id -u)" == 0 ]]
  run_release activate "${prepared}"
  state_namespace=/data/foundation-platform/building-register-floor
  dropin=/etc/systemd/system/foundation-building-register-floor.service.d/10-state.conf
  configure_state() {
    python3 - "${config_source}" "${config_target}" "$1" "$2" <<'PY'
import pathlib, sys
source, target, state, ivy = sys.argv[1:]
values = dict(line.split('=', 1) for line in pathlib.Path(source).read_text().splitlines())
values['FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT'] = state
values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'] = ivy
pathlib.Path(target).write_text(''.join(f'{k}={v}\n' for k, v in values.items()))
PY
  }
  prepare_state() {
    FOUNDATION_PLATFORM_RELEASE_ROOT="${release_root}" bash -c \
      'source <(sed "/^command=/,\$d" "$1"); prepare_floor_state' _ "${release_script}"
  }
  configure_state "${state_namespace}" "${state_namespace}/ivy"
  chown "${service_uid}:${service_gid}" "${history_witness}"
  if prepare_state; then echo 'service-owned history witness accepted' >&2; exit 1; fi
  [[ ! -e "${dropin}" && ! -e "${state_namespace}" ]]
  chown "root:${service_gid}" "${history_witness}"
  chmod 0440 "${history_witness}"
  mkdir -p /var/lib/foundation-platform
  ln -s /tmp /var/lib/foundation-platform/building-register-floor
  if prepare_state; then echo 'unselected sandbox root symlink accepted' >&2; exit 1; fi
  [[ ! -e "${dropin}" ]]
  rm /var/lib/foundation-platform/building-register-floor
  prepare_state
  [[ "$(stat -c '%u:%g:%a' "${state_namespace}")" == "${service_uid}:${service_gid}:2770" ]]
  [[ "$(stat -c '%u:%g:%a' "${state_namespace}/ivy")" == "${service_uid}:${service_gid}:2770" ]]
  [[ "$(stat -c '%u:%g:%a' "${dropin}")" == 0:0:644 ]]
  policy='ReadWritePaths=-/var/lib/foundation-platform/building-register-floor -/data/foundation-platform/building-register-floor'
  grep -Fx "${policy}" "${dropin}"
  dropin_before="$(stat -c '%i:%Y' "${dropin}")"
  touch "${state_namespace}/spark-output"
  chown "185:${service_gid}" "${state_namespace}/spark-output"
  prepare_state
  [[ "$(stat -c '%i:%Y' "${dropin}")" == "${dropin_before}" ]]
  [[ "$(stat -c '%u:%g' "${state_namespace}/spark-output")" == "185:${service_gid}" ]]
  dropin_sha="$(sha256sum "${dropin}")"
  for bad_state in /data /data/foundation-platform /tmp/floor "${state_namespace}/nested"; do
    configure_state "${bad_state}" "${bad_state}/ivy"
    # `timers` installs the FLOOR unit only when this succeeds, and fails after installing the rest.
    if prepare_state; then echo 'unsafe state accepted' >&2; exit 1; fi
    [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  done
  configure_state "${state_namespace}" "${state_namespace}"
  if prepare_state; then echo 'same Ivy accepted' >&2; exit 1; fi
  configure_state "${state_namespace}" "${state_namespace}/../foreign"
  if prepare_state; then echo 'traversal Ivy accepted' >&2; exit 1; fi
  configure_state "${state_namespace}" "${state_namespace}/linked/ivy"
  ln -s /tmp "${state_namespace}/linked"
  if prepare_state; then echo 'symlink ancestor accepted' >&2; exit 1; fi
  rm "${state_namespace}/linked"
  configure_state "${state_namespace}" "${state_namespace}/ivy"
  chown root:root "${state_namespace}/ivy"
  if prepare_state; then echo 'foreign Ivy ownership accepted' >&2; exit 1; fi
  [[ "$(stat -c '%u:%g' "${state_namespace}/ivy")" == 0:0 ]]
  [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  chown "${service_uid}:${service_gid}" "${state_namespace}/ivy"
  configure_state /var/lib/foundation-platform/building-register-floor /var/lib/foundation-platform/building-register-floor/ivy
  prepare_state
  grep -Fx "${policy}" "${dropin}"
  [[ "$(stat -c '%i:%Y' "${dropin}")" == "${dropin_before}" ]]
  mkdir -p "${release_root}/config/${release_a}"
  python3 - "${config_target}" "${release_root}/releases/${prepared}" "${release_root}/releases/${release_a}" \
    "${release_root}/config/${release_a}/building-register-floor.env" <<'PY'
import pathlib, sys
source, prepared, release, target = map(pathlib.Path, sys.argv[1:])
body = source.read_text().replace(str(prepared), str(release))
body = body.replace('/var/lib/foundation-platform/building-register-floor',
                    '/data/foundation-platform/building-register-floor')
target.write_text(body)
PY
  run_release activate "${release_a}"
  assert_link "${release_root}/current" "releases/${release_a}"
  [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  run_release rollback
  assert_link "${release_root}/current" "releases/${prepared}"
  [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  # Execute the actual embedded validator with an OS-call interposer, no production hook.
  python3 - "${release_script}" "${config_target}" "${release_root}/releases/${prepared}" <<'PY'
import os, pathlib, stat, sys, tempfile
script, config, release = map(pathlib.Path, sys.argv[1:])
code = 'import hashlib' + script.read_text().split("<<'PY'\nimport hashlib", 1)[1].split('\nPY', 1)[0]
values = dict(line.split('=', 1) for line in config.read_text().splitlines())
ivy = pathlib.Path(values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'])
victim = pathlib.Path(tempfile.mkdtemp())
os.chmod(victim, 0o711)
before = victim.stat()
real_open, real_mkdir = os.open, os.mkdir
sys.argv = ['validator', str(config), str(release), 'state']
held = ivy.with_name('held-ivy')
os.chmod(ivy, 0o750)
triggered = False
def swap_after_open(path, flags, *args, **kwargs):
    global triggered
    if path == 'etc' and flags & os.O_DIRECTORY and not triggered:
        triggered = True
        ivy.rename(held)
        ivy.symlink_to(victim, target_is_directory=True)
    return real_open(path, flags, *args, **kwargs)
os.open = swap_after_open
try:
    try:
        exec(compile(code, str(script), 'exec'), {})
    except SystemExit as error:
        assert error.code == 0
    assert triggered
    assert stat.S_IMODE(held.stat().st_mode) == 0o2770
finally:
    os.open = real_open
    ivy.unlink()
    held.rename(ivy)
after = victim.stat()
assert (before.st_uid, before.st_gid, before.st_mode) == (after.st_uid, after.st_gid, after.st_mode)

# Replace a just-created directory before its open: O_NOFOLLOW must reject it.
new = ivy.with_name('new-ivy')
values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'] = str(new)
original = config.read_bytes()
config.write_text(''.join(f'{k}={v}\n' for k, v in values.items()))
triggered = False
def swap_after_mkdir(path, mode=0o777, *, dir_fd=None):
    global triggered
    real_mkdir(path, mode, dir_fd=dir_fd)
    if path == new.name and dir_fd is not None:
        triggered = True
        os.rename(path, 'new-held', src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.symlink(str(victim), path, dir_fd=dir_fd)
os.mkdir = swap_after_mkdir
try:
    try:
        exec(compile(code, str(script), 'exec'), {})
    except OSError:
        pass
    else:
        raise AssertionError('replacement after mkdir was accepted')
    assert triggered
finally:
    os.mkdir = real_mkdir
    config.write_bytes(original)
after = victim.stat()
assert (before.st_uid, before.st_gid, before.st_mode) == (after.st_uid, after.st_gid, after.st_mode)
print('floor-state-symlink-race=pass')
PY
  printf 'floor-state-rehearsal=pass\n'
fi

# --- A release cannot silently forget a running capability (root ADR-0132) --------------------
# Every candidate is a canonical commit: an installed release can no longer be edited into one.
guard_source="${test_root}/source-guard"
build_source "${guard_source}" release-guard "20260719000001 20260719000002 20260901000001"
mkdir -p "${guard_source}/orchestration"
printf '%s\n' '{"jobs":[{"id":"fixture_job","enabled":true}]}' >"${guard_source}/orchestration/jobs.v1.json"
guard_current="$(register_release "${guard_source}")"
tar -C "${guard_source}" -czf "${test_root}/guard-current.tar.gz" .
run_release install "${guard_current}" "${test_root}/guard-current.tar.gz"
previous_before_guard="$(readlink "${release_root}/previous")"
guard_candidate() {
  if [[ "$1" == missing ]]; then
    rm -f "${guard_source}/orchestration/jobs.v1.json"
    printf 'missing\n' >"${guard_source}/orchestration/reason.txt"
  else
    printf '%s\n' "$1" >"${guard_source}/orchestration/jobs.v1.json"
  fi
  candidate="$(register_release "${guard_source}")"
  tar -C "${guard_source}" -czf "${test_root}/guard-${candidate}.tar.gz" .
  run_release prepare "${candidate}" "${test_root}/guard-${candidate}.tar.gz"
  rm -f "${guard_source}/orchestration/reason.txt"
}
for content in missing '{"jobs":[]}' '{"jobs":[{"id":"different_job","enabled":true}]}' \
  '{"jobs":[{"id":"fixture_job","enabled":"false"}]}' \
  '{"jobs":[{"id":"fixture_job","enabled":true},{"id":"fixture_job","enabled":false}]}'; do
  guard_candidate "${content}"
  if run_release activate "${candidate}"; then
    echo 'activation silently removed an enabled job' >&2; exit 1
  fi
  assert_link "${release_root}/current" "releases/${guard_current}"
  assert_link "${release_root}/previous" "${previous_before_guard}"
done
guard_candidate '{"jobs":[{"id":"fixture_job","enabled":false},{"id":"new_only_job","enabled":true}]}'
run_release activate "${candidate}"
assert_link "${release_root}/current" "releases/${candidate}"
# Explicit rollback remains a recovery operation, not a new activation admission: the previous
# release lacks new_only_job, which is enabled now.
run_release rollback
assert_link "${release_root}/current" "releases/${guard_current}"

# --- Admission is installed on the unit systemd loads (root ADR-0134 §3) ----------------------
dropin_unit() {
  FOUNDATION_PLATFORM_RELEASE_ROOT="${release_root}" bash -c \
    'source <(sed "/^command=/,\$d" "$1"); admission_dropin_unit "$2"' _ "${release_script}" "$1"
}
[[ "$(dropin_unit foundation-map-edit-fold@complex.service)" == foundation-map-edit-fold@.service ]]
[[ "$(dropin_unit foundation-building-register-floor.service)" == foundation-building-register-floor.service ]]
for invalid_service in ssh.service 'foundation-x@../y.service' foundation-x.timer; do
  if dropin_unit "${invalid_service}" >/dev/null 2>&1; then
    printf 'admission drop-in accepted an invalid unit: %s\n' "${invalid_service}" >&2
    exit 1
  fi
done
printf 'admission-dropin-on-template=pass\n'

# --- sudo names only the control checkout's copy of this script (root ADR-0134 §2) -------------
release_functions() {
  FOUNDATION_PLATFORM_RELEASE_ROOT="${release_root}" bash -c \
    'source <(sed "/^command=/,\$d" "$1"); shift; "$@"' _ "${release_script}" "$@"
}
release_functions render_deployer_sudoers deployer >"${test_root}/sudoers-rendered"
[[ "$(grep -v '^#' "${test_root}/sudoers-rendered")" == \
  'deployer ALL=(root) NOPASSWD: /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh' ]]
if release_functions render_deployer_sudoers 'deployer ALL' >/dev/null 2>&1; then
  printf 'a sudoers rule was rendered for an injected account name\n' >&2
  exit 1
fi
mkdir -p "${test_root}/sudoers.d"
cp "${test_root}/sudoers-rendered" "${test_root}/sudoers.d/foundation-release"
release_functions find_other_release_grants "${test_root}/sudoers.d/"* >/dev/null
# The rule production carries today: it matches every directory under the release root.
printf 'deployer ALL=(root) NOPASSWD: /opt/foundation-platform/*/scripts/deploy/foundation-release.sh\n' \
  >"${test_root}/sudoers.d/legacy-deployer"
if release_functions find_other_release_grants "${test_root}/sudoers.d/"* >"${test_root}/grants.log"; then
  printf 'a wildcard grant over every release was not reported\n' >&2
  exit 1
fi
grep -q 'legacy-deployer' "${test_root}/grants.log"
# The order ADR-0134 §2 prescribes: installing the new rule beside the old one reports and
# succeeds; the final --exclusive step fails until the old line is gone.
release_functions deployer_grant_verdict deployer '' "${test_root}/sudoers.d/"* >"${test_root}/verdict.log"
grep -q '^deployer-access-installed' "${test_root}/verdict.log"
if release_functions deployer_grant_verdict deployer --exclusive "${test_root}/sudoers.d/"* >/dev/null 2>&1; then
  printf 'the exclusive step passed while the wildcard grant remained\n' >&2
  exit 1
fi
# A second path on the same line as the exact one is still a second grant.
rm "${test_root}/sudoers.d/legacy-deployer"
release_functions deployer_grant_verdict deployer --exclusive "${test_root}/sudoers.d/"* | grep -q '^deployer-access-ok'
printf 'deployer ALL=(root) NOPASSWD: %s, /opt/foundation-platform/releases/*/scripts/deploy/foundation-release.sh\n' \
  /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh \
  >"${test_root}/sudoers.d/combined"
if release_functions find_other_release_grants "${test_root}/sudoers.d/"* >/dev/null; then
  printf 'a wildcard grant sharing a line with the control path was not reported\n' >&2
  exit 1
fi
printf 'sudoers-control-path-only=pass\n'

# --- Retention: prune keeps what rollback and the emergency return need -------------------------
# A host of its own, planted the way production looks: read-only releases, artifacts and config
# per release, two image tags each. Ids are synthetic; `n` days old, so newest is smallest n.
prune_root="${test_root}/prune/opt/foundation-platform"
prune_id() { printf 'e%039d' "$1"; }
plant_release() {
  local id; id="$(prune_id "$1")"
  mkdir -p "${prune_root}/releases/${id}/scripts" "${prune_root}/artifacts/${id}" "${prune_root}/config/${id}"
  printf '%s\n' "${id}" >"${prune_root}/releases/${id}/.foundation-release-id"
  head -c 65536 /dev/zero >"${prune_root}/artifacts/${id}/foundation-outbox-publisher"
  printf '{"publisher_image":"sha256:p%s","tippecanoe_image":"sha256:t%s"}\n' "${id}" "${id}" \
    >"${prune_root}/artifacts/${id}/build.json"
  printf 'K=v\n' >"${prune_root}/config/${id}/building-register-floor.env"
  printf 'sha256:p%s 1000\n' "${id}" >"${REHEARSAL_IMAGES}/foundation-outbox-publisher:${id}"
  printf 'sha256:t%s 2000\n' "${id}" >"${REHEARSAL_IMAGES}/foundation-tippecanoe:${id}"
  chmod -R a-w "${prune_root}/releases/${id}" "${prune_root}/artifacts/${id}"
  touch -d "@$(( $(date +%s) - $1 * 86400 ))" \
    "${prune_root}/releases/${id}" "${prune_root}/artifacts/${id}" "${prune_root}/config/${id}"
}
plant_control() {
  local id; id="$(prune_id "$1")"
  mkdir -p "${control_releases}/${id}/tools"
  touch -d "@$(( $(date +%s) - $1 * 86400 ))" "${control_releases}/${id}"
}
gone() { [[ ! -e "${prune_root}/releases/$1" && ! -e "${prune_root}/artifacts/$1" &&
            ! -e "${prune_root}/config/$1" && ! -e "${REHEARSAL_IMAGES}/foundation-outbox-publisher:$1" &&
            ! -e "${REHEARSAL_IMAGES}/foundation-tippecanoe:$1" ]]; }
present() { [[ -d "${prune_root}/releases/$1" && -d "${prune_root}/artifacts/$1" &&
               -e "${REHEARSAL_IMAGES}/foundation-tippecanoe:$1" ]]; }
expect() { "$@" || { printf 'prune rehearsal: expected: %s\n' "$*" >&2; return 1; }; }

# Runs every retention case against one script; returns non-zero on the first broken expectation.
prune_rehearsal() {
  local script="$1" status sleeper n
  [[ ! -e "${prune_root}" ]] || { chmod -R u+w "${prune_root}"; rm -rf "${prune_root}"; }
  rm -rf "${control_releases%/releases}" "${REHEARSAL_IMAGES}"
  mkdir -p "${REHEARSAL_IMAGES}"
  : >"${REHEARSAL_CONTAINERS}"
  # 1 newest .. 10 oldest. current=5, previous=8, emergency=10 (the oldest), newest three 1 2 3.
  for n in 1 2 3 4 5 6 7 8 9 10; do plant_release "${n}"; done
  # An artifacts directory whose release directory is already gone is still a release to remove.
  chmod -R u+w "${prune_root}/releases/$(prune_id 9)"; rm -rf "${prune_root}/releases/$(prune_id 9)"
  ln -s "releases/$(prune_id 5)" "${prune_root}/current"
  ln -s "releases/$(prune_id 8)" "${prune_root}/previous"
  write_retention_contract "$(prune_id 10)" 3
  for n in 1 2 3 4 5; do plant_control "${n}"; done
  ln -s "releases/$(prune_id 4)" "${control_releases%/releases}/current"
  # Release 6 is held by a running process (a unit started from an older current), release 7 by a
  # running container through the image ID its build recorded.
  ( cd "${prune_root}/releases/$(prune_id 6)/scripts" && exec sleep 600 ) &
  sleeper=$!
  printf 'c7 /fixture-floor sha256:p%s sha256:p%s /srv/unrelated\n' "$(prune_id 7)" "$(prune_id 7)" \
    >"${REHEARSAL_CONTAINERS}"
  status=0
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune \
    >"${test_root}/prune.log" 2>&1 || status=$?
  kill "${sleeper}"; wait "${sleeper}" 2>/dev/null || true
  expect [ "${status}" = 75 ] || return 1
  for n in 1 2 3 5 8 10; do expect present "$(prune_id "${n}")" || return 1; done
  expect gone "$(prune_id 4)" || return 1
  expect gone "$(prune_id 9)" || return 1
  expect present "$(prune_id 6)" || return 1
  expect present "$(prune_id 7)" || return 1
  expect grep -q "^prune refused $(prune_id 6): still referenced by process [0-9]* (sleep)" "${test_root}/prune.log" || return 1
  expect grep -q "^prune refused $(prune_id 7): still referenced by container fixture-floor" "${test_root}/prune.log" || return 1
  expect grep -qE '^prune-incomplete removed=3 refused=2 failed=0 freed_bytes=[1-9][0-9]* ' "${test_root}/prune.log" || return 1
  # Control checkout: current (4) and the newest three (1 2 3) stay; 5 goes.
  for n in 1 2 3 4; do expect test -d "${control_releases}/$(prune_id "${n}")" || return 1; done
  expect test ! -e "${control_releases}/$(prune_id 5)" || return 1

  # Once nothing holds them they go; and a host with no `previous` still prunes.
  : >"${REHEARSAL_CONTAINERS}"
  rm "${prune_root}/previous"
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >"${test_root}/prune.log" 2>&1 ||
    { cat "${test_root}/prune.log" >&2; return 1; }
  for n in 6 7 8; do expect gone "$(prune_id "${n}")" || return 1; done
  for n in 1 2 3 5 10; do expect present "$(prune_id "${n}")" || return 1; done
  expect grep -q '^prune-ok removed=3 refused=0 failed=0' "${test_root}/prune.log" || return 1

  # Fail closed: no readable contract, or no Docker answer, removes nothing.
  plant_release 11
  printf '{"keep_newest":3}\n' >"${retention_contract}"
  if FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >/dev/null 2>&1; then return 1; fi
  write_retention_contract "$(prune_id 10)" 3
  mkdir -p "${test_root}/broken-docker"
  printf '#!/usr/bin/env bash\nexit 1\n' >"${test_root}/broken-docker/docker"
  chmod +x "${test_root}/broken-docker/docker"
  if PATH="${test_root}/broken-docker:${PATH}" FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" \
    "${script}" prune >/dev/null 2>&1; then
    return 1
  fi
  expect present "$(prune_id 11)" || return 1

  # A release build holds the lock: prune skips, removes nothing, and says so without failing.
  hold_build_lock || return 1
  status=0
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >"${test_root}/prune.log" 2>&1 || status=$?
  release_build_lock_holder
  expect [ "${status}" = 0 ] || return 1
  expect grep -q '^prune-skipped: another release build is running' "${test_root}/prune.log" || return 1
  expect present "$(prune_id 11)" || return 1

  # Docker answering an image inspect with something else refuses that release, not the run.
  printf 'unexpected\n' >"${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 11)"
  status=0
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >"${test_root}/prune.log" 2>&1 || status=$?
  expect [ "${status}" = 75 ] || return 1
  expect grep -q "^prune refused $(prune_id 11): docker image inspect foundation-tippecanoe:$(prune_id 11) answered 'unexpected'" \
    "${test_root}/prune.log" || return 1
  expect test -z "$(grep -l Traceback "${test_root}/prune.log")" || return 1
  expect present "$(prune_id 11)" || return 1
  printf 'sha256:t%s 2000\n' "$(prune_id 11)" >"${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 11)"
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >"${test_root}/prune.log" 2>&1 || return 1
  expect gone "$(prune_id 11)" || return 1
  return 0
}

# Holds admission's release build lock from another process, as a running build does.
locker=""
hold_build_lock() {
  rm -f "${test_root}/lock-held"
  python3 -c 'import fcntl, sys, time
handle = open(sys.argv[1], "a")
fcntl.flock(handle, fcntl.LOCK_EX)
open(sys.argv[2], "w").close()
time.sleep(600)' "${REHEARSAL_BUILD_LOCK}" "${test_root}/lock-held" &
  locker=$!
  local _
  for _ in $(seq 100); do
    [[ ! -e "${test_root}/lock-held" ]] || return 0
    sleep 0.1
  done
  printf 'prune rehearsal: the build lock was never taken\n' >&2
  return 1
}
release_build_lock_holder() {
  [[ -n "${locker}" ]] || return 0
  kill "${locker}" 2>/dev/null || true
  wait "${locker}" 2>/dev/null || true
  locker=""
}

prune_rehearsal "${release_script}" || { cat "${test_root}/prune.log" >&2; exit 1; }
printf 'prune-retention=pass\n'

# A cached build gives an old release the same image ID as a kept one. That ID belongs to the kept
# release: a running container of the kept release does not hold the old one, and the old release's
# tag is not the last thing keeping the kept release's image.
share_tippecanoe() {
  local old kept file
  old="$(prune_id "$1")" kept="$(prune_id "$2")"
  file="${prune_root}/artifacts/${old}/build.json"
  chmod u+w "${prune_root}/artifacts/${old}" "${file}"
  printf '{"publisher_image":"sha256:p%s","tippecanoe_image":"sha256:t%s"}\n' "${old}" "${kept}" >"${file}"
  chmod a-w "${prune_root}/artifacts/${old}" "${file}"
  printf 'sha256:t%s 2000\n' "${kept}" >"${REHEARSAL_IMAGES}/foundation-tippecanoe:${old}"
}
prune_shared_image_rehearsal() {
  local script="$1" status n
  [[ ! -e "${prune_root}" ]] || { chmod -R u+w "${prune_root}"; rm -rf "${prune_root}"; }
  rm -rf "${control_releases%/releases}" "${REHEARSAL_IMAGES}"
  mkdir -p "${REHEARSAL_IMAGES}"
  # current=1 (also the newest one kept), emergency=3; 2 and 4 are old.
  for n in 1 2 3 4; do plant_release "${n}"; done
  ln -s "releases/$(prune_id 1)" "${prune_root}/current"
  write_retention_contract "$(prune_id 3)" 1
  share_tippecanoe 2 1
  # 4 shares 3's image, and 3's own tag is gone: 4's tag is the last one on the kept image.
  share_tippecanoe 4 3
  rm "${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 3)"
  printf 'c1 /fixture-tiles sha256:t%s foundation-tippecanoe:%s\n' "$(prune_id 1)" "$(prune_id 1)" \
    >"${REHEARSAL_CONTAINERS}"
  status=0
  FOUNDATION_PLATFORM_RELEASE_ROOT="${prune_root}" "${script}" prune >"${test_root}/prune.log" 2>&1 || status=$?
  : >"${REHEARSAL_CONTAINERS}"
  expect [ "${status}" = 0 ] || return 1
  expect grep -q '^prune-ok removed=2 refused=0 failed=0' "${test_root}/prune.log" || return 1
  expect test ! -e "${prune_root}/releases/$(prune_id 2)" || return 1
  expect test ! -e "${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 2)" || return 1
  expect test -e "${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 1)" || return 1
  expect test ! -e "${prune_root}/releases/$(prune_id 4)" || return 1
  expect test -e "${REHEARSAL_IMAGES}/foundation-tippecanoe:$(prune_id 4)" || return 1
  expect grep -q "^prune kept image foundation-tippecanoe:$(prune_id 4): sha256:t$(prune_id 3) is a kept release's build" \
    "${test_root}/prune.log" || return 1
  return 0
}
prune_shared_image_rehearsal "${release_script}" || { cat "${test_root}/prune.log" >&2; exit 1; }
printf 'prune-shared-image=pass\n'

# Each rule above is what makes its case pass: with it taken out, the rehearsal must fail.
mutate() {
  local name="$1" expression="$2"
  sed "${expression}" "${release_script}" >"${test_root}/${name}.sh"
  chmod +x "${test_root}/${name}.sh"
  if cmp -s "${release_script}" "${test_root}/${name}.sh"; then
    printf 'mutation %s changed nothing; its expression no longer matches the script\n' "${name}" >&2
    exit 1
  fi
}
mutate shared-image-holds 's/(own - kept_images) | set(tags)/own | set(tags)/'
mutate shared-image-removed 's/found\[0\] in kept_images and found\[0\] not in kept_tagged/False/'
for name in shared-image-holds shared-image-removed; do
  if prune_shared_image_rehearsal "${test_root}/${name}.sh" 2>/dev/null; then
    printf 'prune shared-image rehearsal passed with mutation %s\n' "${name}" >&2
    exit 1
  fi
  printf 'prune-%s-mutation=refused\n' "${name}"
done
mutate without-build-lock 's/build_lock.enter_context(admission.release_build_lock())/pass/'
if prune_rehearsal "${test_root}/without-build-lock.sh" 2>/dev/null; then
  printf 'prune rehearsal passed without taking the release build lock\n' >&2
  exit 1
fi
grep -q '^prune-ok ' "${test_root}/prune.log"
printf 'prune-without-build-lock-mutation=refused\n'

# The emergency rule is what keeps the oldest release here: without it this rehearsal must fail.
sed '/retention rule: the emergency return/d' "${release_script}" >"${test_root}/without-emergency.sh"
chmod +x "${test_root}/without-emergency.sh"
! cmp -s "${release_script}" "${test_root}/without-emergency.sh"
if prune_rehearsal "${test_root}/without-emergency.sh" 2>/dev/null; then
  printf 'prune rehearsal passed with the emergency-keep rule removed\n' >&2
  exit 1
fi
printf 'prune-emergency-rule-mutation=refused\n'

# The emergency release is named in the contract and nowhere else: a copy would be a second list
# that the next change to the contract leaves behind.
named_twice() {
  grep -rlF --exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules \
    --exclude=release-retention.contract.json "$1" "$2"
}
real_contract="${repo_root}/../../tools/release-retention.contract.json"
real_emergency="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["emergency_release"])' "${real_contract}")"
if named_twice "${real_emergency}" "${repo_root}/../.."; then
  printf 'the emergency release id is written outside %s (files above)\n' "${real_contract}" >&2
  exit 1
fi
mkdir -p "${test_root}/copy-check/docs"
printf 'old=%s\n' "${real_emergency}" >"${test_root}/copy-check/docs/runbook.md"
if ! named_twice "${real_emergency}" "${test_root}/copy-check" >/dev/null; then
  printf 'a planted copy of the emergency release id was not found\n' >&2
  exit 1
fi
printf 'emergency-release-named-once=pass\n'

# Activation prunes afterwards; a prune that fails does not fail or undo the activation.
chmod -R u+w "${prune_root}"; rm -rf "${prune_root}" "${control_releases%/releases}"
write_retention_contract "$(prune_id 0)" 20
run_release activate "${candidate}" >"${test_root}/activate-prune.log" 2>&1
grep -q '^prune-ok ' "${test_root}/activate-prune.log"
run_release rollback
assert_link "${release_root}/current" "releases/${guard_current}"
printf 'not json\n' >"${retention_contract}"
run_release activate "${candidate}" >"${test_root}/activate-prune.log" 2>&1
assert_link "${release_root}/current" "releases/${candidate}"
grep -q 'RELEASE PRUNE FAILED' "${test_root}/activate-prune.log"
write_retention_contract "$(prune_id 0)" 20
printf 'activate-survives-prune-failure=pass\n'

# A release build in progress holds the lock: activation still succeeds, and the skipped prune is
# not a failure alarm.
hold_build_lock
run_release activate "${candidate}" >"${test_root}/activate-prune.log" 2>&1
release_build_lock_holder
assert_link "${release_root}/current" "releases/${candidate}"
grep -q '^prune-skipped: ' "${test_root}/activate-prune.log"
if grep -q 'RELEASE PRUNE FAILED' "${test_root}/activate-prune.log"; then
  printf 'a prune skipped for a running release build raised the failure alarm\n' >&2
  exit 1
fi
printf 'activate-skips-prune-during-build=pass\n'

# `install` activates too, so it prunes too; and `rollback` still works on what prune left. A host
# of its own, installed release by release with only the newest one kept beyond current/previous.
install_root="${test_root}/install-prune/opt/foundation-platform"
run_install_root() {
  FOUNDATION_PLATFORM_RELEASE_ROOT="${install_root}" \
  FOUNDATION_PLATFORM_STATE_ROOT="${test_root}/install-prune/var/lib/foundation-platform" \
  FOUNDATION_PLATFORM_ENV_FILE="${install_root}/current/rehearsal.env" "$@"
}
install_prune_rehearsal() {
  local script="$1" pair id
  [[ ! -e "${test_root}/install-prune" ]] || { chmod -R u+w "${test_root}/install-prune"; rm -rf "${test_root}/install-prune"; }
  write_retention_contract "$(prune_id 0)" 1
  for pair in "${release_a}:a" "${release_b}:b" "${prepared}:prepared" "${release_large}:large"; do
    run_install_root "${script}" install "${pair%%:*}" "${test_root}/release-${pair#*:}.tar.gz" \
      >"${test_root}/install-prune.log" 2>&1 || return 1
  done
  # current=large, previous=prepared; a and b are gone, with their artifacts.
  for id in "${release_a}" "${release_b}"; do
    expect test ! -e "${install_root}/releases/${id}" || return 1
    expect test ! -e "${install_root}/artifacts/${id}" || return 1
  done
  expect grep -q "^prune removed ${release_b} " "${test_root}/install-prune.log" || return 1
  expect grep -q '^prune-ok removed=1 ' "${test_root}/install-prune.log" || return 1
  run_install_root "${script}" rollback || return 1
  expect test "$(readlink "${install_root}/current")" = "releases/${prepared}" || return 1
  expect test "$(readlink "${install_root}/previous")" = "releases/${release_large}" || return 1
  run_install_root "${test_root}/admission" verify-current >"${test_root}/install-prune.log" 2>&1 || return 1
  run_install_root "${script}" verify >>"${test_root}/install-prune.log" 2>&1 || return 1
}
# These releases ship the first two migrations; the database answers with exactly those here.
cp "${test_root}/applied.txt" "${test_root}/applied.saved"
printf '20260719000001\n20260719000002\n' >"${test_root}/applied.txt"
install_prune_rehearsal "${release_script}" || { cat "${test_root}/install-prune.log" >&2; exit 1; }
printf 'install-prunes-and-rollback-after-prune=pass\n'
mutate install-without-prune 's/then activate_and_prune "${release_id}"; fi/then activate_release "${release_id}"; fi/'
if install_prune_rehearsal "${test_root}/install-without-prune.sh" 2>/dev/null; then
  printf 'install prune rehearsal passed with install not pruning\n' >&2
  exit 1
fi
printf 'prune-install-without-prune-mutation=refused\n'
cp "${test_root}/applied.saved" "${test_root}/applied.txt"
write_retention_contract "$(prune_id 0)" 20

# A modified active tree cannot install host units. The stand-in records any attempted host
# mutation, so "failed later because the test user cannot write /etc" cannot masquerade as a gate.
export REHEARSAL_INSTALL_CALLED="${test_root}/install-called"
cat >"${test_root}/bin/install" <<'INSTALL'
#!/usr/bin/env bash
: >"${REHEARSAL_INSTALL_CALLED}"
exit 1
INSTALL
chmod +x "${test_root}/bin/install"
tampered="${release_root}/releases/${release_b}/version.txt"
with_writable "${tampered}" sh -c 'printf "private\n" >"$1"' _ "${tampered}"
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
