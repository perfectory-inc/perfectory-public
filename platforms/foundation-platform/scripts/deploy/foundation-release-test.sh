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

run_release prepare "${release_a}" "${test_root}/release-a.tar.gz"
[[ ! -e "${release_root}/current" && ! -L "${release_root}/current" ]]
[[ ! -e "${release_root}/previous" && ! -L "${release_root}/previous" ]]
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
run_release install "4444444444444444444444444444444444444444" \
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

current_before_prepare="$(readlink "${release_root}/current")"
previous_before_prepare="$(readlink "${release_root}/previous")"
prepared=5555555555555555555555555555555555555555
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
run_release prepare "${prepared}" "${test_root}/release-a.tar.gz"
assert_inactive
run_release prepare "${prepared}" "${test_root}/release-a.tar.gz"
assert_inactive
refuse prepare "${prepared}" "${test_root}/release-b.tar.gz"
refuse prepare invalid "${test_root}/release-a.tar.gz"
printf '#!/bin/sh\nexit 0\n' >"${test_root}/publisher"
publisher_sha="$(sha256sum "${test_root}/publisher" | awk '{print $1}')"
publisher_path="${release_root}/releases/${prepared}/bin/foundation-outbox-publisher"
refuse publisher "${prepared}" "${test_root}/publisher" "$(printf '0%.0s' {1..64})"
[[ ! -e "${publisher_path}" ]]
refuse publisher 6666666666666666666666666666666666666666 "${test_root}/publisher" "${publisher_sha}"
ln -s "${test_root}/publisher" "${test_root}/publisher-link"
refuse publisher "${prepared}" "${test_root}/publisher-link" "${publisher_sha}"
mkfifo "${test_root}/publisher-fifo"
refuse publisher "${prepared}" "${test_root}/publisher-fifo" "${publisher_sha}"
mkdir "${test_root}/outside"
ln -s "${test_root}/outside" "${release_root}/releases/${prepared}/bin"
refuse publisher "${prepared}" "${test_root}/publisher" "${publisher_sha}"
[[ ! -e "${test_root}/outside/foundation-outbox-publisher" ]]
rm "${release_root}/releases/${prepared}/bin"
run_release publisher "${prepared}" "${test_root}/publisher" "${publisher_sha}"
assert_inactive
cmp "${test_root}/publisher" "${publisher_path}"
[[ "$(stat -c '%a' "${publisher_path}")" == 755 ]]
if [[ "$(id -u)" == 0 ]]; then [[ "$(stat -c '%u:%g' "${publisher_path}")" == 0:0 ]]; fi
before_retry="$(stat -c '%i:%Y' "${publisher_path}")"
run_release publisher "${prepared}" "${test_root}/publisher" "${publisher_sha}"
[[ "$(stat -c '%i:%Y' "${publisher_path}")" == "${before_retry}" ]]
printf '#!/bin/sh\nexit 1\n' >"${test_root}/different"
different_sha="$(sha256sum "${test_root}/different" | awk '{print $1}')"
refuse publisher "${prepared}" "${test_root}/different" "${different_sha}"
cmp "${test_root}/publisher" "${publisher_path}"
mv "${publisher_path}" "${publisher_path}.saved"
ln -s "${publisher_path}.saved" "${publisher_path}"
refuse publisher "${prepared}" "${test_root}/publisher" "${publisher_sha}"
rm "${publisher_path}"
mv "${publisher_path}.saved" "${publisher_path}"
mv "${release_root}/releases/${prepared}" "${test_root}/relocated-release"
ln -s "${test_root}/relocated-release" "${release_root}/releases/${prepared}"
refuse publisher "${prepared}" "${test_root}/publisher" "${publisher_sha}"
rm "${release_root}/releases/${prepared}"
mv "${test_root}/relocated-release" "${release_root}/releases/${prepared}"
assert_inactive
config_source="${test_root}/floor.env"
config_target="${release_root}/releases/${prepared}/.foundation-floor.env"
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
    if name == 'FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH':
        value = sys.argv[2]
    print(f'{name}={value}')
PY
run_release floor-config "${prepared}" "${config_source}"
assert_inactive
cmp "${config_source}" "${config_target}"
[[ "$(stat -c '%a' "${config_target}")" == 644 ]]
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
cp "${history_witness}" "${release_root}/releases/${prepared}/history.json"
chmod 0444 "${release_root}/releases/${prepared}/history.json"
sed "s|^FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=.*|FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH=${release_root}/releases/${prepared}/history.json|" \
  "${config_source}" >"${test_root}/internal-history.env"
refuse floor-config "${prepared}" "${test_root}/internal-history.env"
cp "${config_source}" "${test_root}/secret.env"
printf 'DATABASE_URL=postgres://fixture\n' >>"${test_root}/secret.env"
refuse floor-config "${prepared}" "${test_root}/secret.env"
sed '/^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=/d' "${config_source}" >"${test_root}/missing.env"
refuse floor-config "${prepared}" "${test_root}/missing.env"
sed 's|^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=.*|FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=${IMAGE}|' "${config_source}" >"${test_root}/interpolation.env"
refuse floor-config "${prepared}" "${test_root}/interpolation.env"
for invalid_value in '' '"quoted"' 'back\slash' 'two words'; do
  FIXTURE_VALUE="${invalid_value}" awk '
    /^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=/ { print "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=" ENVIRON["FIXTURE_VALUE"]; next }
    { print }
  ' "${config_source}" >"${test_root}/unsafe.env"
  refuse floor-config "${prepared}" "${test_root}/unsafe.env"
done
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
sed 's|^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=.*|FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=changed|' "${config_source}" >"${test_root}/conflict.env"
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
    # Exercise the public timers path: invalid input must fail before any unit install.
    if run_release timers; then echo 'unsafe state accepted' >&2; exit 1; fi
    [[ ! -e /etc/systemd/system/foundation-building-register-floor.service ]]
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
  python3 - "${config_target}" "${release_root}/releases/${release_a}" <<'PY'
import pathlib, sys
source, release = map(pathlib.Path, sys.argv[1:])
body = source.read_text().replace(str(source.parent), str(release))
body = body.replace('/var/lib/foundation-platform/building-register-floor',
                    '/data/foundation-platform/building-register-floor')
(release / '.foundation-floor.env').write_text(body)
PY
  run_release activate "${release_a}"
  assert_link "${release_root}/current" "releases/${release_a}"
  [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  run_release rollback
  assert_link "${release_root}/current" "releases/${prepared}"
  [[ "$(sha256sum "${dropin}")" == "${dropin_sha}" ]]
  # Execute the actual embedded validator with an OS-call interposer, no production hook.
  python3 - "${release_script}" "${config_target}" <<'PY'
import os, pathlib, stat, sys, tempfile
script, config = map(pathlib.Path, sys.argv[1:])
code = 'import hashlib' + script.read_text().split("<<'PY'\nimport hashlib", 1)[1].split('\nPY', 1)[0]
values = dict(line.split('=', 1) for line in config.read_text().splitlines())
ivy = pathlib.Path(values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'])
victim = pathlib.Path(tempfile.mkdtemp())
os.chmod(victim, 0o711)
before = victim.stat()
real_open, real_mkdir = os.open, os.mkdir
sys.argv = ['validator', str(config), str(config.parent), 'state']
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
# A release cannot silently forget a running capability. Initial fixtures above deliberately
# lack a jobs file, exercising bootstrap from releases that predate orchestration.
guard_candidate=8888888888888888888888888888888888888888
run_release prepare "${guard_candidate}" "${test_root}/release-a.tar.gz"
current_before_guard="$(readlink "${release_root}/current")"
previous_before_guard="$(readlink "${release_root}/previous")"
current_jobs="${release_root}/current/orchestration/jobs.v1.json"
candidate_jobs="${release_root}/releases/${guard_candidate}/orchestration/jobs.v1.json"
mkdir -p "$(dirname "${current_jobs}")" "$(dirname "${candidate_jobs}")"
printf '%s\n' '{"jobs":[{"id":"fixture_job","enabled":true}]}' >"${current_jobs}"
for candidate in missing '{"jobs":[]}' '{"jobs":[{"id":"different_job","enabled":true}]}' \
  '{"jobs":[{"id":"fixture_job","enabled":"false"}]}' \
  '{"jobs":[{"id":"fixture_job","enabled":true},{"id":"fixture_job","enabled":false}]}'; do
  if [[ "${candidate}" == missing ]]; then rm -f "${candidate_jobs}"; else printf '%s\n' "${candidate}" >"${candidate_jobs}"; fi
  if run_release activate "${guard_candidate}"; then
    echo 'activation silently removed an enabled job' >&2; exit 1
  fi
  assert_link "${release_root}/current" "${current_before_guard}"
  assert_link "${release_root}/previous" "${previous_before_guard}"
done
printf '%s\n' '{"jobs":[{"id":"fixture_job","enabled":false}]}' >"${candidate_jobs}"
run_release activate "${guard_candidate}"
assert_link "${release_root}/current" "releases/${guard_candidate}"
# Explicit rollback remains a recovery operation, not a new activation admission.
printf '%s\n' '{"jobs":[{"id":"new_only_job","enabled":true}]}' >"${candidate_jobs}"
run_release rollback
assert_link "${release_root}/current" "${current_before_guard}"
printf 'foundation-release-test=pass\n'
