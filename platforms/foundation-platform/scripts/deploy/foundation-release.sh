#!/usr/bin/env bash
set -Eeuo pipefail
umask 022
# A command that stops this script under `set -e` says where, before the exit. Without it a failed
# assignment such as `x="$(… 2>/dev/null | …)"` ended the deploy with status 1 and no line at all
# (2026-10-13: `timers` after scheduler-access-ok, with the Airflow DAGs left paused). `-E` carries
# the trap into functions and command substitutions; it reports, it does not change the exit.
trap 'printf "foundation-release.sh: line %s: \`%s\` exited %s\n" "${LINENO}" "${BASH_COMMAND}" "$?" >&2' ERR

# Independent root-owned control checkout, not code taken from the candidate archive (root
# ADR-0134 §1). No environment override: a caller cannot appoint the verifier that admits it.
admission="/opt/perfectory-control/current/scripts/deploy/foundation-release-admission.py"
# The one path sudo may run this script from (root ADR-0134 §2). A wildcard over release
# directories also matches a release that was never admitted.
control_release_script="/opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh"
# The operator's cut-over steps, granted the same way (root ADR-0161): this path and no other.
control_operator_script="/opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh"
# What survives `prune` is read from the control checkout too, for the same reason: a release
# cannot choose what survives it. The emergency release is named there and nowhere else.
retention_contract="/opt/perfectory-control/current/tools/release-retention.contract.json"
control_releases_dir="/opt/perfectory-control/releases"
# The prune itself is a program of the control checkout too, for the same reason.
prune_program="/opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release-prune.py"

release_root="${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform}"
state_root="${FOUNDATION_PLATFORM_STATE_ROOT:-/var/lib/foundation-platform}"
releases_dir="${release_root}/releases"
# Mutable inputs live beside the immutable releases, keyed by release id (root ADR-0134 §3):
# the trusted build output under artifacts/<sha>/, operator configuration under config/<sha>/.
config_dir="${release_root}/config"
lakehouse_uid="${FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}"
lakehouse_gid="${FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}"

usage() {
  cat >&2 <<'USAGE'
usage:
  foundation-release.sh prepare <40-char-git-sha> <source.tar.gz>
  foundation-release.sh floor-config <40-char-git-sha> <non-secret-env-file>
  foundation-release.sh staging-smoke <40-char-git-sha>
  foundation-release.sh install <40-char-git-sha> <source.tar.gz>
  foundation-release.sh activate <40-char-git-sha>
  foundation-release.sh migrate [--skip-unchanged]
  foundation-release.sh verify
  foundation-release.sh timers [<airflow-scheduler-ssh-public-key-file>]
  foundation-release.sh datahub
  foundation-release.sh rollback
  foundation-release.sh prune
  foundation-release.sh host-retention
  foundation-release.sh deployer-access <account> [--exclusive]
  foundation-release.sh operator-access <account>
  foundation-release.sh env-set <group> <NAME=value-file>
  foundation-release.sh status
  foundation-release.sh verify-registry <release-id>
USAGE
  exit 64
}

require_release_id() {
  local release_id="$1"
  [[ "${release_id}" =~ ^[0-9a-f]{40}$ ]] || {
    printf 'release id must be a lowercase 40-character Git SHA: %s\n' "${release_id}" >&2
    exit 64
  }
}

release_path() {
  printf '%s/%s' "${releases_dir}" "$1"
}

# Airflow's only way onto this host (root ADR-0122 §2): an account that cannot log in, whose one
# SSH key runs one command, which may start and stop exactly the services of enabled jobs. All of
# it is rebuilt from the release on every `timers`, so a job switched on or off changes it too.
scheduler_account=foundation-scheduler
scheduler_home=/var/lib/foundation-scheduler

install_scheduler_access() {
  local jobs_file="$1" public_key_file="$2" key subnet sudoers staged
  if ! id "${scheduler_account}" >/dev/null 2>&1; then
    useradd --system --home-dir "${scheduler_home}" --create-home --shell /bin/bash \
      --groups systemd-journal "${scheduler_account}"
    # No password and not locked: sshd still accepts its key, nothing accepts a password.
    usermod -p '*' "${scheduler_account}"
  fi
  install -d -o "${scheduler_account}" -g "${scheduler_account}" -m 0700 "${scheduler_home}/.ssh"
  if [[ -n "${public_key_file}" ]]; then
    key="$(cat "${public_key_file}")"
    [[ "${key}" =~ ^ssh-ed25519\ [A-Za-z0-9+/=]+(\ [A-Za-z0-9@._-]+)?$ ]] || {
      printf 'scheduler key must be one ssh-ed25519 public key line: %s\n' "${public_key_file}" >&2
      exit 65
    }
    # The key works only from the scheduler's own network, read from Docker rather than written
    # down: start Airflow (airflow-runtime.sh up -d) before installing it.
    subnet="$(docker network inspect airflow_default \
      -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' 2>/dev/null | awk '{print $1}')"
    [[ "${subnet}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$ ]] || {
      printf 'the airflow_default network is missing; run airflow-runtime.sh up -d first\n' >&2
      exit 69
    }
    # No pty, forwarding or agent; the command is fixed whatever the client asks for.
    printf 'restrict,from="%s",command="%s/current/scripts/ops/start-scheduled-job.sh" %s\n' \
      "${subnet}" "${release_root}" "${key}" >"${scheduler_home}/.ssh/authorized_keys"
    chown "${scheduler_account}:${scheduler_account}" "${scheduler_home}/.ssh/authorized_keys"
    chmod 0600 "${scheduler_home}/.ssh/authorized_keys"
  fi
  # Exact command lines, one start and one stop per enabled job's service; nothing else.
  staged="$(mktemp)"
  python3 - "${jobs_file}" "${scheduler_account}" >"${staged}" <<'PY'
import json, re, sys
jobs, account = sys.argv[1], sys.argv[2]
print("# Generated by foundation-release.sh timers from orchestration/jobs.v1.json (root ADR-0122).")
for job in json.load(open(jobs))["jobs"]:
    # A job's check service (root ADR-0277) is granted as its service is.
    for service in filter(None, (job["systemd_service"], job.get("check_service"))):
        if not re.fullmatch(r"foundation-[a-z0-9-]+(@[a-z0-9-]+)?\.service", service):
            sys.exit(f"refusing service name {service!r}")
        if job["enabled"]:
            print(f"{account} ALL=(root) NOPASSWD: /usr/bin/systemctl start --no-block {service}, "
                  f"/usr/bin/systemctl stop {service}")
PY
  sudoers=/etc/sudoers.d/foundation-scheduler
  visudo -cqf "${staged}" || {
    rm -f "${staged}"
    printf 'generated sudoers did not validate; %s left unchanged\n' "${sudoers}" >&2
    exit 65
  }
  install -o root -g root -m 0440 "${staged}" "${sudoers}"
  rm -f "${staged}"
  printf 'scheduler-access-ok account=%s key=%s\n' "${scheduler_account}" \
    "$([[ -s "${scheduler_home}/.ssh/authorized_keys" ]] && echo installed || echo missing)"
}

prepare_mutable_state() {
  local directory
  [[ "${lakehouse_uid}" =~ ^[0-9]+$ && "${lakehouse_gid}" =~ ^[0-9]+$ ]] || {
    printf 'lakehouse uid and gid must be numeric\n' >&2
    exit 64
  }
  for directory in "${state_root}/lakehouse" "${state_root}/remote-lakehouse"; do
    mkdir -p "${directory}"
    if [[ "$(id -u)" -eq 0 ]]; then
      chown "${lakehouse_uid}:${lakehouse_gid}" "${directory}"
    fi
    chmod 0770 "${directory}"
    [[ -w "${directory}" || "$(id -u)" -eq 0 ]] || {
      printf 'mutable state directory is not writable: %s\n' "${directory}" >&2
      exit 73
    }
  done
}

atomic_link() {
  local target="$1"
  local link_path="$2"
  local pending="${link_path}.next.$$"
  ln -s "${target}" "${pending}"
  mv -Tf "${pending}" "${link_path}"
}

assert_installed_release() {
  local release_id="$1"
  local path
  path="$(release_path "${release_id}")"
  [[ -d "${path}" ]] || {
    printf 'release is not installed: %s\n' "${release_id}" >&2
    exit 66
  }
  [[ "$(cat "${path}/.foundation-release-id")" == "${release_id}" ]] || {
    printf 'release identity evidence is missing or invalid: %s\n' "${path}" >&2
    exit 65
  }
  # Canonical-main ancestry and every installed byte, read again; the marker above is a name. Then
  # the tree is made immutable (chattr +i) and read once more, so a release is sealed before anything
  # runs from it: activation, rollback and FLOOR all come through here, and a release installed
  # before root ADR-0184 is sealed the first time it is used. Sealing a sealed release changes nothing.
  # `verify` adds the trusted build output and its images.
  "${admission}" seal "${path}"
  "${admission}" verify "${path}"
}

assert_current_release() {
  local current_target
  current_target="$(readlink "${release_root}/current")"
  [[ "${current_target}" =~ ^releases/([0-9a-f]{40})$ ]] || {
    printf 'current is not an installed release link\n' >&2
    exit 65
  }
  assert_installed_release "${BASH_REMATCH[1]}"
}

# The drop-in goes on the unit systemd actually loads for the service. For a template instance
# that is the template, so an instance nobody registered (started by hand) is checked too.
admission_dropin_unit() {
  local service="$1"
  [[ "${service}" =~ ^(foundation-[a-z0-9-]+)(@[a-z0-9-]*)?\.service$ ]] || {
    printf 'refusing invalid service name for release admission: %s\n' "${service}" >&2
    return 65
  }
  if [[ -n "${BASH_REMATCH[2]}" ]]; then
    printf '%s@.service\n' "${BASH_REMATCH[1]}"
  else
    printf '%s.service\n' "${BASH_REMATCH[1]}"
  fi
}

install_release_admission() {
  local unit
  unit="$(admission_dropin_unit "$1")" || exit 65
  install -d -o root -g root -m 0755 "/etc/systemd/system/${unit}.d"
  install -o root -g root -m 0644 \
    "${release_root}/current/infra/systemd/foundation-release-admission.conf" \
    "/etc/systemd/system/${unit}.d/10-release-admission.conf"
}

# sudo's grant for this script names the control checkout's copy and nothing else (root
# ADR-0134 §2). The rule this replaces, /opt/foundation-platform/*/scripts/deploy/..., matched
# every directory under the release root, including a release that was never admitted.
render_deployer_sudoers() {
  local account="$1"
  [[ "${account}" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || {
    printf 'deployer account must be a plain user name: %s\n' "${account}" >&2
    return 64
  }
  printf '# Generated by foundation-release.sh deployer-access (root ADR-0134 §2).\n'
  printf '%s ALL=(root) NOPASSWD: %s\n' "${account}" "${control_release_script}"
}

# Prints every sudoers line that still grants this script by any other path. Exit 0 only when
# none does: an old wildcard left beside the exact rule keeps the unadmitted path open.
find_other_release_grants() {
  local found=0 file
  for file in "$@"; do
    [[ -f "${file}" ]] || continue
    # Rule lines only: a comment that names this script grants nothing. The exact control path is
    # removed before looking, so a second path on the same line as it is still reported.
    if grep -HnE '^[[:space:]]*[^#[:space:]].*foundation-release\.sh' "${file}" |
      sed "s|${control_release_script}||g" | grep -E 'foundation-release\.sh'; then
      found=1
    fi
  done
  return "${found}"
}

# Two steps, in this order (root ADR-0134 §2): `deployer-access <account>` installs the exact rule
# and lists any older grant still present, which keeps working until it is deleted; after the new
# path has been used successfully, the old line is deleted and `deployer-access <account>
# --exclusive` fails while any other grant remains.
install_deployer_access() {
  local account="$1" mode="${2:-}" staged sudoers=/etc/sudoers.d/foundation-release
  [[ -z "${mode}" || "${mode}" == --exclusive ]] || usage
  id "${account}" >/dev/null 2>&1 || {
    printf 'deployer account does not exist: %s\n' "${account}" >&2
    exit 67
  }
  staged="$(mktemp)"
  render_deployer_sudoers "${account}" >"${staged}" || { rm -f "${staged}"; exit 64; }
  visudo -cqf "${staged}" || {
    rm -f "${staged}"
    printf 'generated sudoers did not validate; %s left unchanged\n' "${sudoers}" >&2
    exit 65
  }
  install -o root -g root -m 0440 "${staged}" "${sudoers}"
  rm -f "${staged}"
  deployer_grant_verdict "${account}" "${mode}" /etc/sudoers /etc/sudoers.d/* || exit 65
}

# The operator account runs a cut-over's gates, publish and canary verdicts through one script that
# takes a lane, an action and a generation or version id, never a path or a command (root
# ADR-0161). The grant names the control checkout's copy, which only an admitted main reaches.
install_operator_access() {
  local account="$1" staged sudoers=/etc/sudoers.d/foundation-operator
  [[ "${account}" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || {
    printf 'operator account must be a plain user name: %s\n' "${account}" >&2
    exit 64
  }
  id "${account}" >/dev/null 2>&1 || {
    printf 'operator account does not exist: %s\n' "${account}" >&2
    exit 67
  }
  staged="$(mktemp)"
  {
    printf '# Generated by foundation-release.sh operator-access (root ADR-0161).\n'
    printf '%s ALL=(root) NOPASSWD: %s\n' "${account}" "${control_operator_script}"
  } >"${staged}"
  visudo -cqf "${staged}" || {
    rm -f "${staged}"
    printf 'generated sudoers did not validate; %s left unchanged\n' "${sudoers}" >&2
    exit 65
  }
  install -o root -g root -m 0440 "${staged}" "${sudoers}"
  rm -f "${staged}"
  printf 'operator-access-ok account=%s command=%s\n' "${account}" "${control_operator_script}"
}

deployer_grant_verdict() {
  local account="$1" mode="$2"; shift 2
  if ! find_other_release_grants "$@"; then
    if [[ "${mode}" == --exclusive ]]; then
      printf 'another sudoers rule still grants this script; delete it (runbook: release admission)\n' >&2
      return 65
    fi
    printf 'deployer-access-installed account=%s; the grants listed above remain until deleted\n' "${account}"
    return 0
  fi
  printf 'deployer-access-ok account=%s command=%s\n' "${account}" "${control_release_script}"
}

activate_release() {
  local release_id="$1"
  local next_target="releases/${release_id}"
  local current_target=""
  assert_installed_release "${release_id}"

  if [[ -L "${release_root}/current" ]]; then
    current_target="$(readlink "${release_root}/current")"
  elif [[ -e "${release_root}/current" ]]; then
    printf 'current exists but is not a symbolic link\n' >&2
    exit 65
  fi

  # Compare the two existing contracts, not a second capability registry. A release predating
  # orchestration has nothing to preserve; removing an enabled job requires an explicit false
  # entry first. The dedicated rollback command intentionally bypasses this admission gate.
  python3 - "${release_root}/current/orchestration/jobs.v1.json" \
    "$(release_path "${release_id}")/orchestration/jobs.v1.json" <<'PY'
import json, pathlib, sys

def jobs(path):
    if not path.exists():
        if path.is_symlink():
            raise ValueError('dangling jobs contract')
        return {}
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict) or not isinstance(raw.get('jobs'), list):
        raise ValueError('jobs contract must contain a jobs array')
    result = {}
    for job in raw['jobs']:
        if (not isinstance(job, dict) or not isinstance(job.get('id'), str) or
            not job['id'] or type(job.get('enabled')) is not bool or job['id'] in result):
            raise ValueError('invalid or duplicate job identity/enabled value')
        result[job['id']] = job['enabled']
    return result

try:
    previous, candidate = map(jobs, map(pathlib.Path, sys.argv[1:]))
    missing = sorted(name for name, enabled in previous.items() if enabled and name not in candidate)
    if missing:
        raise ValueError('candidate removes enabled jobs; retain explicit enabled:false entries: ' + ', '.join(missing))
except (OSError, ValueError) as error:
    sys.exit('release activation refused: ' + str(error))
PY

  if [[ -n "${current_target}" && "${current_target}" != "${next_target}" ]]; then
    atomic_link "${current_target}" "${release_root}/previous"
  fi
  # Before the switch: a host that cannot make them keeps the release it runs.
  prepare_job_state_directories "$(release_path "${release_id}")"
  atomic_link "${next_target}" "${release_root}/current"
}

# A release is a tree rooted at platforms/foundation-platform, and this is where that is
# enforced — before anything is staged or activated. On 2026-09-03 an archive of the monorepo
# root installed, activated, and only then failed the schema check because scripts/deploy/ was
# a level deeper than every path this script derives. By that point the release id was sealed
# to the wrong archive's sha256 and could never be installed correctly again. The marker asked
# for is this script's own runtime schema check: the thing `install` will need to run last.
validate_archive_is_a_release() {
  local archive="$1" listing
  # The listing is captured, not piped into the match. Piped, `grep -q` exits on its first hit
  # and closes the pipe; tar dies of SIGPIPE (141), and under `pipefail` the pipeline fails even
  # though the file was found — so this gate refused every valid archive large enough for tar to
  # still be writing when grep matched. That is exactly what happened on 2026-09-03: the first
  # real deployment after this gate merged was refused with the "not a release tree" message,
  # while the same grep run by hand on the same archive counted one match. The single-file
  # rehearsal fixture never triggered it, because tar had already finished. A verdict must never
  # sit downstream of a pipe whose producer can be killed by the verdict being reached.
  listing="$(tar -tzf "${archive}")"
  grep -qE '^(\./)?scripts/deploy/assert-runtime-migrations\.sh$' <<<"${listing}" || {
    printf 'archive is not a release tree: scripts/deploy/assert-runtime-migrations.sh is not at its root\n' >&2
    printf 'archive the subtree, not the monorepo: git archive "<sha>:platforms/foundation-platform"\n' >&2
    exit 65
  }
}

validate_archive_paths() {
  local archive="$1"
  local entry normalized
  while IFS= read -r entry; do
    normalized="${entry#./}"
    [[ -z "${normalized}" || "${normalized}" == "." ]] && continue
    if [[ "${normalized}" == /* || "${normalized}" =~ (^|/)\.\.(/|$) ]]; then
      printf 'archive contains unsafe path: %s\n' "${entry}" >&2
      exit 65
    fi
  done < <(tar -tzf "${archive}")
}

install_release() {
  local release_id="$1"
  local archive="$2"
  local activate="${3:-yes}"
  local target archive_sha recorded_sha staging=""
  require_release_id "${release_id}"
  [[ -f "${archive}" ]] || {
    printf 'release archive does not exist: %s\n' "${archive}" >&2
    exit 66
  }
  validate_archive_paths "${archive}"
  validate_archive_is_a_release "${archive}"
  archive_sha="$(sha256sum "${archive}" | awk '{print $1}')"
  target="$(release_path "${release_id}")"

  mkdir -p "${releases_dir}" "${state_root}/recovery"
  prepare_mutable_state
  if [[ -e "${target}" ]]; then
    # The build reads a sealed release only; one moved in place by a run that stopped before its seal
    # is sealed here (root ADR-0184).
    "${admission}" seal "${target}"
    "${admission}" build "${target}"
    assert_installed_release "${release_id}"
    recorded_sha="$(cat "${target}/.foundation-release-archive-sha256")"
    # A release is its contents, not its archive's bytes (root ADR-0363 §7). The lines above proved
    # every installed byte is the canonical commit's; other archive bytes of the same commit
    # (2026-10-30: the first try of that commit archived with the time of day as every mtime) are the
    # same release when admission finds their contents to be that commit's. Other contents refuse.
    if [[ "${recorded_sha}" != "${archive_sha}" ]]; then
      "${admission}" check-archive "${release_id}" "${archive}" || {
        printf 'release id already exists with a different archive: %s\n' "${release_id}" >&2
        exit 65
      }
      printf 'release %s is installed from other bytes of the same contents (archive %s, installed from %s); kept\n' \
        "${release_id}" "${archive_sha}" "${recorded_sha}"
    fi
    if [[ "${activate}" == yes ]]; then activate_and_prune "${release_id}"; fi
    return
  fi

  staging="$(mktemp -d "${releases_dir}/.${release_id}.tmp.XXXXXX")"
  trap '[[ -z "${staging:-}" ]] || { chmod -R u+w "${staging}"; rm -rf "${staging}"; }' RETURN
  # Admission fetches canonical main, checks ancestry AND the complete archive contents, then
  # writes read-only files from those Git objects. The supplied tar is never extracted.
  "${admission}" prepare "${release_id}" "${archive}" "${staging}"
  mv "${staging}" "${target}"
  staging=""
  trap - RETURN
  # Sealed in place, before the build reads it (root ADR-0184): an immutable directory cannot be
  # renamed, so this comes after the move. A run that stops here leaves it unsealed, and the next
  # install of the same id seals it through assert_installed_release.
  "${admission}" seal "${target}"
  # The publisher and Spark jars are built from the admitted tree into artifacts/<sha>/, never
  # accepted from the caller and never written inside the release.
  "${admission}" build "${target}"
  if [[ "${activate}" == yes ]]; then activate_and_prune "${release_id}"; fi
}

# FLOOR's non-secret configuration is per release but outside it: the release is read-only and
# admission refuses any file it does not hold (root ADR-0134 §3). A same-directory hard link
# publishes it create-only, so concurrent installers cannot overwrite each other.
floor_config_path() {
  printf '%s/%s/building-register-floor.env' "${config_dir}" "$1"
}

install_floor_config_file() (
  set -Eeuo pipefail
  local release_id="$1" source="$2" expected="$3"
  local directory output staged=""
  require_release_id "${release_id}"
  [[ "${expected}" =~ ^[0-9a-f]{64}$ ]] || { echo 'invalid floor config sha256' >&2; exit 64; }
  [[ -f "${source}" && ! -L "${source}" ]] || { echo 'floor config must be a regular non-symlink file' >&2; exit 65; }
  directory="${config_dir}/${release_id}"
  output="$(floor_config_path "${release_id}")"
  for path in "${config_dir}" "${directory}"; do
    [[ ! -L "${path}" ]] || { echo 'floor config directory is a symlink' >&2; exit 65; }
    if [[ ! -e "${path}" ]]; then mkdir -m 0755 "${path}"; fi
    [[ -d "${path}" && "$(realpath -e "${path}")" == "$(realpath -ms "${path}")" ]] || {
      echo 'invalid floor config directory' >&2; exit 65;
    }
  done
  check_config() {
    [[ -f "${output}" && ! -L "${output}" &&
       "$(sha256sum "${output}" | awk '{print $1}')" == "${expected}" &&
       "$(stat -c '%a' "${output}")" == 644 ]] || {
      echo 'existing floor config differs or is not a regular 0644 file' >&2; return 65;
    }
    if [[ "$(id -u)" == 0 ]]; then
      [[ "$(stat -c '%u:%g' "${output}")" == 0:0 ]] || { echo 'floor config is not root-owned' >&2; return 65; }
    fi
  }
  if [[ -e "${output}" || -L "${output}" ]]; then check_config; exit; fi
  staged="$(mktemp "${directory}/.config.XXXXXX")"
  trap '[[ -z "${staged}" ]] || rm -f -- "${staged}"' EXIT
  cp -- "${source}" "${staged}"
  [[ "$(sha256sum "${staged}" | awk '{print $1}')" == "${expected}" ]] || {
    echo 'floor config changed while copying' >&2; exit 65;
  }
  chmod 644 "${staged}"
  if [[ "$(id -u)" == 0 ]]; then chown root:root "${staged}"; fi
  if ! ln -T -- "${staged}" "${output}"; then check_config; fi
)

validate_floor_config() {
  local source="$1" target="$2" mode="$3"
  [[ -f "${source}" && ! -L "${source}" ]] || { echo 'floor config must be a regular non-symlink file' >&2; return 65; }
  # The release example owns the key set. This only validates non-secret env transport;
  # image/network/endpoint semantics remain with the runtime validators.
  python3 - "${source}" "${target}" "${mode}" <<'PY'
import hashlib, grp, json, os, pathlib, pwd, re, secrets, stat, subprocess, sys
source, release = map(pathlib.Path, sys.argv[1:3])
mode = sys.argv[3]
example = release / 'infra/systemd/building-register-floor.env.example'
def fail():
    sys.exit('invalid public FLOOR configuration or release configuration schema')
def parse(text, example=False):
    values = {}
    for line in text.split('\n'):
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        match = re.fullmatch(r'([A-Z][A-Z0-9_]*)=([A-Za-z0-9_./:@-]*)', line)
        if not match or match[1] in values or (not example and not match[2]):
            fail()
        values[match[1]] = match[2]
    return values
try:
    if not example.is_file() or example.is_symlink() or example.resolve() != example.absolute():
        fail()
    with source.open('rb') as handle:
        raw = handle.read(65537)
    if len(raw) > 65536 or any(byte < 32 and byte != 10 or byte == 127 for byte in raw):
        fail()
    values = parse(raw.decode('utf-8'))
    allowed = parse(example.read_text(encoding='utf-8'), example=True)
    if not allowed or values.keys() != allowed.keys():
        fail()
    if values.get('FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT') != str(release.resolve()):
        fail()
    # FLOOR's native image is the one this release's trusted build produced (root ADR-0134 §3):
    # the image ID build.json recorded, not any digest that merely looks pinned.
    build = release.resolve().parent.parent / 'artifacts' / release.name / 'build.json'
    if values.get('FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE') != json.loads(build.read_text(encoding='utf-8')).get('publisher_image'):
        sys.exit('FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE must be publisher_image from ' + str(build))
    witness = pathlib.Path(values['FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH'])
    if (not witness.is_absolute() or '..' in witness.parts or
        witness.resolve(strict=True) != witness or release.resolve() in witness.parents):
        fail()
    # Production installs/state preparation run as root. Rootless temporary rehearsals own
    # their own fixture; they cannot write the protected production release/config paths.
    owners = {0, os.geteuid()}
    for parent in witness.parents:
        info = parent.lstat()
        sticky_root = info.st_uid == 0 and info.st_mode & stat.S_ISVTX
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid not in owners or
            (info.st_mode & 0o022 and not sticky_root)):
            fail()
    descriptor = os.open(witness, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in owners or
            info.st_nlink != 1 or info.st_mode & 0o222 or not 0 < info.st_size <= 65536):
            fail()
        handle.read(1)
    if os.geteuid() == 0:
        account = pwd.getpwnam('foundation-platform')
        service_gid = grp.getgrnam('foundation-platform').gr_gid
        # Root can read 0400 files that the host/native service and Spark cannot. Ask the
        # kernel under their actual IDs, without inheriting root's supplementary groups.
        probe = ('import os,stat,sys; '
                 'fd=os.open(sys.argv[1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK); '
                 'assert stat.S_ISREG(os.fstat(fd).st_mode); os.read(fd,1); os.close(fd)')
        for reader_uid in {account.pw_uid, 185}:
            subprocess.run([sys.executable, '-I', '-c', probe, str(witness)],
                           user=reader_uid, group=service_gid, extra_groups=[],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=True, timeout=10)
except (OSError, UnicodeError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError):
    fail()
if mode == 'hash':
    print(hashlib.sha256(raw).hexdigest())
    sys.exit(0)
if mode != 'state' or os.geteuid() != 0:
    fail()

# Only the two dedicated host namespaces are supported. The configuration owns the
# selected path; this allowlist is a privilege boundary, not another default.
state = pathlib.Path(values['FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT'])
ivy = pathlib.Path(values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'])
approved_namespaces = ('/var/lib/foundation-platform/building-register-floor',
                       '/data/foundation-platform/building-register-floor')
if str(state) not in approved_namespaces:
    fail()
if str(state) != values['FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT']:
    fail()
if (str(ivy) != values['FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE'] or
    '..' in ivy.parts or ivy == state or state not in ivy.parents):
    fail()
uid, gid = account.pw_uid, service_gid
dropin = pathlib.Path('/etc/systemd/system/foundation-building-register-floor.service.d/10-state.conf')
dedicated = [state]
for part in ivy.relative_to(state).parts:
    dedicated.append(dedicated[-1] / part)

directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
descriptors = {pathlib.Path('/'): os.open('/', directory_flags)}

def open_directory(path, create=False):
    if path in descriptors:
        return descriptors[path]
    parent = open_directory(path.parent, create)
    if parent is None:
        return None
    created = None
    try:
        fd = os.open(path.name, directory_flags, dir_fd=parent)
    except FileNotFoundError:
        if not create:
            return None
        # A concurrent creator is an existing directory, never ours to chown.
        try:
            os.mkdir(path.name, 0o700, dir_fd=parent)
            created = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        except FileExistsError:
            pass
        fd = os.open(path.name, directory_flags, dir_fd=parent)
    info = os.fstat(fd)
    if created is not None:
        if ((created.st_dev, created.st_ino) != (info.st_dev, info.st_ino) or
            info.st_uid != 0 or info.st_mode & 0o777 != 0o700):
            os.close(fd)
            fail()
        if path in dedicated:
            os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o2770 if path in dedicated else 0o755)
        info = os.fstat(fd)
    if (path in dedicated or str(path) in approved_namespaces) and (info.st_uid, info.st_gid) != (uid, gid):
        os.close(fd)
        fail()
    if path == dropin.parent and (info.st_uid != 0 or info.st_mode & 0o022):
        os.close(fd)
        fail()
    descriptors[path] = fd
    return fd

# Hold existing directory inodes through mutation; no later pathname chmod/chown.
for path in (*(pathlib.Path(name) for name in approved_namespaces), ivy, dropin.parent):
    open_directory(path)
if source.stat().st_uid != 0 or stat.S_IMODE(source.stat().st_mode) != 0o644:
    fail()
body = ('[Service]\nReadWritePaths=\nReadWritePaths=' +
        ' '.join('-' + path for path in approved_namespaces) + '\n').encode()
existing = None
parent = open_directory(dropin.parent)
if parent is not None:
    try:
        fd = os.open(dropin.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    except FileNotFoundError:
        pass
    else:
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0:
                fail()
            existing = (handle.read(len(body) + 1), stat.S_IMODE(info.st_mode), info.st_gid)
for directory in dedicated:
    fd = open_directory(directory, create=True)
    if stat.S_IMODE(os.fstat(fd).st_mode) != 0o2770:
        os.fchmod(fd, 0o2770)
parent = open_directory(dropin.parent, create=True)
if existing == (body, 0o644, 0):
    sys.exit(0)
pending = '.state.' + secrets.token_hex(16)
fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
try:
    with os.fdopen(fd, 'wb') as handle:
        handle.write(body)
        os.fchmod(handle.fileno(), 0o644)
        os.fchown(handle.fileno(), 0, 0)
    os.replace(pending, dropin.name, src_dir_fd=parent, dst_dir_fd=parent)
finally:
    try:
        os.unlink(pending, dir_fd=parent)
    except FileNotFoundError:
        pass
PY
}

install_floor_config() {
  local release_id="$1" source="$2" expected target
  require_release_id "${release_id}"
  assert_installed_release "${release_id}"
  target="$(release_path "${release_id}")"
  expected="$(validate_floor_config "${source}" "${target}" hash)" || exit 65
  install_floor_config_file "${release_id}" "${source}" "${expected}"
}

prepare_floor_state() {
  local target release_id
  target="$(realpath -e "${release_root}/current")"
  release_id="${target##*/}"
  require_release_id "${release_id}"
  assert_installed_release "${release_id}"
  [[ "${target}" == "$(realpath -ms "$(release_path "${release_id}")")" ]] || {
    echo 'current FLOOR release is outside the installed release tree' >&2; return 65;
  }
  validate_floor_config "$(floor_config_path "${release_id}")" "${target}" state
}

rollback_release() {
  local previous_target previous_id current_target
  prepare_mutable_state
  [[ -L "${release_root}/previous" ]] || {
    printf 'no previous release is available\n' >&2
    exit 66
  }
  previous_target="$(readlink "${release_root}/previous")"
  [[ "${previous_target}" =~ ^releases/([0-9a-f]{40})$ ]] || {
    printf 'previous release link is invalid: %s\n' "${previous_target}" >&2
    exit 65
  }
  previous_id="${BASH_REMATCH[1]}"
  assert_installed_release "${previous_id}"
  current_target="$(readlink "${release_root}/current")"
  atomic_link "${previous_target}" "${release_root}/current"
  atomic_link "${current_target}" "${release_root}/previous"
}

# Every deploy left about 0.9GB on the root disk — releases/<sha>, artifacts/<sha>, config/<sha>
# and two image tags — and nothing ever took it back: 49 releases were kept, and on 2026-10-02 the
# root disk filled to zero twice. This keeps what rollback and the emergency return need, the
# newest few, and removes the rest. The control checkout's releases are pruned the same way.
#
# Fail closed: an unreadable contract, a `current` that is not a release link, or a Docker that
# cannot list its containers removes nothing. A release a running process or container still
# references is refused by name, not removed. Exit 75 when anything was refused.
#
# It holds admission's release build lock for its whole run, taken with admission's own code: a
# build writes artifacts/<sha> and image tags while it runs. A held lock skips the prune (exit 0,
# nothing removed) rather than raising the failure alarm; the next activation, or `prune` by hand,
# catches up.
prune_releases() {
  python3 -I -B "${prune_program}" "${retention_contract}" "${release_root}" "${control_releases_dir}" "${admission}"
}

# Retention runs after the switch, never instead of it: a failed prune leaves the new release
# active and the deploy succeeded, but it must not pass quietly, since the next deploys fill the
# disk again (runbook: 릴리스 보존).
prune_after_activation() {
  local status=0
  prune_releases || status=$?
  [[ "${status}" == 0 ]] && return 0
  printf '\n!!! RELEASE PRUNE FAILED (exit %s) after activating %s; the activation stands.\n' \
    "${status}" "$1" >&2
  printf '!!! Old releases are still on the root disk. Fix the cause above, then run: foundation-release.sh prune\n\n' >&2
  if command -v logger >/dev/null 2>&1; then
    logger -p user.err -t foundation-release "release prune failed (exit ${status}) after activating $1" || true
  fi
  return 0
}

# Every path that switches `current` to a new release prunes after it, the same way: `install`
# once activated without pruning, so a host deployed by `install` alone kept every release.
activate_and_prune() {
  activate_release "$1"
  prune_after_activation "$1"
}

verify_runtime_schema() {
  assert_current_release
  # Deploying source cannot move the schema: the migrations are compiled into the runtime image
  # by `sqlx::migrate!`. On 2026-09-01 that gap was six weeks and thirty-three-minus-four
  # migrations wide, with every catalog table empty and the API answering `[]`, and no step
  # anywhere compared the two. This is that step.
  local checker="${release_root}/current/scripts/deploy/assert-runtime-migrations.sh"
  [[ -x "${checker}" ]] || {
    printf 'runtime migration check is missing from the release: %s\n' "${checker}" >&2
    exit 66
  }
  local environment="${release_root}/current/scripts/deploy/assert-runtime-environment.sh"
  [[ -x "${environment}" ]] || {
    printf 'runtime environment check is missing from the release: %s\n' "${environment}" >&2
    exit 66
  }
  # The environment first. A variable the compose files require and the host does not carry
  # surfaces as an interpolation error in the middle of a migration run — after the image
  # rebuild, with nothing having said beforehand that the file was behind. Measured 2026-09-01
  # while repairing the schema gap: six variables were missing, and compose named one per
  # attempt, so the hole looked two deep until the whole file was compared (root ADR-0071).
  "${environment}" "${release_root}/current"
  verify_runtime_secrets
  "${checker}" "${release_root}/current"
}

# Every environment file the release's units load exists, is owned and no more open than the
# runtime-secrets contract says, and holds every name it declares (root ADR-0153). Names only: the
# checker prints a missing name, never a value. On 2026-10-05 a unit's script required the lakehouse
# reader key that none of its files held, and the first run found out after writing to Bronze.
verify_runtime_secrets() {
  local secrets="${release_root}/current/scripts/deploy/runtime_secrets.py"
  [[ -f "${secrets}" ]] || {
    printf 'runtime secrets check is missing from the release: %s\n' "${secrets}" >&2
    exit 66
  }
  # FOUNDATION_PLATFORM_SECRETS_ROOT is the filesystem root the group paths are under: `/` on the
  # host, a fake host in foundation-release-test.sh.
  # First a renamed credential gains its derived name, with the value its old name has, in every
  # file that holds it (root ADR-0247): the files change through the contract, never by hand.
  python3 -I "${secrets}" --area "${release_root}/current" rename --root "${FOUNDATION_PLATFORM_SECRETS_ROOT:-/}" || {
    printf 'renaming credentials in the host environment files failed; see above (names only)\n' >&2
    exit 65
  }
  python3 -I "${secrets}" --area "${release_root}/current" host --root "${FOUNDATION_PLATFORM_SECRETS_ROOT:-/}" || {
    printf 'the host environment files do not match config/runtime-secrets.contract.json; see above (names only)\n' >&2
    exit 65
  }
}

migrate_runtime() {
  local lakehouse="$1"
  assert_current_release
  # Rebuild before applying. The migrator is the release's own binary and the migrations live
  # inside it, so an image built from an older tree applies an older set however new the source
  # on disk is — which is exactly how the runtime came to be six weeks behind.
  local runtime="${release_root}/current/scripts/deploy/foundation-runtime.sh"
  [[ -x "${runtime}" ]] || {
    printf 'runtime compose wrapper is missing from the release: %s\n' "${runtime}" >&2
    exit 66
  }
  # A release from the registry (root ADR-0276 §13) records the API and recovery images CI built and
  # admission checked against the signed manifest; they become the images compose names, unbuilt.
  local tagged=0 steps="${release_root}/current/scripts/deploy/deploy_steps.py"
  "${admission}" tag-compose-images || tagged=$?
  case "${tagged}" in
    0) ;;
    75) "${runtime}" build foundation-api ;;
    *) printf 'release admission refused the recorded compose images\n' >&2; exit 65 ;;
  esac
  # Asks for the end state, and names no step on the way to it.
  #
  # The compose file already declares the order — postgres, bootstrap, migrate, grants,
  # finalize, api — each link a `service_completed_successfully`. This used to run
  # `up --no-deps postgres` and then `run foundation-migrate`, which is that chain written a
  # second time and stopped in the middle. On 2026-09-02 the consequence surfaced: the database
  # had every migration and the API answered `permission denied for schema catalog`, because
  # `foundation-runtime-grants` never ran. Nothing said so — `/health` does not touch the
  # database, so the stack looked up for a day.
  #
  # The chain runs to completion first, with the running API left alone, and the API is replaced
  # only after it succeeded: a refused migration leaves the previous release serving (root
  # ADR-0245). Asking for `foundation-api` alone replaced its container before the chain ran, and a
  # refused migration on 2026-10-14 left it Created for 30 minutes.
  local in_order="${release_root}/current/scripts/deploy/runtime-up-in-order.sh"
  [[ -r "${in_order}" ]] || {
    printf 'runtime ordering helper is missing from the release: %s\n' "${in_order}" >&2
    exit 66
  }
  # shellcheck source=runtime-up-in-order.sh
  source "${in_order}"
  # The chain's own time (root ADR-0308 postgres-chain budget): from its first `up` to its last.
  local started="${SECONDS}"
  runtime_chain_before "${runtime}" foundation-api
  # `foundation-api` is the last link: requiring it requires all of them, in the compose file's
  # order. `--wait` returns once the state is reached, not when the request was accepted.
  "${runtime}" up -d --wait foundation-api
  # It runs on every deploy and says when it took longer than its budget (root ADR-0308).
  [[ ! -f "${steps}" ]] \
    || python3 -I -B "${steps}" budget "${release_root}/current" postgres-chain "$((SECONDS - started))" || true
  # The identity loopback sidecars are asked for separately, and not redundantly: they depend
  # on foundation-api, and compose only pulls dependencies in the direction of the arrow —
  # asking for the api alone leaves its own loopback listeners down, which surfaced on the
  # first identity bring-up as policy calls dying inside a healthy-looking container.
  "${runtime}" up -d --wait identity-zitadel-loopback identity-api-loopback
  # Prometheus and Alertmanager bind their rule and route files through `current`, and Docker
  # resolves that link when it creates a container: a release that changes an alert rule or a
  # route is otherwise never read (the compose service itself did not change, so `up` keeps the
  # old container on the old release's files). Their data and the notification log live in
  # volumes, so a recreate loses neither, and a firing alert is restored, not sent again.
  "${runtime}" up -d --wait --force-recreate prometheus alertmanager
  "${runtime}" up -d --wait node-exporter # serves the deploy's textfile (root ADR-0321 §5)
  verify_runtime_schema
  migrate_lakehouse "${lakehouse}"
}

# The lakehouse half of `migrate` (root ADR-0124): every published lakehouse table is brought to
# the release's contracts -- columns added in contract order, order fixed by metadata, registered
# backfills run -- and anything that cannot be (a column the contract does not declare, a required
# column with no backfill) stops the deploy here, the way a failed database migration does.
# Temporary compatibility exceptions are owned and reported by the migration job (ADR-0124).
# What ages out on the host ships with the release (root ADR-0198): every file under
# infra/host/<directory>/ is installed into the /etc directory of the same name, so the rule lives
# once, in the repository, and a deploy cannot leave an older copy behind. Scratch is cleaned
# right away rather than at the OS's next daily tmpfiles run, and journald is restarted only when
# its drop-in changed.
# The drop-ins' digest, also when there are none yet: the first deploy finds the directory empty,
# and `cat <dir>/*.conf` then fails, which under pipefail stopped `timers` (2026-10-13).
journald_dropins_digest() {
  local -a dropins=()
  local file
  for file in /etc/systemd/journald.conf.d/*.conf; do
    [[ -f "${file}" ]] && dropins+=("${file}")
  done
  if ((${#dropins[@]})); then cat -- "${dropins[@]}"; fi | sha256sum
}

install_host_retention() {
  local host="${release_root}/current/infra/host" file journald_before journald_after
  install -d -o root -g root -m 0755 /etc/tmpfiles.d /etc/systemd/journald.conf.d
  journald_before="$(journald_dropins_digest)"
  for file in "${host}"/tmpfiles.d/*.conf; do
    install -o root -g root -m 0644 -t /etc/tmpfiles.d "${file}"
    systemd-tmpfiles --clean "/etc/tmpfiles.d/${file##*/}"
  done
  for file in "${host}"/journald.conf.d/*.conf; do
    install -o root -g root -m 0644 -t /etc/systemd/journald.conf.d "${file}"
  done
  journald_after="$(journald_dropins_digest)"
  [[ "${journald_before}" == "${journald_after}" ]] || systemctl restart systemd-journald
}

# Every Spark container's /tmp (config/host-layout.contract.json spark_tmp): its spill stays off
# the system disk. 1777: the container's uid is 185 or the caller's, and each run owns its files.
# install -d also resets a directory Docker made first, as root 0755, when a container mounted it.
prepare_spark_tmp() {
  install -d -m 1777 "$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["spark_tmp"])' \
    "${release_root}/current/config/host-layout.contract.json")"
}

# The job state directories, each with its owner and mode, before any unit or container uses one
# (orchestration/jobs.v1.json state_directories, rendered by foundation-jobs; root ADR-0270 §1). On
# 2026-10-15 a directory made only in `timers` was mounted by `migrate` first, Docker made it root
# 0755, and every deploy stopped (#625). `d` makes a missing directory and resets the owner and mode
# of one that exists; the file also goes to /etc/tmpfiles.d, so a reboot makes them again. A release
# from before the file made them in `timers` itself. The argument is the release to read them from
# (activate passes the one it is about to switch to); the current one when absent.
prepare_job_state_directories() {
  local release="${1:-${release_root}/current}"
  local conf="${release}/infra/tmpfiles/foundation-job-state.conf"
  local accounts name ids
  [[ -f "${conf}" ]] || return 0
  # The accounts the directories name first, each a container's uid (infra/sysusers): sysusers picks
  # another uid when the asked one is taken; then the container could not write what it owns, so a
  # uid other than the one its file states stops the deploy here instead.
  install -d -o root -g root -m 0755 /etc/sysusers.d /etc/tmpfiles.d
  for accounts in "${release}"/infra/sysusers/*.conf; do
    install -o root -g root -m 0644 -t /etc/sysusers.d "${accounts}"
    systemd-sysusers "/etc/sysusers.d/${accounts##*/}"
    read -r name ids < <(awk '$1 == "u" { print $2, $3; exit }' "${accounts}")
    [[ "$(id -u "${name}"):$(id -g "${name}")" == "${ids}" ]] || {
      printf '%s is not uid:gid %s (%s); its directories were not made\n' "${name}" "${ids}" "${accounts##*/}" >&2
      exit 65
    }
  done
  install -o root -g root -m 0644 -t /etc/tmpfiles.d "${conf}"
  systemd-tmpfiles --create /etc/tmpfiles.d/foundation-job-state.conf
}

install_lakehouse_migration_unit() {
  install -o root -g root -m 0644 -t /etc/systemd/system \
    "${release_root}/current/infra/systemd/foundation-lakehouse-migrate.service"
  install_release_admission foundation-lakehouse-migrate.service
  install -d -o foundation-platform -g foundation-platform /var/lib/foundation-platform/lakehouse-migrate
}

# The records of the deploy steps (config/deploy-steps.contract.json state_directory, root ADR-0308):
# root's, 0700. deploy_steps.py refuses them under a parent another account could write.
prepare_deploy_steps_state() {
  install -d -o root -g root -m 0700 "$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["state_directory"])' \
    "${release_root}/current/config/deploy-steps.contract.json")"
}

# <run|skip-unchanged>: skip-unchanged (the deploy) skips it while what it runs against is what its
# last successful run here recorded (root ADR-0308: inputs, catalog settings, jars, and no drift in
# shape_ledger_check's capture); only exit 3 skips, anything else runs it. `run` (by hand) always runs.
# Its record goes before any run and comes back only after a successful one, so a run that fails or
# is cut short is never taken for the state of the catalog.
migrate_lakehouse() {
  local mode="$1" steps="${release_root}/current/scripts/deploy/deploy_steps.py" gate=0
  assert_current_release
  if [[ -f "${steps}" ]]; then
    prepare_deploy_steps_state
    if [[ "${mode}" == skip-unchanged ]]; then
      python3 -I -B "${steps}" should-run "${release_root}/current" lakehouse-migrate || gate=$?
      [[ "${gate}" != 3 ]] || return 0
    fi
    python3 -I -B "${steps}" forget "${release_root}/current" lakehouse-migrate
  fi
  install_lakehouse_migration_unit
  # The migration's lakehouse-target-init checks /tmp is writable; it is made before, not in `timers`.
  prepare_spark_tmp
  systemctl daemon-reload
  # A oneshot's start returns when it has finished, with its result.
  if systemctl start foundation-lakehouse-migrate.service; then
    journalctl -u foundation-lakehouse-migrate.service --since "-2h" --no-pager -o cat |
      grep -a '^lakehouse-migrate apply:' | tail -1
    # A record that cannot be written only means the next deploy runs it again.
    [[ ! -f "${steps}" ]] \
      || python3 -I -B "${steps}" ran "${release_root}/current" lakehouse-migrate || true
  else
    journalctl -u foundation-lakehouse-migrate.service --since "-2h" --no-pager -o cat |
      grep -a '^lakehouse-migrate' | tail -40 >&2
    printf 'lakehouse migration failed; the release is installed but not migrated\n' >&2
    exit 75
  fi
}

# The data catalog's containers follow the release (root ADR-0117 §7): DataHub is its own Compose
# project, run by datahub-runtime.sh from secrets only root reads, so a merged change to
# infra/datahub/compose.yml used to wait for someone with sudo (2026-10-16, #628). This recreates
# the services whose rendered config differs from their container's (the hash Compose itself
# compares). Compose keeps the order it owns: a changed GMS brings its system-update one-shot
# first, as DataHub's own `up` does (the-deploy-does-not-restate-the-compose-chain). Nothing
# changed: nothing is touched. datahub-runtime.sh waits until the UI and GMS answer.
apply_datahub() {
  assert_current_release
  local runtime="${release_root}/current/scripts/deploy/datahub-runtime.sh" work changed
  # The state is the contract's datahub.state_dir; a host installed elsewhere is adopted once.
  python3 -I -B "${release_root}/current/scripts/deploy/datahub_state.py" adopt
  work="$(mktemp -d)"
  "${runtime}" config --hash '*' >"${work}/config"
  docker ps -a --filter label=com.docker.compose.project=datahub     --format '{{.Label "com.docker.compose.service"}} {{.Label "com.docker.compose.config-hash"}}' >"${work}/running"
  changed="$(python3 -I -B "${release_root}/current/scripts/deploy/datahub_changes.py" "${work}/config" "${work}/running")"
  rm -rf "${work}"
  if [[ -z "${changed}" ]]; then
    printf 'datahub: every service already runs its rendered config
'
    return 0
  fi
  printf 'datahub: recreating %s
' "$(tr '
' ' ' <<<"${changed}")"
  # shellcheck disable=SC2086 # one service name per word
  "${runtime}" up -d ${changed}
}

# The staging smoke (root ADR-0177): the installed, not yet current, release runs a few real inputs
# under the staging namespace before the switch. scripts/ops/staging-smoke.sh does the work, as the
# service account, in a transient unit that loads exactly the environment files the runtime-secrets
# contract gives the run `staging-smoke` and is bounded by config/staging-gate.contract.json; its
# exit status is this step's. The off switch skips it and says so; nothing else does.
staging_smoke_off_switch=/etc/foundation-platform/staging-gate.off
staging_smoke() {
  local release_id="$1" target limit memory properties
  require_release_id "${release_id}"
  if [[ -e "${staging_smoke_off_switch}" ]]; then
    printf 'staging smoke: off (%s exists); %s goes on without it\n' "${staging_smoke_off_switch}" "${release_id}"
    return 0
  fi
  assert_installed_release "${release_id}"
  target="$(release_path "${release_id}")"
  [[ -x "${target}/scripts/ops/staging-smoke.sh" ]] || {
    printf 'staging smoke is missing from the release: %s\n' "${target}/scripts/ops/staging-smoke.sh" >&2
    exit 66
  }
  limit="$(python3 -I -c 'import json, sys; print(int(json.load(open(sys.argv[1]))["unit_limit_seconds"]))' \
    "${target}/config/staging-gate.contract.json")"
  memory="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["unit_memory_max"])' \
    "${target}/config/staging-gate.contract.json")"
  properties="$(python3 -I "${target}/scripts/deploy/runtime_secrets.py" --area "${target}" properties staging-smoke)"
  install -d -o foundation-platform -g foundation-platform /data/foundation-platform/staging-smoke
  # shellcheck disable=SC2086 # the properties are `-p EnvironmentFile=<path>` words
  systemd-run --wait --collect --pipe --quiet --unit=foundation-staging-smoke \
    -p User=foundation-platform -p Group=foundation-platform -p WorkingDirectory="${target}" \
    -p RuntimeMaxSec="${limit}" -p MemoryMax="${memory}" ${properties} \
    "${target}/scripts/ops/staging-smoke.sh"
}

status() {
  printf 'release_root=%s\n' "${release_root}"
  printf 'state_root=%s\n' "${state_root}"
  printf 'current=%s\n' "$(readlink "${release_root}/current" 2>/dev/null || true)"
  printf 'previous=%s\n' "$(readlink "${release_root}/previous" 2>/dev/null || true)"
}

command="${1:-}"
case "${command}" in
  prepare)
    [[ "$#" == 3 ]] || usage
    install_release "$2" "$3" no
    ;;
  floor-config)
    [[ "$#" == 3 ]] || usage
    install_floor_config "$2" "$3"
    ;;
  install)
    [[ "$#" == 3 ]] || usage
    install_release "$2" "$3"
    # A deploy that leaves the running schema behind has not finished, and saying otherwise is
    # what let the runtime sit six weeks and twenty-nine migrations behind while three deploys
    # reported success. The release is installed and activated by this point, so the operator
    # can run `migrate` — this refuses to call the deploy done, it does not undo it.
    verify_runtime_schema
    ;;
  activate)
    [[ "$#" == 2 ]] || usage
    require_release_id "$2"
    mkdir -p "${releases_dir}" "${state_root}/recovery"
    prepare_mutable_state
    activate_and_prune "$2"
    ;;
  migrate)
    # By hand it runs every step: the operator's way to bring the lakehouse to the contracts again
    # (after a catalog restore, docs/runbooks/iceberg-snapshot-rollback.md). The deploy passes
    # --skip-unchanged (root ADR-0308).
    case "$#:${2:-}" in
      1:) migrate_runtime run ;;
      2:--skip-unchanged) migrate_runtime skip-unchanged ;;
      *) usage ;;
    esac
    ;;
  staging-smoke)
    [[ "$#" == 2 ]] || usage
    staging_smoke "$2"
    ;;
  verify)
    [[ "$#" == 1 ]] || usage
    verify_runtime_schema
    ;;
  datahub)
    [[ "$#" == 1 ]] || usage
    apply_datahub
    ;;
  timers)
    # The release installs and enables its own systemd timers (root ADR-0077). The unit files
    # ship inside the release, so "which timers exist" has exactly one home; installing them by
    # hand is how the backup timer's install steps and the deployed tree drifted apart before.
    # Idempotent: reinstalling the same files and re-enabling an enabled timer are no-ops.
    [[ "$#" == 1 || "$#" == 2 ]] || usage
    assert_current_release
    # No unit is installed whose environment files the host does not have (root ADR-0153).
    verify_runtime_secrets
    # FLOOR's unit is installed only with a valid FLOOR config and state, and nothing else waits on
    # them: an invalid FLOOR config used to stop this step before it installed any unit, retired
    # any timer, regenerated the scheduler's sudoers or enabled the backup timer.
    floor_unit=foundation-building-register-floor.service
    floor_status=0
    prepare_floor_state || floor_status=$?
    # Every unit the release ships, not a list of them here: a job's service added to infra/systemd
    # is installed by the same release that lists it in orchestration/jobs.v1.json.
    for unit in "${release_root}"/current/infra/systemd/*.service \
      "${release_root}"/current/infra/systemd/*.timer "${release_root}"/current/infra/systemd/*.path; do
      [[ "${unit##*/}" != "${floor_unit}" || "${floor_status}" == 0 ]] || continue
      install -o root -g root -m 0644 -t /etc/systemd/system "${unit}"
    done
    # The fold was one complex-only unit before it became a per-unit template; a host upgraded
    # from then still carries it, and it would fold complex a second time every hour.
    if [[ -e /etc/systemd/system/foundation-map-edit-fold.timer ]]; then
      systemctl disable --now foundation-map-edit-fold.timer || true
      rm -f /etc/systemd/system/foundation-map-edit-fold.timer \
        /etc/systemd/system/foundation-map-edit-fold.service
    fi
    # The parcel tile bake was one unit before it became the gold/bake template (root ADR-0253). It
    # never ran (its job was off) and has no timer; a host that installed it would keep a unit no
    # job names.
    rm -f /etc/systemd/system/foundation-parcel-tile-bake.service
    rm -rf /etc/systemd/system/foundation-parcel-tile-bake.service.d
    # Likewise the building serving monitor became the building instance of the by-PNU monitor
    # template (root ADR-0160). A host whose building timer was on keeps the hourly read under the
    # new name (enabled after the reload below); one where it was off keeps it off.
    building_monitor_was_on=no
    if [[ -e /etc/systemd/system/foundation-building-serving-monitor.timer ]]; then
      ! systemctl is-enabled --quiet foundation-building-serving-monitor.timer || building_monitor_was_on=yes
      systemctl disable --now foundation-building-serving-monitor.timer || true
      rm -f /etc/systemd/system/foundation-building-serving-monitor.timer \
        /etc/systemd/system/foundation-building-serving-monitor.service
    fi
    # A job Airflow runs (orchestration/jobs.v1.json `enabled`) loses its timer in the release that
    # switches its DAG on (root ADR-0122 §4), so it runs in exactly one place. Its service stays:
    # systemd still runs it, Airflow only starts it.
    jobs_file="${release_root}/current/orchestration/jobs.v1.json"
    # systemd owns every scheduled entry point, including a direct `systemctl start`; a check in
    # the SSH dispatcher alone would leave that path open. The migration unit is not scheduled,
    # so it is named here too.
    install_lakehouse_migration_unit
    while read -r service; do
      [[ -n "${service}" ]] || continue
      install_release_admission "${service}"
    done < <(python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" services "${jobs_file}")
    while read -r timer; do
      [[ -n "${timer}" ]] || continue
      if [[ -e "/etc/systemd/system/${timer}" ]]; then
        systemctl disable --now "${timer}" || true
        rm -f "/etc/systemd/system/${timer}"
        printf 'timer retired to airflow: %s\n' "${timer}"
      fi
    done < <(python3 -c '
import json, sys
for job in json.load(open(sys.argv[1]))["jobs"]:
    if job["enabled"]:
        print(job["systemd_timer"])
' "${jobs_file}")
    install_scheduler_access "${jobs_file}" "${2:-}"
    # The job state directories the units name in ReadWritePaths and the containers mount: one
    # generated file, applied here too so a `timers` run on its own (recovery) makes them as well.
    prepare_job_state_directories
    prepare_spark_tmp
    install_host_retention
    # The service checks' file, served read-only by foundation-api (compose.orchestration-bridge.yml),
    # and their kept CI runs (root ADR-0209). Its unit names the path in ReadWritePaths.
    install -d -o foundation-platform -g foundation-platform -m 0755 /var/lib/foundation-platform/service-checks
    systemctl daemon-reload
    [[ "${building_monitor_was_on}" == no ]] \
      || systemctl enable --now foundation-by-pnu-serving-monitor@building.timer
    # The database backup is host infrastructure, not a data job: it stays a systemd timer
    # (root ADR-0118 §1). Every data job is started by Airflow (root ADR-0122).
    systemctl enable --now foundation-postgres-backup.timer
    # The PostgreSQL backup's age check: host infrastructure too.
    systemctl enable --now foundation-postgres-backup-freshness.timer
    # The identity databases' backup, its age check and its weekly restore drill: host infrastructure too.
    systemctl enable --now foundation-identity-backup.timer foundation-identity-backup-freshness.timer \
      foundation-identity-restore-drill.timer
    # Like the backup, the root disk's retention (root ADR-0198) and the by-PNU edge probe (ADR-0327) are host infrastructure.
    systemctl enable --now foundation-host-retention.timer foundation-by-pnu-edge-probe.timer
    # The operations map's service checks (root ADR-0209): a host check too.
    systemctl enable --now foundation-service-checks.timer
    # An operator's run-now request is started when foundation-api writes it (root ADR-0282 §4).
    systemctl enable --now foundation-run-now.path
    # The host deploys main by itself once it names its deployer (root ADR-0159); a host without
    # that file is deployed by hand, and its timer stays off. Deploying is not a data job either.
    autodeploy=off
    if [[ -r /etc/foundation-platform/release-deploy.conf ]]; then
      systemctl enable --now foundation-autodeploy.timer
      autodeploy="$(systemctl is-enabled foundation-autodeploy.timer)"
    fi
    if [[ "${floor_status}" != 0 ]]; then
      printf 'timers incomplete: %s was not installed; its FLOOR config or state is invalid\n' \
        "${floor_unit}" >&2
      exit 65
    fi
    printf 'timers-ok backup=%s identity-backup=%s autodeploy=%s\n' \
      "$(systemctl is-enabled foundation-postgres-backup.timer)" \
      "$(systemctl is-enabled foundation-identity-backup.timer)" "${autodeploy}"
    ;;
  rollback)
    [[ "$#" == 1 ]] || usage
    rollback_release
    ;;
  prune)
    [[ "$#" == 1 ]] || usage
    prune_releases
    ;;
  host-retention)
    # The host-retention part of `timers` on its own: re-applies what ages out on the host from
    # the current release (root ADR-0198), and is what the rehearsal drives.
    [[ "$#" == 1 ]] || usage
    assert_current_release
    install_host_retention
    ;;
  deployer-access)
    [[ "$#" == 2 || "$#" == 3 ]] || usage
    install_deployer_access "$2" "${3:-}"
    ;;
  operator-access)
    [[ "$#" == 2 ]] || usage
    install_operator_access "$2"
    ;;
  env-set)
    # A host environment file changes through the contract, not an editor (root ADR-0247): the
    # names must be ones the group holds, and the file keeps the contract's owner, group and mode.
    # Prints names, never values; the caller removes its source file.
    [[ "$#" == 3 ]] || usage
    assert_current_release
    python3 -I "${release_root}/current/scripts/deploy/runtime_secrets.py" --area "${release_root}/current" \
      set --root "${FOUNDATION_PLATFORM_SECRETS_ROOT:-/}" "$2" "$3"
    ;;
  verify-registry)
    # Stage R2 of root ADR-0276: verify the release CI built and signed for one commit, pulled from
    # GitHub's container registry by digest and checked offline; nothing is installed or tagged.
    [[ "$#" == 2 ]] || usage
    require_release_id "$2"
    "${admission}" verify-registry "$2"
    ;;
  status)
    [[ "$#" == 1 ]] || usage
    status
    ;;
  *) usage ;;
esac
