#!/usr/bin/env bash
set -Eeuo pipefail

release_root="${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform}"
state_root="${FOUNDATION_PLATFORM_STATE_ROOT:-/var/lib/foundation-platform}"
releases_dir="${release_root}/releases"
lakehouse_uid="${FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}"
lakehouse_gid="${FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}"

usage() {
  cat >&2 <<'USAGE'
usage:
  foundation-release.sh install <40-char-git-sha> <source.tar.gz>
  foundation-release.sh activate <40-char-git-sha>
  foundation-release.sh migrate
  foundation-release.sh verify
  foundation-release.sh timers
  foundation-release.sh rollback
  foundation-release.sh status
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

  if [[ -n "${current_target}" && "${current_target}" != "${next_target}" ]]; then
    atomic_link "${current_target}" "${release_root}/previous"
  fi
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
    assert_installed_release "${release_id}"
    recorded_sha="$(cat "${target}/.foundation-release-archive-sha256")"
    [[ "${recorded_sha}" == "${archive_sha}" ]] || {
      printf 'release id already exists with a different archive: %s\n' "${release_id}" >&2
      exit 65
    }
    activate_release "${release_id}"
    return
  fi

  staging="$(mktemp -d "${releases_dir}/.${release_id}.tmp.XXXXXX")"
  trap '[[ -z "${staging:-}" ]] || rm -rf "${staging}"' RETURN
  tar --no-same-owner --no-same-permissions -xzf "${archive}" -C "${staging}"
  chmod 0755 "${staging}"
  printf '%s\n' "${release_id}" >"${staging}/.foundation-release-id"
  printf '%s\n' "${archive_sha}" >"${staging}/.foundation-release-archive-sha256"
  mv "${staging}" "${target}"
  staging=""
  trap - RETURN
  activate_release "${release_id}"
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

verify_runtime_schema() {
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
  "${checker}" "${release_root}/current"
}

migrate_runtime() {
  # Rebuild before applying. The migrator is the release's own binary and the migrations live
  # inside it, so an image built from an older tree applies an older set however new the source
  # on disk is — which is exactly how the runtime came to be six weeks behind.
  local runtime="${release_root}/current/scripts/deploy/foundation-runtime.sh"
  [[ -x "${runtime}" ]] || {
    printf 'runtime compose wrapper is missing from the release: %s\n' "${runtime}" >&2
    exit 66
  }
  "${runtime}" build foundation-api
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
  # `foundation-api` is the last link, so requiring it requires all of them, in the order the
  # compose file owns. `--wait` makes the command finish when the state is reached rather than
  # when the request was accepted.
  "${runtime}" up -d --wait foundation-api
  verify_runtime_schema
}

status() {
  printf 'release_root=%s\n' "${release_root}"
  printf 'state_root=%s\n' "${state_root}"
  printf 'current=%s\n' "$(readlink "${release_root}/current" 2>/dev/null || true)"
  printf 'previous=%s\n' "$(readlink "${release_root}/previous" 2>/dev/null || true)"
}

command="${1:-}"
case "${command}" in
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
    activate_release "$2"
    ;;
  migrate)
    [[ "$#" == 1 ]] || usage
    migrate_runtime
    ;;
  verify)
    [[ "$#" == 1 ]] || usage
    verify_runtime_schema
    ;;
  timers)
    # The release installs and enables its own systemd timers (root ADR-0077). The unit files
    # ship inside the release, so "which timers exist" has exactly one home; installing them by
    # hand is how the backup timer's install steps and the deployed tree drifted apart before.
    # Idempotent: reinstalling the same files and re-enabling an enabled timer are no-ops.
    [[ "$#" == 1 ]] || usage
    install -o root -g root -m 0644 \
      -t /etc/systemd/system \
      "${release_root}/current/infra/systemd/foundation-postgres-backup.service" \
      "${release_root}/current/infra/systemd/foundation-postgres-backup.timer" \
      "${release_root}/current/infra/systemd/foundation-source-sweep.service" \
      "${release_root}/current/infra/systemd/foundation-source-sweep.timer"
    systemctl daemon-reload
    systemctl enable --now foundation-postgres-backup.timer foundation-source-sweep.timer
    # A fresh timer that has never fired is unproven (the backup timer's own rule), so kick the
    # sweep once now, non-blocking. The sweep skips already-held files by fingerprint, so an
    # extra run is idempotent by construction.
    systemctl start --no-block foundation-source-sweep.service
    printf 'timers-ok backup=%s sweep=%s\n' \
      "$(systemctl is-enabled foundation-postgres-backup.timer)" \
      "$(systemctl is-enabled foundation-source-sweep.timer)"
    ;;
  rollback)
    [[ "$#" == 1 ]] || usage
    rollback_release
    ;;
  status)
    [[ "$#" == 1 ]] || usage
    status
    ;;
  *) usage ;;
esac
