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
    ;;
  activate)
    [[ "$#" == 2 ]] || usage
    require_release_id "$2"
    mkdir -p "${releases_dir}" "${state_root}/recovery"
    prepare_mutable_state
    activate_release "$2"
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
