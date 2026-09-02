#!/usr/bin/env bash
# Fail when the runtime environment file lacks a variable this release's compose files require.
#
# What this prevents: on 2026-09-01, repairing a six-week schema gap stopped here. The compose
# files had gained a Postgres-to-R2 backup writer, `/etc/foundation-platform/recovery.env` had
# never been updated, and the failure surfaced as a compose interpolation error in the middle of
# a migration run — after the image rebuild, with nothing having said beforehand that the file
# was behind. It is the same drift as the schema one, one file over.
#
# **It reads names, never values.** The required set comes from `${VAR:?...}` in the compose
# files, and presence is decided by a `^NAME=` match. Nothing here prints, compares, or copies a
# secret; a check that had to read them would be a second place they live.
#
# **It fails when it cannot look.** A missing compose file, an unreadable env file, an empty
# required set: each is a question that went unanswered, and the whole family of defects this
# repository keeps finding is an unanswered question recorded as a pass.
set -uo pipefail

release_dir="${1:-${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform}/current}"
env_file="${2:-${FOUNDATION_PLATFORM_ENV_FILE:-/etc/foundation-platform/recovery.env}}"

fail() {
  printf 'FAIL runtime-environment: %s\n' "$1" >&2
  exit 1
}

# Which compose files the runtime loads is `foundation-runtime.sh`'s `-f` arguments, and that is
# where it is read from. Listing them here was a second copy: a third compose file would join the
# runtime and this check would keep asking about two, reporting complete coverage of a set it no
# longer covers.
runtime_script="${release_dir}/scripts/deploy/foundation-runtime.sh"
[[ -f "${runtime_script}" ]] || fail "release has no runtime compose wrapper: ${runtime_script}"

compose_files=()
while read -r file; do
  [[ -n "${file}" ]] && compose_files+=("${release_dir}/${file}")
done < <(
  sed -nE 's|^[[:space:]]*-f "\$\{root_dir\}/([^"]+)".*|\1|p' "${runtime_script}"
)
[[ "${#compose_files[@]}" -gt 0 ]] \
  || fail "read no compose files out of ${runtime_script}, which cannot be right"

for file in "${compose_files[@]}"; do
  [[ -f "${file}" ]] || fail "release is missing a compose file: ${file}"
done

# Only the variables compose itself calls required. A variable with a default is a choice the
# deployment may leave alone, and demanding it here would make this check refuse working hosts.
required="$(
  grep -ohE '\$\{[A-Z_][A-Z0-9_]*:\?' "${compose_files[@]}" \
    | tr -d '${:?' | sort -u
)"
required_count="$(printf '%s\n' "${required}" | grep -c '[A-Z]')"
[[ "${required_count}" -gt 0 ]] || fail "found no required variables in the compose files, which cannot be right"

[[ -r "${env_file}" ]] || fail "cannot read the runtime environment file: ${env_file}"

missing=""
while IFS= read -r name; do
  [[ -n "${name}" ]] || continue
  grep -qE "^${name}=" "${env_file}" || missing="${missing}${name}"$'\n'
done <<<"${required}"

if [[ -n "${missing//[$'\n' ]/}" ]]; then
  printf 'FAIL runtime-environment: %s required variable(s) are absent from %s\n' \
    "$(printf '%s\n' "${missing}" | grep -c '[A-Z]')" "${env_file}" >&2
  printf '%s' "${missing}" | sed 's/^/  missing /' >&2
  printf '  Compose refuses to start without these, and the refusal surfaces mid-run as an\n' >&2
  printf '  interpolation error. Add them to the environment file; this check never reads values.\n' >&2
  exit 1
fi

printf 'OK runtime-environment: %s required variable(s), all present\n' "${required_count}"
