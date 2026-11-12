#!/usr/bin/env bash
# Applies the host configuration a release installs (tmpfiles, units) to a throwaway root shaped like
# the host, with the host's own systemd tools, before the change merges (root ADR-0555).
#
#   sudo host-config-check.sh [<platform dir>]        (Linux with systemd; run as root)
#
# 2026-11-11 15:34Z: #1256 added `/data/foundation-platform/intelligence-documents/lakehouse` to the
# job state directories. systemd-tmpfiles refused to step from the operator's directory into ours
# ("unsafe path transition", exit 73) and every deploy failed for 6.5 hours. No check had run the
# file through systemd-tmpfiles: the first run was the production deploy.
#
# What runs here is what scripts/deploy/foundation-release.sh runs on the host, against `--root`:
#   - systemd-tmpfiles --create on infra/tmpfiles/*.conf (prepare_job_state_directories), twice: the
#     second run meets the directories the first made, as every deploy after the first does;
#   - systemd-tmpfiles --create and --clean on infra/host/tmpfiles.d/*.conf;
#   - systemd-analyze verify on every unit of infra/systemd. A command that is not there (the release
#     tree and Docker are not in the throwaway root) is not a finding; anything else it says is.
# The root starts as the host is (measured 2026-11-12, ai-server, Ubuntu 24.04, systemd 255.4, the
# version of GitHub's ubuntu-24.04 runner): the directories and owners in host-layout.measured.
set -euo pipefail

platform="$(cd "${1:-$(dirname "$0")/../..}" && pwd)"
layout="${platform}/scripts/deploy/host-layout.measured"
fail() { printf 'FAIL host-config-check: %s\n' "$*" >&2; exit 1; }

[[ "$(id -u)" == 0 ]] || fail "run as root (sudo): the check owns directories as other accounts"
for tool in systemd-tmpfiles systemd-analyze; do
  command -v "${tool}" >/dev/null || fail "missing ${tool}"
done
[[ -f "${layout}" ]] || fail "missing ${layout}"

root="$(mktemp -d)"
trap 'rm -rf -- "${root}"' EXIT
install -d -m 0755 "${root}/etc"

# Accounts: the uid a sysusers file states, the operator's measured uid, and a number of its own for
# every other owner a tmpfiles line or the layout names.
declare -A uid=([root]=0)
next=20001
account() {
  local name="$1" id="${2:-}"
  [[ -z "${uid[${name}]:-}" ]] || return 0
  if [[ -z "${id}" ]]; then id="${next}"; next=$((next + 1)); fi
  uid["${name}"]="${id}"
}
for file in "${platform}"/infra/sysusers/*.conf; do
  while read -r kind name ids _; do
    [[ "${kind}" == u ]] && account "${name}" "${ids%%:*}"
  done <"${file}"
done
while read -r path mode owner id; do
  [[ -n "${path}" && "${path}" != \#* ]] || continue
  account "${owner}" "${id:-}"
done <"${layout}"
confs=("${platform}"/infra/tmpfiles/*.conf "${platform}"/infra/host/tmpfiles.d/*.conf)
for file in "${confs[@]}"; do
  while read -r kind _ _ owner group _; do
    [[ "${kind}" =~ ^[a-zA-Z] ]] || continue
    for name in "${owner}" "${group}"; do
      [[ -z "${name}" || "${name}" == - || "${name}" =~ ^[0-9]+$ ]] || account "${name}"
    done
  done <"${file}"
done
: >"${root}/etc/passwd"; : >"${root}/etc/group"
for name in "${!uid[@]}"; do
  printf '%s:x:%s:%s::/:/usr/sbin/nologin\n' "${name}" "${uid[${name}]}" "${uid[${name}]}" >>"${root}/etc/passwd"
  printf '%s:x:%s:\n' "${name}" "${uid[${name}]}" >>"${root}/etc/group"
done

# The host as it is before the release touches it.
while read -r path mode owner _; do
  [[ -n "${path}" && "${path}" != \#* ]] || continue
  install -d -m "${mode}" -o "${uid[${owner}]}" -g "${uid[${owner}]}" "${root}${path}"
done <"${layout}"

status=0
apply() {
  local label="$1"; shift
  if ! output="$(systemd-tmpfiles --root="${root}" "$@" 2>&1)"; then
    printf 'FAIL host-config-check: %s: systemd-tmpfiles exited non-zero, as it would on the host\n%s\n' \
      "${label}" "${output//${root}/}" >&2
    status=1
  fi
}
for file in "${platform}"/infra/tmpfiles/*.conf; do
  apply "${file##*/} (first deploy)" --create "${file}"
  apply "${file##*/} (a later deploy)" --create "${file}"
done
for file in "${platform}"/infra/host/tmpfiles.d/*.conf; do
  apply "${file##*/}" --create "${file}"
  apply "${file##*/} (clean)" --clean "${file}"
done

# Units: installed as the release installs them (root 0644), then read by systemd itself.
units="${root}/etc/systemd/system"
install -d -m 0755 "${units}"
shopt -s nullglob
for file in "${platform}"/infra/systemd/*.service "${platform}"/infra/systemd/*.timer "${platform}"/infra/systemd/*.path; do
  install -m 0644 "${file}" "${units}/"
done
shopt -u nullglob
findings="$(cd "${units}" && systemd-analyze --root="${root}" verify --recursive-errors=no ./* 2>&1 \
  | grep -v -e 'is not executable: No such file or directory' -e '^$' || true)"
if [[ -n "${findings}" ]]; then
  printf 'FAIL host-config-check: systemd-analyze verify:\n%s\n' "${findings//${root}/}" >&2
  status=1
fi

[[ "${status}" == 0 ]] || exit 1
printf 'OK host-config-check: %s tmpfiles file(s) and %s unit(s) apply on a root shaped like the host\n' \
  "${#confs[@]}" "$(find "${units}" -maxdepth 1 -type f | wc -l)"
