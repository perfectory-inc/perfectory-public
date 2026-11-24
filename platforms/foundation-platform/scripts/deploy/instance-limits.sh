#!/usr/bin/env bash
# Sourced by foundation-release.sh install_release_units (it provides release_root).
# An instance of a template that holds less than the template gets its own time limit and memory
# drop-ins (root ADR-0574 §8), generated from its job by foundation-jobs and scripts/ci/unit_memory.py;
# a service whose job states open_files gets its open-files drop-in (foundation-jobs), instance or not.
# Installed as the release ships them, and ours that the release no longer ships are removed, so an
# instance whose job now holds the template's limit goes back to it; a drop-in we did not write stays.
# shellcheck disable=SC2154 # release_root is set by foundation-release.sh, which sources this file
install_instance_limits() {
  local shipped=" " dropin dir name
  for dropin in "${release_root}"/current/infra/systemd/foundation-*@*.service.d/50-time-limit.conf \
    "${release_root}"/current/infra/systemd/foundation-*@*.service.d/50-memory.conf \
    "${release_root}"/current/infra/systemd/foundation-*.service.d/50-open-files.conf; do
    [[ -f "${dropin}" ]] || continue
    dir="${dropin%/*}"
    dir="${dir##*/}"
    name="${dropin##*/}"
    install -d -o root -g root -m 0755 "/etc/systemd/system/${dir}"
    install -o root -g root -m 0644 "${dropin}" "/etc/systemd/system/${dir}/${name}"
    shipped+="${dir}/${name} "
  done
  for dropin in /etc/systemd/system/foundation-*@*.service.d/50-time-limit.conf \
    /etc/systemd/system/foundation-*@*.service.d/50-memory.conf \
    /etc/systemd/system/foundation-*.service.d/50-open-files.conf; do
    [[ -f "${dropin}" ]] || continue
    dir="${dropin%/*}"
    name="${dir##*/}/${dropin##*/}"
    [[ "${shipped}" == *" ${name} "* ]] && continue
    rm -f "${dropin}"
    rmdir --ignore-fail-on-non-empty "${dir}"
  done
}
