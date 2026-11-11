#!/usr/bin/env bash
# Sourced by foundation-release.sh `trino` (it provides release_root and assert_current_release).
# Trino follows the release (root ADR-0504). Its container mounts files of the release it was
# created from and nothing recreated it on deploy, so a merged memory limit waited for the next crash
# (2026-11-11: the kernel killed Trino at 16.55GB under the old limits). This recreates it when what it
# runs differs from this release: the compose config (the hash Compose itself compares; the catalog
# directory from config/host-layout.contract.json reproduces the running label) or the bytes of a
# file it mounts from infra/lakehouse/trino. Same: nothing is touched. No container: nothing to follow.
trino_container=foundation-platform-trino
trino_compose() {
  local current="${release_root}/current"
  FOUNDATION_PLATFORM_TRINO_CATALOG_DIR="$(host_layout_value trino_catalog_dir)" \
  FOUNDATION_PLATFORM_TRINO_SPILL="$(host_layout_value trino_spill)" \
    docker compose -p foundation-platform-compute --project-directory "${current}" \
      -f "${current}/compose.lakehouse.yml" --profile lakehouse-query "$@"
}

host_layout_value() {
  python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' \
    "${release_root}/current/config/host-layout.contract.json" "$1"
}

# The files the release's Trino mounts whose bytes differ from the running container's, one per line.
trino_changed_files() {
  local file want have
  for file in "${release_root}"/current/infra/lakehouse/trino/*.properties \
    "${release_root}"/current/infra/lakehouse/trino/*.json "${release_root}/current/infra/lakehouse/trino/jvm.config"; do
    want="$(sha256sum <"${file}" | cut -d' ' -f1)"
    have="$(docker exec "${trino_container}" cat "/etc/trino/${file##*/}" 2>/dev/null | sha256sum | cut -d' ' -f1)"
    [[ "${want}" == "${have}" ]] || printf '%s\n' "${file##*/}"
  done
}

apply_trino() {
  assert_current_release
  local want have changed attempt
  if ! docker inspect "${trino_container}" >/dev/null 2>&1; then
    printf 'trino: no container on this host; nothing follows the release\n'
    return 0
  fi
  install -d -m 1777 "$(host_layout_value trino_spill)"
  want="$(trino_compose config --hash trino | awk '$1 == "trino" {print $2}')"
  have="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.config-hash"}}' "${trino_container}")"
  changed="$(trino_changed_files)"
  if [[ -n "${want}" && "${want}" == "${have}" && -z "${changed}" ]]; then
    printf 'trino: already runs this release\n'
    return 0
  fi
  printf 'trino: recreating (compose config %s; files differing: %s)\n' \
    "$([[ "${want}" == "${have}" ]] && echo same || echo changed)" "$(tr '\n' ' ' <<<"${changed:-none}")"
  trino_compose up -d --force-recreate --no-deps trino
  for attempt in $(seq 1 90); do
    if docker exec "${trino_container}" trino --server localhost:8080 --execute 'SELECT 1' >/dev/null 2>&1; then
      printf 'trino: answers after the recreate (attempt %s)\n' "${attempt}"
      return 0
    fi
    sleep 2
  done
  printf 'trino: did not answer within 180s of the recreate\n' >&2
  return 1
}
