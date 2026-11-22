#!/usr/bin/env bash
# Sourced by foundation-release.sh `trino` (it provides release_root and assert_current_release).
# Trino follows the release (root ADR-0504). Its container mounts files of the release it was
# created from and nothing recreated it on deploy, so a merged memory limit waited for the next crash
# (2026-11-11: the kernel killed Trino at 16.55GB under the old limits). `up -d` recreates it when
# what it runs differs from this release, by the config hash Compose itself compares: the compose
# config (the catalog directory from config/host-layout.contract.json reproduces the running one) and,
# through the `foundation.trino-files-digest` label, the bytes of the files it mounts from
# infra/lakehouse/trino, whose paths stay the same across releases. Same: nothing is touched. No
# container: nothing to follow.
trino_container=foundation-platform-trino
trino_compose() {
  local current="${release_root}/current"
  FOUNDATION_PLATFORM_TRINO_CATALOG_DIR="$(host_layout_value trino_catalog_dir)" \
  FOUNDATION_PLATFORM_TRINO_SPILL="$(host_layout_value trino_spill)" \
  FOUNDATION_TRINO_FILES_DIGEST="$(trino_files_digest)" \
    docker compose -p foundation-platform-compute --project-directory "${current}" \
      -f "${current}/compose.lakehouse.yml" --profile lakehouse-query "$@"
}

host_layout_value() {
  python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' \
    "${release_root}/current/config/host-layout.contract.json" "$1"
}

# The names and bytes of the files the release's Trino mounts from infra/lakehouse/trino, as one
# digest (the same form as foundation-runtime.sh's for Prometheus and Alertmanager).
trino_files_digest() {
  local file
  for file in "${release_root}"/current/infra/lakehouse/trino/*.properties \
    "${release_root}"/current/infra/lakehouse/trino/*.json "${release_root}/current/infra/lakehouse/trino/jvm.config"; do
    printf '%s\n' "${file##*/}" "$(sha256sum <"${file}")"
  done | sha256sum | cut -c1-16
}

apply_trino() {
  assert_current_release
  local before after attempt
  # The container and when it started: `up` leaves both alone, starts a stopped container, or
  # recreates it, and after the last two Trino is not answering yet.
  if ! before="$(docker inspect -f '{{.Id}} {{.State.StartedAt}}' "${trino_container}" 2>/dev/null)"; then
    printf 'trino: no container on this host; nothing follows the release\n'
    return 0
  fi
  install -d -m 1777 "$(host_layout_value trino_spill)"
  trino_compose up -d --no-deps trino
  after="$(docker inspect -f '{{.Id}} {{.State.StartedAt}}' "${trino_container}")"
  if [[ "${after}" == "${before}" ]]; then
    printf 'trino: already runs this release\n'
    return 0
  fi
  if [[ "${after%% *}" == "${before%% *}" ]]; then
    printf 'trino: started the stopped container\n'
  else
    printf 'trino: recreated for this release\n'
  fi
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
