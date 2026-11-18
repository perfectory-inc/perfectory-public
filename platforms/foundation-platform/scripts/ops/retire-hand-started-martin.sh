#!/usr/bin/env bash
# one-off: until the deploy's `retire-hand-started-martin` line reports `gone` for both containers on
# the host; the PR after that deploy removes this file, its deploy step, its test and the
# `host.uncapped` entry of platform.definition.json.
#
# Removes the two Martin tile servers an operator started by hand with `docker run` in September 2026
# (martin-complex 09-04, martin-static 09-27). Root ADR-0112 §10 and ADR-0133 decided to take them
# down once the map was served from R2; root ADR-0343 §5 asks for it. Measured 2026-11-18 (W83c,
# read-only): no Compose labels, `restart=no`, host network on 0.0.0.0:3110 and :3111, a health check
# on port 3000 that has failed for four weeks, no established connection on either port, and every
# unit of the current runtime manifest (catalog.vector_tile_runtime_manifest_pointer) and every active
# release (vector_tile_publication_unit.active_release_id) addressed to tiles.perfectory.io. The six
# releases still recorded at 127.0.0.1:3110/3111 are September ones no manifest serves.
#
# A container is removed only while it is still that one: no Compose project label (a declared
# service of that name is the deploy's, never this script's), the Martin image, and no established
# connection on its port. Otherwise it is left and the line says why. Removing is `docker rm -f`;
# the config files under /home/perfectory stay, so `docker run` with the September arguments brings
# one back.
#
#     retire-hand-started-martin.sh        prints one line per container; exit 0 unless docker fails
set -Eeuo pipefail

declare -A PORTS=([martin-complex]=3110 [martin-static]=3111)
status=0
for name in martin-complex martin-static; do
  port="${PORTS[${name}]}"
  if ! docker container inspect "${name}" >/dev/null 2>&1; then
    echo "retire-hand-started-martin ${name}: gone"
    continue
  fi
  project="$(docker container inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "${name}")"
  image="$(docker container inspect -f '{{.Config.Image}}' "${name}")"
  if [[ -n "${project}" ]]; then
    echo "retire-hand-started-martin ${name}: kept, it belongs to Compose project ${project}"
    continue
  fi
  if [[ "${image}" != ghcr.io/maplibre/martin:* ]]; then
    echo "retire-hand-started-martin ${name}: kept, its image is ${image}, not Martin"
    continue
  fi
  connections="$(ss -Htn state established "( sport = :${port} )" | wc -l)"
  if [[ "${connections}" != 0 ]]; then
    echo "retire-hand-started-martin ${name}: kept, ${connections} established connection(s) on :${port}"
    continue
  fi
  if docker rm -f "${name}" >/dev/null; then
    echo "retire-hand-started-martin ${name}: removed (no connection on :${port})"
  else
    echo "retire-hand-started-martin ${name}: docker rm failed" >&2
    status=1
  fi
done
exit "${status}"
