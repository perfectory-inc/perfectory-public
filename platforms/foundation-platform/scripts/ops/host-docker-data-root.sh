#!/usr/bin/env bash
# Move Docker and containerd from the system disk to the data disk (root ADR-0241).
#
#   host-docker-data-root.sh dry-run      what a run would do, changing nothing (root not required)
#   sudo host-docker-data-root.sh run     the move, with its own rollback
#   sudo host-docker-data-root.sh rollback        put the host back on the system disk trees
#   sudo host-docker-data-root.sh delete-old      after the owner checked the host: remove the old trees
#
# Where things go is config/host-layout.contract.json (host_docker_layout.py reads it). Docker 29
# keeps images, snapshots and the build cache in containerd, so both roots move: Docker's
# data-root (daemon.json `data-root`) and containerd's (config.toml `root`).
#
# A run:
#  1. preflight: no autodeploy runs (and none may start: autodeploy.off is held for the run), no
#     registered job runs (release admission's answer) and no Spark container exists, the data disk
#     has `free_space_factor` times the measured size free, and the targets are empty.
#  2. records which containers run, which are healthy and which foundation_ DAGs are unpaused,
#     pauses those DAGs, and copies both trees once while Docker still runs (the long part).
#  3. stops docker.socket, docker and containerd, copies again (only what changed), and checks a
#     third pass would copy nothing.
#  4. sets data-root and root (keeping every other key), makes both units wait for the data mount,
#     renames the old trees to <tree>.old, starts containerd and Docker.
#  5. verifies: Docker's root is the new one, every container that ran runs again, every container
#     that was healthy is healthy again, and each readiness URL of the contract answers 200.
#  6. unpauses exactly the DAGs that were unpaused before, in the registry's consumers-first order,
#     and gives autodeploy back.
# Any failure after the stop rolls back: stop, restore both config files, remove the drop-ins,
# rename the old trees back, start, unpause. The copies on the data disk stay for inspection.
#
# Idempotent: a host already on the data disk is reported and left alone.
set -euo pipefail

MODE="${1:-}"
case "${MODE}" in dry-run|run|rollback|delete-old) ;; *)
  echo "usage: host-docker-data-root.sh dry-run|run|rollback|delete-old" >&2; exit 64 ;;
esac
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PLATFORM_ROOT="$(cd "${HERE}/../.." && pwd -P)"
export PYTHONDONTWRITEBYTECODE=1
layout() { python3 -I "${HERE}/host_docker_layout.py" layout "$1"; }
DATA_MOUNT="$(layout data_mount)"
DOCKER_ROOT="$(layout docker.data_root)"
DOCKER_OLD="$(layout docker.legacy_root)"
DAEMON_JSON="$(layout docker.daemon_json)"
CTRD_ROOT="$(layout containerd.root)"
CTRD_OLD="$(layout containerd.legacy_root)"
CTRD_TOML="$(layout containerd.config_toml)"
FACTOR="$(layout free_space_factor)"
LIVE_CAP="$(layout time_caps_minutes.live_copy)"
STOPPED_CAP="$(layout time_caps_minutes.stopped_copy)"
mapfile -t READINESS < <(layout readiness)
STATE_ROOT=/var/lib/foundation-host-layout
AUTODEPLOY_OFF=/etc/foundation-platform/autodeploy.off
ADMISSION=/opt/perfectory-control/current/scripts/deploy/foundation-release-admission.py
DROPIN_NAME=10-data-disk.conf
RSYNC=(rsync -aHAXx --numeric-ids --delete)

log() { printf '%s host-docker-data-root: %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { log "REFUSED: $1"; exit "${2:-1}"; }
need_root() { [[ ${EUID} == 0 ]] || die "${MODE} changes the host; run it with sudo" 64; }

# Airflow as the deployer, the way the deploy pauses DAGs (foundation-deploy.sh step 0).
airflow() {
  local runner=(bash /opt/foundation-platform/current/scripts/deploy/airflow-runtime.sh exec airflow-scheduler airflow "$@")
  if [[ ${EUID} == 0 ]]; then
    local deployer
    deployer="$(sed -n 's/^FOUNDATION_DEPLOYER=\([a-z_][a-z0-9_-]*\)$/\1/p' /etc/foundation-platform/release-deploy.conf | tail -1)"
    [[ -n "${deployer}" ]] || die "/etc/foundation-platform/release-deploy.conf names no FOUNDATION_DEPLOYER" 78
    sudo -u "${deployer}" -H "${runner[@]}"
  else
    "${runner[@]}"
  fi
}
consumers_first() {
  python3 -I - "${PLATFORM_ROOT}/orchestration/dags" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from job_specs import unpause_order
print("\n".join(unpause_order()))
PY
}
size_bytes() {
  if [[ ${EUID} == 0 ]]; then
    du -sxb "$1" | cut -f1
  else
    # A dry run without root measures through Docker, read-only.
    docker run --rm -v "$1:/m:ro" alpine:3 du -sxb /m | cut -f1
  fi
}
on_data_disk() { [[ "$(docker info --format '{{.DockerRootDir}}' 2>/dev/null)" == "${DOCKER_ROOT}" ]]; }
spark_containers() { docker ps --format '{{.Names}}' | grep -E -- '-spark-run-|spark-small-run|lakehouse-control-run' || true; }
running_containers() { docker ps --format '{{.Names}}' | sort; }
healthy_containers() {
  docker ps --format '{{.Names}}' | while read -r name; do
    [[ "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${name}")" == healthy ]] && echo "${name}"
  done | sort
}

preflight() {
  local failures=0
  if on_data_disk; then
    log "Docker already runs from ${DOCKER_ROOT}; nothing to do"
    exit 0
  fi
  findmnt -n "${DATA_MOUNT}" >/dev/null || { log "check: ${DATA_MOUNT} is not a mount point"; failures=1; }
  case "$(systemctl is-active foundation-autodeploy.service 2>/dev/null || true)" in
    active|activating|reloading) log "check: an autodeploy is running"; failures=1 ;;
    *) log "check: no autodeploy runs" ;;
  esac
  local busy status=0
  busy="$("${ADMISSION}" running-jobs 2>&1)" || status=$?
  case "${status}" in
    0) log "check: no registered job runs" ;;
    75) log "check: registered jobs run: ${busy//$'\n'/ }"; failures=1 ;;
    *) log "check: release admission gave no answer (exit ${status}): ${busy}"; failures=1 ;;
  esac
  local spark
  spark="$(spark_containers)"
  if [[ -n "${spark}" ]]; then log "check: Spark containers exist: ${spark//$'\n'/ }"; failures=1; else log "check: no Spark container"; fi
  local docker_bytes ctrd_bytes free_bytes
  docker_bytes="$(size_bytes "${DOCKER_OLD}")"
  ctrd_bytes="$(size_bytes "${CTRD_OLD}")"
  free_bytes="$(df -B1 --output=avail "${DATA_MOUNT}" | tail -1 | tr -d ' ')"
  log "check: ${DOCKER_OLD} $((docker_bytes / 1024**3)) GiB, ${CTRD_OLD} $((ctrd_bytes / 1024**3)) GiB; ${DATA_MOUNT} free $((free_bytes / 1024**3)) GiB (needs ${FACTOR}x = $(((docker_bytes + ctrd_bytes) * FACTOR / 1024**3)) GiB)"
  ((free_bytes > (docker_bytes + ctrd_bytes) * FACTOR)) || { log "check: not enough free space on ${DATA_MOUNT}"; failures=1; }
  local target
  for target in "${DOCKER_ROOT}" "${CTRD_ROOT}"; do
    if [[ -e "${target}" && "${MODE}" == dry-run ]]; then
      log "check: ${target} exists (a run resumes the copy into it)"
    fi
  done
  for target in "${DOCKER_OLD}.old" "${CTRD_OLD}.old"; do
    [[ ! -e "${target}" ]] || { log "check: ${target} exists from an earlier run; delete-old or rollback first"; failures=1; }
  done
  return "${failures}"
}

write_configs() {
  local new
  new="$(mktemp)"
  python3 -I "${HERE}/host_docker_layout.py" daemon-json "${DAEMON_JSON}" "${DOCKER_ROOT}" >"${new}"
  install -D -m 0644 "${new}" "${DAEMON_JSON}.tmp" && mv -f "${DAEMON_JSON}.tmp" "${DAEMON_JSON}"
  python3 -I "${HERE}/host_docker_layout.py" containerd-toml "${CTRD_TOML}" "${CTRD_ROOT}" >"${new}"
  install -D -m 0644 "${new}" "${CTRD_TOML}.tmp" && mv -f "${CTRD_TOML}.tmp" "${CTRD_TOML}"
  rm -f "${new}"
  local unit
  for unit in docker containerd; do
    install -d -m 0755 "/etc/systemd/system/${unit}.service.d"
    printf '[Unit]\n# Docker and containerd keep their data on the data disk (root ADR-0241).\nRequiresMountsFor=%s\n' \
      "${DATA_MOUNT}" >"/etc/systemd/system/${unit}.service.d/${DROPIN_NAME}"
  done
  systemctl daemon-reload
}

stop_engine() { systemctl stop docker.socket docker.service containerd.service; }
start_engine() { systemctl start containerd.service docker.socket docker.service; }

restore_configs() {
  local run="$1"
  if [[ -f "${run}/daemon.json" ]]; then cp -p "${run}/daemon.json" "${DAEMON_JSON}"; else rm -f "${DAEMON_JSON}"; fi
  cp -p "${run}/config.toml" "${CTRD_TOML}"
  rm -f "/etc/systemd/system/docker.service.d/${DROPIN_NAME}" "/etc/systemd/system/containerd.service.d/${DROPIN_NAME}"
  systemctl daemon-reload
}

unpause_recorded() {
  local run="$1" id failed=0
  [[ -s "${run}/unpaused-dags" ]] || { log "no DAG was unpaused before the run; none unpaused"; return 0; }
  for id in $(consumers_first); do
    grep -qx "foundation_${id}" "${run}/unpaused-dags" || continue
    airflow dags unpause "foundation_${id}" >/dev/null || { log "FAILED to unpause foundation_${id}"; failed=1; }
  done
  # A recorded DAG the registry no longer orders (renamed since) is unpaused last.
  while read -r dag; do
    consumers_first | sed 's/^/foundation_/' | grep -qx "${dag}" && continue
    airflow dags unpause "${dag}" >/dev/null || { log "FAILED to unpause ${dag}"; failed=1; }
  done <"${run}/unpaused-dags"
  log "unpaused the $(wc -l <"${run}/unpaused-dags") DAGs that were unpaused before"
  return "${failed}"
}

give_autodeploy_back() {
  local run="$1"
  [[ -f "${run}/autodeploy-off-created" ]] && rm -f "${AUTODEPLOY_OFF}" && log "autodeploy given back"
  return 0
}

verify() {
  local run="$1" name missing=0 code url
  [[ "$(docker info --format '{{.DockerRootDir}}')" == "${DOCKER_ROOT}" ]] || { log "verify: Docker's root is not ${DOCKER_ROOT}"; return 1; }
  # containerd 2 prints the merged config with single quotes (root = '/var/lib/containerd').
  containerd config dump 2>/dev/null | grep -qE "^root = [\"']${CTRD_ROOT}[\"']$" || { log "verify: containerd's root is not ${CTRD_ROOT}"; return 1; }
  # Containers with a restart policy come back by themselves; the rest are started by name.
  sleep 20
  comm -23 "${run}/running-before" <(running_containers) | while read -r name; do
    docker start "${name}" >/dev/null 2>&1 || true
  done
  for _ in $(seq 1 60); do
    missing="$(comm -23 "${run}/running-before" <(running_containers) | wc -l)"
    unhealthy="$(comm -23 "${run}/healthy-before" <(healthy_containers) | wc -l)"
    ((missing == 0 && unhealthy == 0)) && break
    sleep 10
  done
  if ((missing > 0 || unhealthy > 0)); then
    log "verify: not running again: $(comm -23 "${run}/running-before" <(running_containers) | tr '\n' ' ')"
    log "verify: not healthy again: $(comm -23 "${run}/healthy-before" <(healthy_containers) | tr '\n' ' ')"
    return 1
  fi
  log "verify: all $(wc -l <"${run}/running-before") containers run again; all $(wc -l <"${run}/healthy-before") healthy ones are healthy"
  for entry in "${READINESS[@]}"; do
    name="${entry%% *}"; url="${entry#* }"; code=000
    for _ in $(seq 1 30); do
      code="$(curl -s -o /dev/null -w '%{http_code}' -m 10 "${url}" || true)"
      [[ "${code}" == 200 ]] && break
      sleep 10
    done
    [[ "${code}" == 200 ]] || { log "verify: ${name} ${url} answered ${code}"; return 1; }
    log "verify: ${name} ready"
  done
}

rollback_run() {
  local run="$1"
  log "ROLLBACK: putting Docker and containerd back on ${DOCKER_OLD} and ${CTRD_OLD}"
  stop_engine || true
  restore_configs "${run}"
  [[ -e "${DOCKER_OLD}.old" && ! -e "${DOCKER_OLD}" ]] && mv "${DOCKER_OLD}.old" "${DOCKER_OLD}"
  [[ -e "${CTRD_OLD}.old" && ! -e "${CTRD_OLD}" ]] && mv "${CTRD_OLD}.old" "${CTRD_OLD}"
  start_engine
  sleep 20
  comm -23 "${run}/running-before" <(running_containers) | while read -r name; do docker start "${name}" >/dev/null 2>&1 || true; done
  unpause_recorded "${run}" || true
  give_autodeploy_back "${run}"
  log "ROLLBACK done: Docker root $(docker info --format '{{.DockerRootDir}}'); the copies under ${DATA_MOUNT} are kept for inspection"
}

case "${MODE}" in
dry-run)
  log "dry run: nothing changes"
  status=0; preflight || status=$?
  log "would pause, then unpause again, these unpaused DAGs: $(airflow dags list -o json 2>/dev/null | python3 -I "${HERE}/host_docker_layout.py" unpaused-dags /dev/stdin | tr '\n' ' ')"
  log "would keep running: $(running_containers | wc -l) containers ($(healthy_containers | wc -l) healthy)"
  log "would copy: ${RSYNC[*]} ${DOCKER_OLD}/ ${DOCKER_ROOT}/ and ${CTRD_OLD}/ ${CTRD_ROOT}/ (once live, once stopped, then a pass that must copy nothing)"
  log "would write ${DAEMON_JSON}:"
  python3 -I "${HERE}/host_docker_layout.py" daemon-json "${DAEMON_JSON}" "${DOCKER_ROOT}"
  log "would set in ${CTRD_TOML}: $(python3 -I "${HERE}/host_docker_layout.py" containerd-toml "${CTRD_TOML}" "${CTRD_ROOT}" | grep -E '^root\s*=')"
  log "would add RequiresMountsFor=${DATA_MOUNT} to docker.service and containerd.service, and keep ${DOCKER_OLD}.old and ${CTRD_OLD}.old until delete-old"
  ((status == 0)) && log "dry run: preflight passes" || log "dry run: preflight would refuse now"
  exit "${status}"
  ;;
run)
  need_root
  mkdir -p "${STATE_ROOT}"
  exec 9>"${STATE_ROOT}/lock"
  flock -n 9 || die "another run holds ${STATE_ROOT}/lock" 75
  preflight || die "preflight failed; nothing changed" 75
  RUN="${STATE_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "${RUN}"
  ln -sfn "${RUN}" "${STATE_ROOT}/last"
  [[ -e "${AUTODEPLOY_OFF}" ]] || { touch "${AUTODEPLOY_OFF}"; : >"${RUN}/autodeploy-off-created"; }
  [[ ! -f "${DAEMON_JSON}" ]] || cp -p "${DAEMON_JSON}" "${RUN}/daemon.json"
  cp -p "${CTRD_TOML}" "${RUN}/config.toml"
  running_containers >"${RUN}/running-before"
  healthy_containers >"${RUN}/healthy-before"
  airflow dags list -o json >"${RUN}/dags-before.json"
  python3 -I "${HERE}/host_docker_layout.py" unpaused-dags "${RUN}/dags-before.json" >"${RUN}/unpaused-dags"
  log "recorded $(wc -l <"${RUN}/running-before") running containers, $(wc -l <"${RUN}/unpaused-dags") unpaused DAGs in ${RUN}"
  trap 'log "stopped before Docker was stopped; unpausing"; unpause_recorded "${RUN}" || true; give_autodeploy_back "${RUN}"' EXIT
  while read -r dag; do airflow dags pause "${dag}" >/dev/null; done <"${RUN}/unpaused-dags"
  log "paused those DAGs; copying while Docker runs"
  for pair in "${DOCKER_OLD} ${DOCKER_ROOT}" "${CTRD_OLD} ${CTRD_ROOT}"; do
    read -r from to <<<"${pair}"
    mkdir -p "${to}"; chmod --reference="${from}" "${to}"
    status=0; timeout "${LIVE_CAP}m" "${RSYNC[@]}" "${from}/" "${to}/" || status=$?
    # 24: files vanished while Docker ran; the stopped pass copies the final state.
    ((status == 0 || status == 24)) || die "the live copy of ${from} failed (rsync ${status})"
  done
  [[ -z "$(spark_containers)" ]] || die "a Spark container started during the copy"
  log "stopping docker.socket, docker and containerd"
  trap 'status=$?; ((status == 0)) || rollback_run "${RUN}"' EXIT
  stop_engine
  for pair in "${DOCKER_OLD} ${DOCKER_ROOT}" "${CTRD_OLD} ${CTRD_ROOT}"; do
    read -r from to <<<"${pair}"
    timeout "${STOPPED_CAP}m" "${RSYNC[@]}" "${from}/" "${to}/"
    left="$("${RSYNC[@]}" -n -i "${from}/" "${to}/")"
    [[ -z "${left}" ]] || { log "a third pass of ${from} would still copy:"; head -20 <<<"${left}"; exit 1; }
    log "copied ${from} -> ${to}; a further pass copies nothing"
  done
  write_configs
  mv "${DOCKER_OLD}" "${DOCKER_OLD}.old"
  mv "${CTRD_OLD}" "${CTRD_OLD}.old"
  start_engine
  verify "${RUN}"
  trap - EXIT
  unpause_recorded "${RUN}" || log "some DAGs did not unpause; unpause them by hand (list: ${RUN}/unpaused-dags)"
  give_autodeploy_back "${RUN}"
  log "DONE: Docker ${DOCKER_ROOT}, containerd ${CTRD_ROOT}; ${DOCKER_OLD}.old and ${CTRD_OLD}.old stay until 'delete-old'"
  df -h / "${DATA_MOUNT}"
  ;;
rollback)
  need_root
  [[ -L "${STATE_ROOT}/last" ]] || die "no recorded run to roll back" 66
  rollback_run "$(readlink -f "${STATE_ROOT}/last")"
  ;;
delete-old)
  need_root
  on_data_disk || die "Docker does not run from ${DOCKER_ROOT}; nothing to delete" 66
  for old in "${DOCKER_OLD}.old" "${CTRD_OLD}.old"; do
    [[ -e "${old}" ]] || continue
    rm -rf --one-file-system "${old}"
    log "deleted ${old}"
  done
  df -h / "${DATA_MOUNT}"
  ;;
esac
