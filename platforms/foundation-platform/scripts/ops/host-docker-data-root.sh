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
#  6. unpauses the DAGs that were unpaused before and whose jobs are enabled (a disabled job stays
#     paused, as the deploy leaves it), in the registry's consumers-first order, and gives autodeploy back.
# Any failure after the stop rolls back: stop, restore both config files, remove the drop-ins,
# rename the old trees back, start, unpause. The copies on the data disk stay for inspection.
#
# Idempotent: a host already on the data disk is reported and left alone.
#
# The file can be sourced (orchestration/tests/test_host_docker_data_root_script.py does): it then
# defines its functions and runs nothing.
set -euo pipefail

MODE=""
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
RELEASE_DEPLOY_CONF=/etc/foundation-platform/release-deploy.conf
AIRFLOW_RUNTIME="${PLATFORM_ROOT}/scripts/deploy/airflow-runtime.sh"
ADMISSION=/opt/perfectory-control/current/scripts/deploy/foundation-release-admission.py
DROPIN_NAME=10-data-disk.conf
RSYNC=(rsync -aHAXx --numeric-ids --delete)
# Only a dry run without root uses it, to measure the trees read-only (tools/technology-versions.contract.json pins it).
MEASURE_IMAGE="busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
JOB_UNITS="${PLATFORM_ROOT}/scripts/deploy/job_units.py"

log() { printf '%s host-docker-data-root: %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { log "REFUSED: $1"; exit "${2:-1}"; }
am_root() { [[ "$(id -u)" == 0 ]]; }
need_root() { am_root || die "${MODE} changes the host; run it with sudo" 64; }

# Airflow as the deployer, the way the deploy pauses DAGs (foundation-deploy.sh step 0). `-T` and
# no stdin: the owner runs this over `ssh -t`, and a terminal on stdin would have compose allocate
# one for the CLI too, which the deploy (under systemd) never does.
airflow() {
  local runner=(bash "${AIRFLOW_RUNTIME}" exec -T airflow-scheduler airflow "$@")
  if am_root; then
    local deployer
    deployer="$(sed -n 's/^FOUNDATION_DEPLOYER=\([a-z_][a-z0-9_-]*\)$/\1/p' "${RELEASE_DEPLOY_CONF}" | tail -1)"
    [[ -n "${deployer}" ]] || die "${RELEASE_DEPLOY_CONF} names no FOUNDATION_DEPLOYER" 78
    sudo -u "${deployer}" -H "${runner[@]}" </dev/null
  else
    "${runner[@]}" </dev/null
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
  if am_root; then
    du -sxb "$1" | cut -f1
  else
    # A dry run without root measures through Docker, read-only.
    # busybox du counts in KiB.
    echo $(( $(docker run --rm --memory 64m -v "$1:/m:ro" "${MEASURE_IMAGE}" du -sxk /m | cut -f1) * 1024 ))
  fi
}
on_data_disk() { [[ "$(docker info --format '{{.DockerRootDir}}' 2>/dev/null)" == "${DOCKER_ROOT}" ]]; }
spark_containers() { docker ps --format '{{.Names}}' | grep -E -- '-spark-run-|spark-small-run|lakehouse-control-run' || true; }
running_containers() { docker ps --format '{{.Names}}' | sort; }
healthy_containers() {
  # `if`, not `[[ … ]] && echo`: a loop whose last container is not healthy would otherwise end
  # with status 1, and under pipefail and `set -e` the caller stops right after writing the list
  # (the 2026-10-15 12:03 run did, after healthy-before, before anything else was recorded).
  docker ps --format '{{.Names}}' | while read -r name; do
    if [[ "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${name}")" == healthy ]]; then
      echo "${name}"
    fi
  done | sort
}

# What a run restores from: the containers that run, the healthy ones, the unpaused DAGs. A dry run
# records the same into a temporary directory, so every call a run makes here (as root, the
# deployer's Airflow call too) is exercised before a run depends on it.
record_state() {
  local dir="$1"
  running_containers >"${dir}/running-before"
  healthy_containers >"${dir}/healthy-before"
  airflow dags list -o json >"${dir}/dags-before.json.tmp"
  mv "${dir}/dags-before.json.tmp" "${dir}/dags-before.json"
  python3 -I "${HERE}/host_docker_layout.py" unpaused-dags "${dir}/dags-before.json" >"${dir}/unpaused-dags"
}

# The run's first steps. The cleanup is in place before autodeploy.off exists, so a failure at any
# step from here on gives autodeploy back; the marker is written before the file, so the cleanup
# removes only a file this run created.
begin_run() {
  local run="$1"
  trap 'log "stopped before Docker was stopped; unpausing"; unpause_recorded "'"${run}"'" || true; give_autodeploy_back "'"${run}"'"' EXIT
  if [[ ! -e "${AUTODEPLOY_OFF}" ]]; then
    : >"${run}/autodeploy-off-created"
    touch "${AUTODEPLOY_OFF}"
  fi
  [[ ! -f "${DAEMON_JSON}" ]] || cp -p "${DAEMON_JSON}" "${run}/daemon.json"
  cp -p "${CTRD_TOML}" "${run}/config.toml"
  record_state "${run}"
  log "recorded $(wc -l <"${run}/running-before") running containers, $(wc -l <"${run}/unpaused-dags") unpaused DAGs in ${run}"
}

preflight() {
  local failures=0
  if on_data_disk; then
    log "Docker already runs from ${DOCKER_ROOT}; nothing to do"
    exit 0
  fi
  findmnt -n "${DATA_MOUNT}" >/dev/null || { log "check: ${DATA_MOUNT} is not a mount point"; failures=1; }
  # Whether a unit runs is job_units.py's one answer (root ADR-0184): a oneshot is `activating`
  # for its whole run. Every foundation service: the autodeploy, the registered jobs and the
  # operator's units the registry does not list (a pack bake, an unconditional Gold rebuild).
  local units units_status=0
  units="$(python3 -I -B "${JOB_UNITS}" running 'foundation-*.service' 2>&1)" || units_status=$?
  if ((units_status != 0)); then
    log "check: systemd could not say which units run (exit ${units_status}): ${units}"; failures=1
  elif [[ -n "${units}" ]]; then
    log "check: foundation units run: ${units//$'\n'/; }"; failures=1
  else
    log "check: no foundation unit runs (the autodeploy included)"
  fi
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

ENGINE_UNITS=(docker.socket docker.service containerd.service)
UNPAUSE_RETRY_SECONDS=10

# While the engine is stopped nothing may start it again. On 2026-10-15 at 14:05:16, seven seconds
# after the run stopped it, the autodeploy timer (every minute) queued its service, whose
# `Requires=docker.service` started containerd and Docker on their old roots: the stopped copy then
# copied from a running engine and the run rolled back. So the foundation timers are stopped for
# the stop (their units would only fail every minute) and the engine units are masked for the
# runtime of the boot (`--runtime`: /run, gone on reboot), which refuses every start, a dependency's
# included. The timers stopped are recorded and started again whatever the outcome.
stop_engine() {
  local run="$1"
  systemctl list-timers --no-legend --plain 'foundation-*.timer' 2>/dev/null \
    | grep -oE 'foundation-[^ ]+\.timer' | sort -u >"${run}/timers-stopped" || true
  if [[ -s "${run}/timers-stopped" ]]; then
    # shellcheck disable=SC2046 # one unit name per line
    systemctl stop $(cat "${run}/timers-stopped")
    log "stopped $(wc -l <"${run}/timers-stopped") foundation timers for the stop: $(tr '\n' ' ' <"${run}/timers-stopped")"
  fi
  systemctl stop "${ENGINE_UNITS[@]}"
  systemctl mask --runtime "${ENGINE_UNITS[@]}" >/dev/null
  log "Docker and containerd stopped and masked for this boot; nothing can start them until the run does"
}
# Right before the stopped copy: every engine unit is stopped, asked through job_units.py (the
# host's one answer to "does this unit run", root ADR-0184; a unit is running unless inactive or
# failed). Anything else means something started it, and the copy would read a live tree.
engine_is_down() {
  local running status=0
  running="$(python3 -I -B "${JOB_UNITS}" running "${ENGINE_UNITS[@]}" 2>&1)" || status=$?
  if ((status != 0)); then
    log "systemd could not say whether the engine is stopped (exit ${status}): ${running}"
    return 1
  fi
  if [[ -n "${running}" ]]; then
    log "the engine runs again although it was stopped and masked: ${running//$'
'/; }"
    return 1
  fi
}
start_engine() {
  systemctl unmask --runtime "${ENGINE_UNITS[@]}" >/dev/null
  systemctl start containerd.service docker.socket docker.service
}
restart_timers() {
  local run="$1"
  [[ -s "${run}/timers-stopped" ]] || return 0
  # shellcheck disable=SC2046 # one unit name per line
  if systemctl start $(cat "${run}/timers-stopped"); then
    log "started the foundation timers again"
  else
    log "FAILED to start the foundation timers again: $(tr '\n' ' ' <"${run}/timers-stopped")"
  fi
}

restore_configs() {
  local run="$1"
  if [[ -f "${run}/daemon.json" ]]; then cp -p "${run}/daemon.json" "${DAEMON_JSON}"; else rm -f "${DAEMON_JSON}"; fi
  cp -p "${run}/config.toml" "${CTRD_TOML}"
  rm -f "/etc/systemd/system/docker.service.d/${DROPIN_NAME}" "/etc/systemd/system/containerd.service.d/${DROPIN_NAME}"
  systemctl daemon-reload
}

unpause_recorded() {
  local run="$1" id attempt count=0
  [[ -s "${run}/unpaused-dags" ]] || { log "no DAG was unpaused before the run; none unpaused"; return 0; }
  # Only an enabled job's DAG, in the registry's consumers-first order (root ADR-0179): a disabled
  # job is paused by design, whatever Airflow showed before, as the deploy does.
  local order
  order="$(consumers_first)"
  : >"${run}/unpause-wanted"
  for id in ${order}; do
    grep -qx "foundation_${id}" "${run}/unpaused-dags" || continue
    echo "foundation_${id}" >>"${run}/unpause-wanted"
    # Airflow comes back with the engine; a call made before it answers is tried again.
    for attempt in 1 2 3 4 5 6; do
      if airflow dags unpause "foundation_${id}" >/dev/null 2>&1; then count=$((count + 1)); break; fi
      log "unpause foundation_${id}: attempt ${attempt} failed; trying again in ${UNPAUSE_RETRY_SECONDS} s"
      sleep "${UNPAUSE_RETRY_SECONDS}"
    done
  done
  local left
  left="$(grep -vxF -f <(sed 's/^/foundation_/' <<<"${order}") "${run}/unpaused-dags" | tr '\n' ' ' || true)"
  [[ -z "${left// }" ]] || log "left paused (their jobs are disabled in the registry): ${left}"
  # Asked back, not assumed: what Airflow lists as still paused among the ones wanted.
  local listed still
  listed="$(mktemp)"
  if airflow dags list -o json >"${listed}" 2>/dev/null; then
    still="$(comm -23 <(sort "${run}/unpause-wanted") \
      <(python3 -I "${HERE}/host_docker_layout.py" unpaused-dags "${listed}" | sort))"
  else
    still="$(cat "${run}/unpause-wanted")"
    log "could not list the DAGs to check the unpause"
  fi
  rm -f "${listed}"
  if [[ -n "${still}" ]]; then
    printf '%s\n' "${still}" >"${run}/unpause-failed"
    log "STILL PAUSED after unpausing ($(wc -l <"${run}/unpause-failed")): $(tr '\n' ' ' <"${run}/unpause-failed") (list: ${run}/unpause-failed)"
    return 1
  fi
  log "unpaused ${count} DAGs that were unpaused before and whose jobs are enabled; Airflow lists all of them unpaused"
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
  systemctl stop "${ENGINE_UNITS[@]}" || true
  restore_configs "${run}"
  [[ -e "${DOCKER_OLD}.old" && ! -e "${DOCKER_OLD}" ]] && mv "${DOCKER_OLD}.old" "${DOCKER_OLD}"
  [[ -e "${CTRD_OLD}.old" && ! -e "${CTRD_OLD}" ]] && mv "${CTRD_OLD}.old" "${CTRD_OLD}"
  start_engine
  sleep 20
  comm -23 "${run}/running-before" <(running_containers) | while read -r name; do docker start "${name}" >/dev/null 2>&1 || true; done
  unpause_recorded "${run}" || true
  restart_timers "${run}"
  give_autodeploy_back "${run}"
  log "ROLLBACK done: Docker root $(docker info --format '{{.DockerRootDir}}'); the copies under ${DATA_MOUNT} are kept for inspection"
}

main() {
MODE="${1:-}"
case "${MODE}" in dry-run|run|rollback|delete-old) ;; *)
  echo "usage: host-docker-data-root.sh dry-run|run|rollback|delete-old" >&2; exit 64 ;;
esac
case "${MODE}" in
dry-run)
  log "dry run: nothing changes$(am_root && echo ' (as root: the root-only checks and the deployer'"'"'s Airflow call run too)')"
  status=0; preflight || status=$?
  # The same recording a run makes, into a directory that is removed again.
  probe="$(mktemp -d)"
  record_state "${probe}" || { rm -rf "${probe}"; die "recording what a run restores from failed; a run would stop here" 70; }
  log "would pause these unpaused DAGs, and afterwards unpause the ones whose jobs are enabled: $(tr '\n' ' ' <"${probe}/unpaused-dags")"
  log "would keep running: $(wc -l <"${probe}/running-before") containers ($(wc -l <"${probe}/healthy-before") healthy)"
  rm -rf "${probe}"
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
  RUN="${STATE_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "${RUN}"
  # Every line goes to the run's own log as well as the terminal: the 14:05 run's lines existed only
  # on the owner's screen. The directory and the log are world-readable (0755/0644), so the deployer
  # account reads them without root.
  : >"${RUN}/run.log"
  chmod 0644 "${RUN}/run.log"
  exec > >(tee -a "${RUN}/run.log") 2>&1
  log "run log: ${RUN}/run.log"
  preflight || die "preflight failed; nothing changed" 75
  ln -sfn "${RUN}" "${STATE_ROOT}/last"
  begin_run "${RUN}"
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
  trap 'status=$?; ((status == 0)) || { log "FAILED (exit ${status}) after the stop; rolling back"; rollback_run "${RUN}"; }' EXIT
  stop_engine "${RUN}"
  engine_is_down || exit 1
  for pair in "${DOCKER_OLD} ${DOCKER_ROOT}" "${CTRD_OLD} ${CTRD_ROOT}"; do
    read -r from to <<<"${pair}"
    status=0; timeout "${STOPPED_CAP}m" "${RSYNC[@]}" "${from}/" "${to}/" || status=$?
    ((status == 0)) || { log "the stopped copy of ${from} failed (rsync ${status})"; exit 1; }
    status=0; left="$("${RSYNC[@]}" -n -i "${from}/" "${to}/")" || status=$?
    ((status == 0)) || { log "the check pass of ${from} failed (rsync ${status})"; exit 1; }
    [[ -z "${left}" ]] || { log "a third pass of ${from} would still copy:"; head -20 <<<"${left}"; exit 1; }
    log "copied ${from} -> ${to}; a further pass copies nothing"
  done
  write_configs
  mv "${DOCKER_OLD}" "${DOCKER_OLD}.old"
  mv "${CTRD_OLD}" "${CTRD_OLD}.old"
  start_engine
  verify "${RUN}"
  trap - EXIT
  unpause_recorded "${RUN}" || log "some DAGs did not unpause; unpause them by hand (list: ${RUN}/unpause-failed)"
  restart_timers "${RUN}"
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
}

[[ "${BASH_SOURCE[0]}" != "$0" ]] || main "$@"
