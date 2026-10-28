#!/usr/bin/env bash
# What foundation-deploy.sh does about the DAGs it paused (root ADR-0159 §4 as amended by root
# ADR-0173), sourced so it can be run against stand-ins. The caller defines
#
#   airflow <args...>   Airflow's CLI in the scheduler container
#   jobs_of <which>     the job registry's lists: `enabled` ids, `started-once` "<service> <minutes>",
#                       `deferred` ids of enabled jobs a release build kept from starting (root ADR-0260)
#
# start_once_then_unpause <sha>   steps 6 and 7. Starts once, with the held DAGs still paused, each job the
#     registry marks `started_once_after_deploy`, then unpauses every enabled DAG whatever those runs
#     did. A run that does not succeed (or outlives its limit) does not undo the deploy and does not
#     keep the schedule off: its unit's OnFailure has posted it to Slack, the next scheduled run tries
#     again, and one line here names it. Returns non-zero only when a DAG could not be unpaused.
#
# deploy_paused_dags_on_exit <status> <phase> <sha>   the deploy's EXIT trap, set before step 0
#     pauses anything. On success it does nothing. A deploy that stops while the DAGs are paused does
#     not leave them paused silently:
#       before-switch  nothing of the new release is active, so the release that runs now gets its
#                      schedule back: every enabled DAG is unpaused, and the line says so.
#       switching      the new release is (being) activated, migrated or loaded into Airflow and did
#                      not finish; running jobs on it could write through a half-applied schema, so
#                      the DAGs stay paused and a line in capitals says so. The deploy's own failure
#                      reaches Slack through foundation-autodeploy.service's OnFailure.
#       after-switch   the release is live and steps 6–7 failed to unpause a DAG: the line says so.
#     Returns non-zero when it leaves DAGs paused.
#
# On 2026-10-09 the sweep's post-deploy run failed in three deploys in a row; each deploy exited at
# step 6 before step 7, every DAG (17 of 17) stayed paused, and nothing scheduled ran for about three
# hours until a person unpaused them by hand.

# deploy_metric_phase <phase>, deploy_metric_paused <all|held|none>, deploy_metric_end   what the
#     deploy is doing, as Prometheus series in the file ${deploy_metrics} that node-exporter's textfile
#     collector serves (root ADR-0321 §5). The alert rules (FoundationDeploy*) fire on how long a
#     phase, the deploy or a pause has lasted, so a pause is never silent again (2026-10-26: step 0
#     waited 1.5h on the by-PNU bake with every DAG paused and collection stopped for 2h unnoticed).
#     The paused series outlives the deploy when its EXIT trap leaves DAGs paused. The file is
#     replaced whole by a rename, so a scrape never reads half of it.
deploy_metric_write() {
  # A metric that cannot be written never stops the deploy; FoundationDeployMetricsUnread says so.
  ( set -e; deploy_metric_render ) || printf 'foundation-deploy: could not write %s\n' "${deploy_metrics}" >&2
}

deploy_metric_render() {
  local file="${deploy_metrics}"
  mkdir -p "$(dirname "${file}")"
  {
    echo "# HELP foundation_deploy_started_timestamp_seconds When the deploy under way started (root ADR-0321)."
    echo "# TYPE foundation_deploy_started_timestamp_seconds gauge"
    [[ -z "${deploy_metric_started:-}" ]] \
      || printf 'foundation_deploy_started_timestamp_seconds{sha="%s"} %s\n' "${sha}" "${deploy_metric_started}"
    echo "# HELP foundation_deploy_phase_started_timestamp_seconds When the deploy's current phase started."
    echo "# TYPE foundation_deploy_phase_started_timestamp_seconds gauge"
    [[ -z "${deploy_metric_phase:-}" ]] \
      || printf 'foundation_deploy_phase_started_timestamp_seconds{phase="%s"} %s\n' "${deploy_metric_phase}" "${deploy_metric_phase_at}"
    echo "# HELP foundation_deploy_dags_paused_since_timestamp_seconds Since when a deploy holds DAGs paused; scope all or held."
    echo "# TYPE foundation_deploy_dags_paused_since_timestamp_seconds gauge"
    [[ -z "${deploy_metric_paused:-}" ]] \
      || printf 'foundation_deploy_dags_paused_since_timestamp_seconds{scope="%s"} %s\n' "${deploy_metric_paused}" "${deploy_metric_paused_at}"
  } >"${file}.next"
  chmod 0644 "${file}.next"
  mv -f "${file}.next" "${file}"
}

deploy_metric_phase() {
  deploy_metric_started="${deploy_metric_started:-$(date +%s)}"
  deploy_metric_phase="$1" deploy_metric_phase_at="$(date +%s)"
  deploy_metric_write
}

# The held DAGs have been paused since step 0 paused them all, so a narrower scope keeps that time.
deploy_metric_paused() {
  if [[ "$1" == none ]]; then
    deploy_metric_paused="" deploy_metric_paused_at=""
  else
    deploy_metric_paused_at="${deploy_metric_paused_at:-$(date +%s)}"
    deploy_metric_paused="$1"
  fi
  deploy_metric_write
}

deploy_metric_end() {
  deploy_metric_started="" deploy_metric_phase=""
  deploy_metric_write
}

# deploy_unpause_unheld <hold-file>   after the build (deploy D5, root ADR-0270 §3): unpauses every
#     enabled DAG the hold file does not name; `*` names every job, so nothing is unpaused. The held
#     ones stay paused until step 7. A job owed a run waits for step 7 too.
deploy_unpause_unheld() {
  local id count=0
  local -a enabled held
  mapfile -t held <"$1"
  if [[ " ${held[*]-} " == *" * "* ]]; then
    echo "every DAG stays paused until the release is live: it brings a Postgres migration, or what it changes could not be told"
    return 0
  fi
  mapfile -t enabled < <(jobs_of enabled-consumers-first)
  for id in "${enabled[@]}"; do
    [[ " ${held[*]-} " == *" ${id} "* ]] && continue
    airflow dags unpause "foundation_${id}" >/dev/null || return 1
    count=$((count + 1))
  done
  echo "unpaused ${count} DAGs; held until the release is live: ${held[*]:-none}"
}

deploy_unpause_enabled() {
  local id
  local -a enabled
  mapfile -t enabled < <(jobs_of enabled-consumers-first)
  for id in "${enabled[@]}"; do
    airflow dags unpause "foundation_${id}" >/dev/null || return 1
  done
  echo "unpaused ${#enabled[@]} DAGs"
  # A job that came due while the release built was not started; it is owed its run now (root
  # ADR-0260). Its record goes when it starts, so a trigger that fails is tried by the next deploy.
  local -a owed
  mapfile -t owed < <(jobs_of deferred)
  for id in "${owed[@]}"; do
    [[ -n "${id}" ]] || continue
    if airflow dags trigger "foundation_${id}" >/dev/null; then
      echo "started foundation_${id}: it came due while the release built"
    else
      echo "foundation-deploy: could not start foundation_${id}, deferred while the release built; its next scheduled run starts it" >&2
    fi
  done
}

start_once_then_unpause() {
  local sha="$1" entry service result
  local -a once failed=()
  mapfile -t once < <(jobs_of started-once)
  for entry in "${once[@]}"; do
    service="${entry% *}"
    # The job's own limit from the registry, and a minute for systemd to report it.
    timeout "$(( ${entry##* } * 60 + 60 ))" systemctl start "${service}" || true
    result="$(systemctl show -p Result --value "${service}")" || result=unknown
    printf '%-45s %s\n' "${service}" "${result}"
    [[ "${result}" == success ]] || failed+=("${service}")
  done

  printf '\n==== %s\n' "7. unpause the enabled DAGs"
  deploy_unpause_enabled || return 1
  if ((${#failed[@]} > 0)); then
    printf '%s %s: %s; %s\n' 'foundation-deploy: the post-deploy run did not succeed under' "${sha}" "${failed[*]}" \
      'the deploy stands and the DAGs are unpaused, so the next scheduled run tries again (OnFailure alerted; journalctl -u <unit>)' >&2
  fi
  return 0
}

deploy_paused_dags_on_exit() {
  local status="$1" phase="$2" sha="$3"
  ((status != 0)) || return 0
  case "${phase}" in
    before-switch)
      if deploy_unpause_enabled; then
        printf 'foundation-deploy: the deploy of %s stopped (exit %s) before the release switch; the running release is unchanged and its DAGs are unpaused again\n' \
          "${sha}" "${status}" >&2
      else
        printf 'foundation-deploy: THE DAGS ARE STILL PAUSED: the deploy of %s stopped (exit %s) before the release switch and unpausing them failed; unpause the enabled jobs of orchestration/jobs.v1.json by hand\n' \
          "${sha}" "${status}" >&2
        return 1
      fi
      ;;
    switching)
      printf 'foundation-deploy: THE HELD DAGS STAY PAUSED: the deploy of %s stopped (exit %s) while activating, migrating or loading the new release; the jobs it does not hold run on; fix it and deploy again, or unpause the enabled jobs of orchestration/jobs.v1.json by hand once the release is known good\n' \
        "${sha}" "${status}" >&2
      return 1
      ;;
    after-switch)
      printf 'foundation-deploy: DAGS MAY STILL BE PAUSED: %s is deployed but unpausing its DAGs failed (exit %s); unpause the enabled jobs of orchestration/jobs.v1.json by hand\n' \
        "${sha}" "${status}" >&2
      return 1
      ;;
  esac
  return 0
}
