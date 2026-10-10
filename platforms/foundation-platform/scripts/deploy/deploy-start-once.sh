#!/usr/bin/env bash
# What foundation-deploy.sh does about the DAGs it paused (root ADR-0159 §4 as amended by root
# ADR-0173), sourced so it can be run against stand-ins. The caller defines
#
#   airflow <args...>   Airflow's CLI in the scheduler container
#   jobs_of <which>     the job registry's lists: `enabled` ids, `started-once` "<service> <minutes>"
#
# start_once_then_unpause <sha>   steps 6 and 7. Starts once, with every DAG still paused, each job the
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
#
# On 2026-10-09 the sweep's post-deploy run failed in three deploys in a row; each deploy exited at
# step 6 before step 7, every DAG (17 of 17) stayed paused, and nothing scheduled ran for about three
# hours until a person unpaused them by hand.

deploy_unpause_enabled() {
  local id
  local -a enabled
  mapfile -t enabled < <(jobs_of enabled-consumers-first)
  for id in "${enabled[@]}"; do
    airflow dags unpause "foundation_${id}" >/dev/null || return 1
  done
  echo "unpaused ${#enabled[@]} DAGs"
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
      fi
      ;;
    switching)
      printf 'foundation-deploy: THE DAGS STAY PAUSED: the deploy of %s stopped (exit %s) while activating, migrating or loading the new release; fix it and deploy again, or unpause the enabled jobs of orchestration/jobs.v1.json by hand once the release is known good\n' \
        "${sha}" "${status}" >&2
      ;;
    after-switch)
      printf 'foundation-deploy: DAGS MAY STILL BE PAUSED: %s is deployed but unpausing its DAGs failed (exit %s); unpause the enabled jobs of orchestration/jobs.v1.json by hand\n' \
        "${sha}" "${status}" >&2
      ;;
  esac
  return 0
}
