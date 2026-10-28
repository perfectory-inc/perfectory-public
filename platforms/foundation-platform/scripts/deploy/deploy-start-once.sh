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
#
# On 2026-10-09 the sweep's post-deploy run failed in three deploys in a row; each deploy exited at
# step 6 before step 7, every DAG (17 of 17) stayed paused, and nothing scheduled ran for about three
# hours until a person unpaused them by hand.

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

# The enabled job ids whose DAG Airflow lists as paused, in the order given on stdin; every one of them
# when Airflow's list cannot be read. One question instead of an unpause per DAG (root ADR-0308): on
# 2026-10-27 unpausing 33 DAGs one by one took 55 s, and a deploy that drains only what its release
# holds leaves most of them unpaused.
deploy_paused_of() {
  local listed
  listed="$(airflow dags list -o json 2>/dev/null)" || listed=""
  python3 -I -c '
import json, sys
order = sys.stdin.read().split()
try:
    rows = json.loads(sys.argv[1])
    paused = {row["dag_id"].removeprefix("foundation_") for row in rows if str(row["is_paused"]).lower() == "true"}
except Exception:
    paused = set(order)
sys.stdout.write("".join(f"{job}\n" for job in order if job in paused))
' "${listed}"
}

deploy_unpause_enabled() {
  local id order count=0
  local -a enabled
  order="$(jobs_of enabled-consumers-first)"
  # A filter that fails leaves nothing paused behind: every enabled DAG is unpaused, as before.
  mapfile -t enabled < <(deploy_paused_of <<<"${order}" || printf '%s\n' "${order}")
  for id in "${enabled[@]}"; do
    [[ -n "${id}" ]] || continue
    airflow dags unpause "foundation_${id}" >/dev/null || return 1
    count=$((count + 1))
  done
  echo "unpaused ${count} DAGs"
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

# deploy_step_budget <step> <started at, $SECONDS>   a step that runs on every deploy states why and how
#     long it may take (config/deploy-steps.contract.json, root ADR-0308); over that, a line on stderr
#     says so. A release without the contract says nothing.
deploy_step_budget() {
  # shellcheck disable=SC2154 # release_root is the caller's (foundation-deploy.sh)
  local steps="${release_root}/current/scripts/deploy/deploy_steps.py"
  [[ -f "${steps}" ]] || return 0
  python3 -I -B "${steps}" budget "${release_root}/current" "$1" "$((SECONDS - $2))" || true
}

start_once_then_unpause() {
  local sha="$1" entry service result started="${SECONDS}"
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
  deploy_step_budget start-once "${started}"

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
      printf 'foundation-deploy: THE HELD DAGS STAY PAUSED: the deploy of %s stopped (exit %s) while activating, migrating or loading the new release; the jobs it does not hold run on; fix it and deploy again, or unpause the enabled jobs of orchestration/jobs.v1.json by hand once the release is known good\n' \
        "${sha}" "${status}" >&2
      ;;
    after-switch)
      printf 'foundation-deploy: DAGS MAY STILL BE PAUSED: %s is deployed but unpausing its DAGs failed (exit %s); unpause the enabled jobs of orchestration/jobs.v1.json by hand\n' \
        "${sha}" "${status}" >&2
      ;;
  esac
  return 0
}
