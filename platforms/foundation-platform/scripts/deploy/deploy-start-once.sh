#!/usr/bin/env bash
# What foundation-deploy.sh does about the DAGs it paused (root ADR-0159 §4 as amended by root
# ADR-0173), sourced so it can be run against stand-ins. The caller defines
#
#   airflow <args...>   Airflow's CLI in the scheduler container
#   jobs_of <which>     the job registry's lists: `enabled` ids, `enabled-consumers-first` ids in
#                       unpause order, `started-once` "<service> <minutes> <job id>" (the id names
#                       its step start-once.<id> in config/deploy-steps.contract.json, root ADR-0308;
#                       a registry from before it prints no id, and such a job always starts),
#                       `deferred` ids of enabled jobs owed a run: a release build kept them from
#                       starting (root ADR-0260) or step 0 stopped them (root ADR-0308)
#   release_root        the release tree root; deploy_step asks its current release's deploy_steps.py
#
# start_once_then_unpause <sha>   steps 6 and 7. Starts once, with the held DAGs still paused, each job the
#     registry marks `started_once_after_deploy` whose step says it has work (it never succeeded here
#     or the release changed what it runs; deploy_steps.py), then unpauses every enabled DAG whatever
#     those runs did, and starts each job owed a run. A run that does not succeed (or outlives its limit) does not undo the deploy and does not
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
  local -a enabled listed
  local -A held=()
  mapfile -t listed <"$1"
  for id in "${listed[@]}"; do held["${id}"]=1; done
  if [[ -n "${held["*"]-}" ]]; then
    echo "every DAG stays paused until the release is live: it brings a Postgres migration, or what it changes could not be told"
    return 0
  fi
  mapfile -t enabled < <(jobs_of enabled-consumers-first)
  for id in "${enabled[@]}"; do
    [[ -z "${held["${id}"]-}" ]] || continue
    airflow dags unpause "foundation_${id}" >/dev/null || return 1
    count=$((count + 1))
  done
  echo "unpaused ${count} DAGs; held until the release is live: ${listed[*]:-none}"
}

# The enabled job ids (the arguments, in their order) whose DAG may be paused: every one Airflow does
# not list as unpaused. A DAG Airflow has not parsed yet (a job the new release adds) is not listed,
# and Airflow creates it paused, so it is unpaused too; a list that cannot be read names every one.
# One question instead of an unpause per DAG (root ADR-0308): on 2026-10-27 unpausing 33 DAGs one by
# one took 55 s, and a deploy that drains only what its release holds leaves most of them unpaused.
# The list goes on stdin, not as an argument (an argument is at most 128 KiB).
deploy_paused_of() {
  { airflow dags list -o json 2>/dev/null || true; } | python3 -I -c '
import json, sys
order = sys.argv[1:]
try:
    rows = json.load(sys.stdin)
    unpaused = {row["dag_id"].removeprefix("foundation_") for row in rows if str(row["is_paused"]).lower() == "false"}
except Exception:
    unpaused = set()
sys.stdout.write("".join(f"{job}\n" for job in order if job not in unpaused))
' "$@"
}

deploy_unpause_enabled() {
  local id count=0
  local -a order enabled
  mapfile -t order < <(jobs_of enabled-consumers-first)
  # A filter that fails leaves nothing paused behind: every enabled DAG is unpaused, as before.
  mapfile -t enabled < <(deploy_paused_of "${order[@]}" || printf '%s\n' "${order[@]}")
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

# deploy_step <should-run|ran> <step>   config/deploy-steps.contract.json of the release now current
#     (root ADR-0308). should-run answers 3 only when the step may be skipped; a release without
#     the contract, or any failure to answer, runs it.
deploy_step() {
  # shellcheck disable=SC2154 # release_root is the caller's (foundation-deploy.sh)
  local steps="${release_root}/current/scripts/deploy/deploy_steps.py"
  [[ -f "${steps}" ]] || return 0
  python3 -I -B "${steps}" "$1" "${release_root}/current" "$2"
}

start_once_then_unpause() {
  local sha="$1" entry service minutes id result gate
  local -a once failed=()
  mapfile -t once < <(jobs_of started-once)
  for entry in "${once[@]}"; do
    read -r service minutes id <<<"${entry}"
    # Only when the release changes what the job runs, or it never succeeded here (2026-10-28: about
    # ten extra legal-dong collections a day, 55 MB each, came from this step, not the schedule).
    gate=0
    [[ -z "${id}" ]] || deploy_step should-run "start-once.${id}" || gate=$?
    if ((gate == 3)); then
      printf '%-45s %s\n' "${service}" "skipped"
      continue
    fi
    # The job's own limit from the registry, and a minute for systemd to report it.
    timeout "$(( minutes * 60 + 60 ))" systemctl start "${service}" || true
    result="$(systemctl show -p Result --value "${service}")" || result=unknown
    printf '%-45s %s\n' "${service}" "${result}"
    if [[ "${result}" == success ]]; then
      [[ -z "${id}" ]] || deploy_step ran "start-once.${id}" >/dev/null || true
    else
      failed+=("${service}")
    fi
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
