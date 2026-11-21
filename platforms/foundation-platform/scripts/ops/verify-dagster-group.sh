#!/usr/bin/env bash
# What the switch of a group to Dagster did, read only (root ADR-0583 §6). An operator runs it after the
# deploy that switches a group, and every hour of the 24 after a Gold switch:
#
#   verify-dagster-group.sh <gold|silver|collect|operations> <switched at, ISO 8601 UTC>
#
# It asks, and writes nothing:
# - Dagster's instance, in the code server's container, which mounts the releases
#   (orchestration/dagster/verify_group.py of the current release): each job's runs since the switch,
#   launch failures, a decision started twice, and the expected runs no run answered (the stop rule: a
#   job that missed two opens the rollback PR first, §3);
# - Airflow: none of the group's jobs has a DAG it may run (an unpaused one);
# - the daemon's and the code server's memory against their caps (§8);
# - for Gold, Prometheus: each by-PNU lane and tile unit's `behind` series (asset_reconcile.py) now and the
#   same hour a week before. Root ADR-0583 §6 named the manifests' age; `behind` is what ADR-0444 measures
#   serving by (a monthly tile edition's age only grows), and the tiles have it where they have no age.
# Exit 1 when the stop rule trips, a job of the group is not at `dagster`, or Airflow lists its DAG unpaused.
set -euo pipefail

group="${1:?group}" since="${2:?switched at}"
RELEASE="${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform/current}"
DAEMON="${FOUNDATION_DAGSTER_DAEMON_CONTAINER:-airflow-dagster-daemon-1}"
CODE_SERVER="${FOUNDATION_DAGSTER_CODE_SERVER_CONTAINER:-airflow-dagster-code-server-1}"
SCHEDULER="${FOUNDATION_AIRFLOW_SCHEDULER_CONTAINER:-airflow-airflow-scheduler-1}"
PROMETHEUS="${FOUNDATION_PROMETHEUS_URL:-http://127.0.0.1:19090}"
status=0

query() {
  curl -fsS --max-time 20 -G "${PROMETHEUS}/api/v1/query" --data-urlencode "query=$1"
}

serving='{"serving": []}'
if [[ "${group}" == gold ]]; then
  lanes_now="$(query 'foundation_by_pnu_lane_behind_seconds')"
  lanes_week="$(query 'foundation_by_pnu_lane_behind_seconds offset 7d')"
  units_now="$(query 'foundation_tile_unit_behind_seconds')"
  units_week="$(query 'foundation_tile_unit_behind_seconds offset 7d')"
  serving="$(python3 -I -c '
import json, sys
def rows(text, label):
    return {tuple(row["metric"].get(key, "") for key in label): float(row["value"][1])
            for row in json.loads(text)["data"]["result"]}
lanes_now, lanes_week, units_now, units_week = (rows(sys.argv[i], keys) for i, keys in
    ((1, ("lane", "bake_job")), (2, ("lane", "bake_job")), (3, ("unit",)), (4, ("unit",))))
out = [{"kind": "by_pnu", "name": lane, "job": job, "now": value, "week_before": lanes_week.get((lane, job))}
       for (lane, job), value in sorted(lanes_now.items())]
out += [{"kind": "tile", "name": unit, "now": value, "week_before": units_week.get((unit,))}
        for (unit,), value in sorted(units_now.items())]
print(json.dumps({"serving": out}))
' "${lanes_now}" "${lanes_week}" "${units_now}" "${units_week}")"
fi

printf '%s' "${serving}" | docker exec -i "${CODE_SERVER}" python -I "${RELEASE}/orchestration/dagster/verify_group.py" \
  "${group}" "${since}" || status=1

# Airflow builds no DAG for a job Dagster decides (orchestration/dags/foundation_jobs.py); none may be unpaused.
still="$(docker exec "${SCHEDULER}" airflow dags list -o json 2>/dev/null | python3 -I -c '
import json, sys
group = sys.argv[2]
jobs = json.load(open(sys.argv[1], encoding="utf-8"))
ids = {jobs["dag_id_prefix"] + job["id"] for job in jobs["jobs"] if job.get("dagster_group") == group and job["enabled"]}
# A DAG the new release stops building stays listed (stale), paused since step 0 of the switch: it decides
# nothing. One still unpaused would.
print(" ".join(sorted(ids & {row["dag_id"] for row in json.load(sys.stdin) if str(row["is_paused"]).lower() != "true"})))
' "${RELEASE}/orchestration/jobs.v1.json" "${group}")"
if [[ -n "${still}" ]]; then
  echo "verify-dagster-group: Airflow lists unpaused ${still}" >&2
  status=1
fi
echo "{\"airflow_dags_left\": \"${still}\"}"

for container in "${DAEMON}" "${CODE_SERVER}"; do
  docker stats --no-stream --format '{"container": "{{.Name}}", "memory": "{{.MemUsage}}"}' "${container}"
done
exit "${status}"
