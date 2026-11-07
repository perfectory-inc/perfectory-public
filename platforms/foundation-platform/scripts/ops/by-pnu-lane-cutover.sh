#!/usr/bin/env bash
# The supervised first cut-over of the pack-only by-PNU lanes (root ADR-0190, ADR-0193 §6), one lane
# after another, each through the operator script's own steps (root ADR-0161, ADR-0166):
#
#   by-pnu-lane-cutover.sh <a by_pnu_lanes lane|lanes|lane,lane,...> <generation> [--dry-run|--start-budget]
#
# Started only by `by-pnu-pack-operator.sh <lane|lanes|lane,lane,...> cutover <generation>` (as root,
# in its own transient unit) or, with --dry-run, run by it in the caller's terminal. `lanes` is every
# lane of the contract that is not cut over yet, in contract order; `lane,lane,...` is those lanes
# that are not cut over yet, in the order given, so the operator puts the Gold builds that fit the
# operator window first and an untimed heavy one last (2026-11-06: five lanes waited for their first
# publish, and contract order put the untimed 124-million-pair regulation-zones Gold second); a
# lane is cut over once its monitor
# file (runtime-secrets group <lane>-serving-monitor, or the kinds' one group, root ADR-0193 §9)
# names its sample, which only `monitor-sample` writes.
#
# For each lane, in order, each step only after the one before passed:
#
#   history    the lane line's first_publish_waits_for_history (root ADR-0350 §6, ADR-0371 §5): every
#              month of those 실거래 datasets is loaded (scripts/ops/serving_history_readiness.py,
#              read with the operations map's reader token); a lane without it is `not-declared`
#   preview    the lane's and its latency baseline lane's preview hostnames answer their
#              capabilities path (gate (나) reads both)
#   gold       the operator's `<lane> gold-rebuild`: gold-panel-rebuild.sh <the table's unit>, the
#              scheduled unit's definition; a Gold whose inputs did not move is "nothing to do".
#              Its window budget is 60 min, or, for a lane whose own Gold job
#              (foundation-gold-lane-rebuild@<unit>) has no measured run in orchestration/jobs.v1.json
#              (per_run_cap.measured_run_seconds, root ADR-0386), the whole time limit of the unit
#              the step starts (gold_panel_rebuild's time_limit_minutes): an untimed first full build
#              is bounded only by that limit, and the panel rebuild ran to its 160-minute limit ten
#              times after 2026-10-20 (its per_run_cap), far past a 60-minute guess
#   parity     the producer's check against the bundle Gold, when the rebuild contract declares one
#              for the table (`pins_from.parity_argument`): a full rebuild refuses, before it
#              writes, a kind that differs from the bundle on any PNU (ADR-0193 §6-2). An
#              incremental commit does not run it, so this step passes only when the Gold head was
#              committed by a full rebuild that carried the parity argument: the newest run of the
#              table's unit in the rebuild's state directory that wrote the table (its summary is
#              not `validate_only`) is `full` and its plan names the argument. That run is this
#              one's gold step, or (gold says "nothing to do") the earlier run that made the head,
#              e.g. the scheduled kind rebuild. An incremental head stops the run. A table without
#              one is reported `not-declared`, never `passed`.
#   bake, equality, latency, publish, monitor-sample
#              the operator's steps of that name for <generation>
#
# The first failing step stops the run with its reason; nothing after it runs, no later lane starts.
# Every step's start, duration and outcome is one JSON line in
# <bake root>/cutover/<run id>.jsonl, and the log says the same.
#
# Two guards around every step that is not read-only:
#   - the daily scheduled window: no step starts unless it would end, at its time budget below,
#     before the first daily Spark job of orchestration/jobs.v1.json starts, or after the last one
#     started plus the tail. The window is job_units.py's (scheduled_window), computed from that
#     file; the scheduler's runner keeps the clock-started batch it places inside it out of the rest
#     of the day, the operator window (start-scheduled-job.sh, root ADR-0425).
#   - one Spark job at a time, by capacity (root ADR-0435): the run claims the whole spark pool in
#     the host's capacity queue for as long as its unit runs (jobs.v1.json capacity_claimants
#     by_pnu_lane_cutover, class `publish`), so no start of a lower class, and no later start of its
#     own, takes the pool while it waits, and a running backfill yields at its next step boundary.
#     Before `gold` and `bake` the run waits until no unit of a spark-pool job, Gold rebuild or pack
#     bake runs. Nothing new can start under the claim, so the wait is bounded by the longest run
#     limit (time_limit_minutes + stop_grace_minutes) of the jobs that ran when it began; a wait
#     past it means the host is wedged, and the run fails and pages. The operator refuses again on
#     its own when one does (exit 75).
#
# --dry-run prints the lanes, their steps, each step's command and window verdict and the preview
# answers; it starts, writes and waits for nothing. It exits 1 when a step could not start now.
# --start-budget prints the minutes the first lane's steps may take, the sum of their budgets below
# (0 when every lane is cut over): the run-now dispatcher starts an approved cut-over only when the
# operator window has that much room left, else when it next opens (root ADR-0433 §3). It reads the
# contracts only.
#
# A cut-over approved in the console (root ADR-0433) carries its approval id in
# FOUNDATION_CUTOVER_APPROVAL (set by by-pnu-pack-operator.sh --approved): each step's line and the
# run's end are also appended to that approval's progress (scripts/ops/release-progress.py), which
# the console shows.
set -euo pipefail
log() { printf '%s by-pnu-lane-cutover: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
refuse() { log "refused: $1"; exit "${2:-64}"; }
USAGE="usage: by-pnu-lane-cutover.sh <a by_pnu_lanes lane|lanes|lane,lane,...> <generation> [--dry-run|--start-budget]"

TARGET="${1:-}" GENERATION="${2:-}" DRY_RUN="" START_BUDGET=""
[[ "$#" == 2 || ( "$#" == 3 && ( "${3}" == --dry-run || "${3}" == --start-budget ) ) ]] || refuse "${USAGE}"
[[ "${3:-}" == --dry-run ]] && DRY_RUN=1
[[ "${3:-}" == --start-budget ]] && START_BUDGET=1
[[ "${GENERATION}" =~ ^[1-9][0-9]{0,3}$ ]] || refuse "${USAGE}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PLATFORM_ROOT="$(cd "${here}/../.." && pwd -P)"
OPERATOR="${here}/by-pnu-pack-operator.sh"
BAKE_ROOT="${FOUNDATION_OPERATOR_BAKE_ROOT:-/data/foundation-platform/by-pnu-bake}"
ETC="${FOUNDATION_OPERATOR_ETC:-/etc/foundation-platform}"
# by-pnu-pack-operator.sh's GOLD_LOG: its gold-rebuild unit's output.
GOLD_LOG="${BAKE_ROOT}/gold-rebuild/logs/gold-rebuild.log"
# gold-panel-rebuild.sh's state root: one directory per run of each unit, its plan and summary.
GOLD_STATE_ROOT="${FOUNDATION_GOLD_REBUILD_STATE_ROOT:-/var/lib/foundation-gold-panel-rebuild}"
# The FOUNDATION_CUTOVER_* overrides exist for the tests; the operator's sudo grant resets them.
POLL_SECONDS="${FOUNDATION_CUTOVER_POLL_SECONDS:-60}"
# A test's bound on the Spark wait; on the host the wait is bounded by what runs (above).
SPARK_WAIT_MINUTES="${FOUNDATION_CUTOVER_SPARK_WAIT_MINUTES:-}"
now() { echo "${FOUNDATION_CUTOVER_NOW:-$(date -u +%s)}"; }

# Each step's time budget in minutes: the window guard starts a step only if this much time is
# left before the window. Gold and bake are twice ADR-0193 §7's targets (Gold full ≤ 30 min for the
# heaviest kind, bake generation ≤ 30 min), Gold an untimed lane's unit limit instead (the lane line's
# gold budget, see `gold` above); the gates are the publisher's, measured at the parcel and building
# cut-overs well inside these.
declare -A BUDGET=([history]=0 [preview]=1 [gold]=60 [parity]=0 [bake]=60 [equality]=30 [latency]=20 [publish]=30
  [monitor-sample]=1)
STEPS=(history preview gold parity bake equality latency publish monitor-sample)

# One line per lane to cut over: lane, gold table, rebuild unit, parity argument or -, preview
# hostname, its capabilities path, baseline preview hostname, its capabilities path, monitor file,
# the 실거래 datasets whose history it waits for (comma-separated) or -, its gold step's budget.
# Then a last line: the window's start and end in minutes of the UTC day, and the spark-pool units.
mapfile -t PLAN < <(python3 -I - "${PLATFORM_ROOT}" "${TARGET}" "${ETC}" <<'PY'
import json, os, sys
root, target, etc = sys.argv[1], sys.argv[2], sys.argv[3]
def load(path):
    with open(os.path.join(root, path), encoding="utf-8") as handle:
        return json.load(handle)
r2 = load("config/r2-connections.contract.json")
# Every table the rebuild builds and every lane's fields, a kind line's derived (root ADR-0193 §9).
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(root, "infra/lakehouse/spark/jobs"))
sys.path.insert(0, os.path.join(root, "scripts/deploy"))
import by_pnu_kinds, gold_rebuild, job_units  # noqa: E402
rebuild = gold_rebuild.load_contract()["tables"]
groups = {group["name"]: group["path"] for group in load("config/runtime-secrets.contract.json")["groups"]}
jobs = load("orchestration/jobs.v1.json")["jobs"]
lanes = r2.get("by_pnu_lanes", {})

def block(lane):
    """The lane's gateway block and capabilities path: its own entry, or (root ADR-0193 §4) its
    host's block with `{kind}/` before the `by-pnu` segment."""
    if lane in ("parcel", "building"):
        own = r2[f"{lane}_by_pnu_gateway"]
        return own, own["request_path"]["capabilities"]
    entry = lanes[lane]
    if "host" in entry:
        host = r2[entry["host"]]
        path = host["request_path"]["capabilities"].replace("/by-pnu/", f"/{entry['kind']}/by-pnu/", 1)
        return host, path
    return entry, entry["request_path"]["capabilities"]

chosen = list(lanes) if target == "lanes" else target.split(",")
if [lane for lane in chosen if lane not in lanes] or len(set(chosen)) != len(chosen):
    sys.exit(f"{target!r} is not a by_pnu_lanes lane of the contract ({', '.join(lanes)}), `lanes`,"
             " or such lanes listed once each as lane,lane,...")
# A list, like `lanes`, leaves out the lanes cut over already; one named lane is cut over again.
listed = target == "lanes" or len(chosen) > 1
by_service = {job["systemd_service"]: job for job in jobs}
panel_limit = next(job["time_limit_minutes"] for job in jobs if job["id"] == "gold_panel_rebuild")
for lane in chosen:
    entry = by_pnu_kinds.lane_fields(lane)
    follows = entry["documents_follow_lane"]
    if follows in lanes and follows in chosen and chosen.index(follows) > chosen.index(lane):
        sys.exit(f"{lane} follows {follows}, which this run cuts over after it")
    monitor = os.path.join(etc, os.path.basename(groups[entry["monitor_group"]]))
    # Cut over already: its monitor file names its sample (the kinds share one file, a line each).
    sample = f"FOUNDATION_PLATFORM_{lane.replace('-', '_').upper()}_BY_PNU_SERVING_MONITOR_SAMPLE_PATH="
    if listed and os.path.exists(monitor) and any(
            line.startswith(sample) for line in open(monitor, encoding="utf-8")):
        continue
    table = rebuild.get(entry["gold_table"])
    if table is None:
        sys.exit(f"{lane}: its Gold {entry['gold_table']} is not a table of gold-panel-rebuild.contract.json")
    own, capabilities = block(lane)
    base, base_capabilities = block(entry["latency_baseline_lane"])
    parity = (table.get("pins_from") or {}).get("parity_argument") or "-"
    # The step starts a copy of the panel rebuild's unit, whose limit bounds an untimed build.
    gold_job = by_service.get(f"foundation-gold-lane-rebuild@{table['unit']}.service")
    timed = gold_job is None or (gold_job.get("per_run_cap") or {}).get("measured_run_seconds") is not None
    print(lane, entry["gold_table"], table["unit"], parity,
          own["section_packs"]["preview_worker"]["public_hostname"], capabilities,
          base["section_packs"]["preview_worker"]["public_hostname"], base_capabilities, monitor,
          ",".join(lanes.get(lane, {}).get("first_publish_waits_for_history") or []) or "-",
          60 if timed else panel_limit)

# The daily scheduled window (job_units.py). Hourly jobs (hour `*`) are not the window; the Spark
# wait catches them.
units = sorted({job["systemd_service"] for job in jobs if job.get("pool") == "spark"})
# Each spark-pool unit's run limit, for the Spark wait's bound; the operator's own Gold and pack bake
# units are bounded like the longest job.
limits = {job["systemd_service"]: job["time_limit_minutes"] + job["stop_grace_minutes"]
          for job in jobs if job.get("pool") == "spark"}
print("limits", max(limits.values()), " ".join(f"{unit}={minutes}" for unit, minutes in sorted(limits.items())))
try:
    start, end = job_units.scheduled_window(jobs)
except ValueError as error:
    sys.exit(f"orchestration/jobs.v1.json: {error}")
print("window", start, end, " ".join(units))
PY
) || refuse "cannot read the lanes to cut over from the contracts" 65
((${#PLAN[@]} > 0)) || refuse "cannot read the lanes to cut over from the contracts" 65
read -r _ WINDOW_START WINDOW_END SPARK_UNITS <<<"${PLAN[-1]}"
unset 'PLAN[-1]'
read -r _ LONGEST_LIMIT UNIT_LIMITS <<<"${PLAN[-1]}"
unset 'PLAN[-1]'
# Units that run Spark: every spark-pool job, the operator's Gold rebuild and the pack bakes.
read -ra SPARK_UNITS <<<"${SPARK_UNITS} foundation-gold-panel-rebuild-unconditional.service foundation-*-pack-bake-g*.service"
hhmm() { printf '%02d:%02d' "$(($1 / 60))" "$(($1 % 60))"; }
budget() { [[ "$1" == gold ]] && echo "${gold_budget}" || echo "${BUDGET[$1]}"; }

# Whether a step of $1 minutes may start now: it may not start inside the window, and may not run
# into it. Minutes of the UTC day; the window does not wrap midnight.
window_verdict() {
  local start=$(( ($(now) / 60) % 1440 )) end
  end=$((start + $1))
  if ((start >= WINDOW_START && start < WINDOW_END)); then
    echo "inside the scheduled window $(hhmm "${WINDOW_START}")-$(hhmm "${WINDOW_END}") UTC"
  elif ((start < WINDOW_START && end > WINDOW_START)) || ((end > 1440 + WINDOW_START)); then
    echo "would run into the scheduled window $(hhmm "${WINDOW_START}")-$(hhmm "${WINDOW_END}") UTC (budget $1 min)"
  fi
}
spark_running() {
  python3 -I -B "${PLATFORM_ROOT}/scripts/deploy/job_units.py" running "${SPARK_UNITS[@]}" \
    || refuse "systemd cannot tell whether a Spark job runs" 70
}
preview_answers() {
  curl -sS -o /dev/null -w '%{http_code}' --max-time 20 "https://$1$2" 2>/dev/null || true
}

if [[ -n "${START_BUDGET}" ]]; then
  total=0
  if ((${#PLAN[@]})); then
    read -r lane table unit parity preview path base base_path monitor history gold_budget <<<"${PLAN[0]}"
    for step in "${STEPS[@]}"; do total=$((total + $(budget "${step}"))); done
  fi
  echo "${total}"
  exit 0
fi

APPROVAL="${FOUNDATION_CUTOVER_APPROVAL:-}"
[[ -z "${APPROVAL}" || "${APPROVAL}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]   || refuse "FOUNDATION_CUTOVER_APPROVAL is not an approval id"
# The approval's progress line, when there is one; a line that cannot be written does not stop the
# run (the evidence file holds every step).
progress() {
  [[ -n "${APPROVAL}" ]] || return 0
  python3 -I -B "${here}/release-progress.py" "${APPROVAL}" "$@" || log "could not append to the progress of ${APPROVAL}"
}

if [[ -n "${DRY_RUN}" ]]; then
  blocked=0
  log "dry run: generation ${GENERATION}; scheduled window $(hhmm "${WINDOW_START}")-$(hhmm "${WINDOW_END}") UTC; nothing is started"
  ((${#PLAN[@]})) || log "dry run: every lane is cut over already"
  running="$(spark_running)"
  [[ -z "${running}" ]] || log "dry run: Spark runs now ($(cut -d' ' -f1 <<<"${running}" | paste -sd' ')); gold and bake would wait"
  for entry in ${PLAN[@]+"${PLAN[@]}"}; do
    read -r lane table unit parity preview path base base_path monitor history gold_budget <<<"${entry}"
    for host_path in "${preview}${path}" "${base}${base_path}"; do
      code="$(preview_answers "${host_path%%/*}" "/${host_path#*/}")"
      log "dry run ${lane}: https://${host_path} answers ${code:-nothing}"
      [[ "${code}" == 200 ]] || blocked=1
    done
    for step in "${STEPS[@]}"; do
      case "${step}" in
        history) [[ "${history}" == - ]] && what="not-declared: the lane waits for no history"                    || what="serving_history_readiness.py ${lane}: every month of ${history} is loaded" ;;
        preview) what="GET the two capabilities paths above" ;;
        gold) what="by-pnu-pack-operator.sh ${lane} gold-rebuild (gold-panel-rebuild.sh ${unit}, ${table}; budget ${gold_budget} min)" ;;
        parity) [[ "${parity}" == - ]] && what="not-declared: ${table} has no pins_from.parity_argument" \
                  || what="passes only if ${table}'s head was committed by a full rebuild, whose ${parity} checks every PNU" ;;
        *) what="by-pnu-pack-operator.sh ${lane} ${step} ${GENERATION}" ;;
      esac
      verdict="$(window_verdict "$(budget "${step}")")"
      log "dry run ${lane} ${step}: ${what}${verdict:+ — would not start now: ${verdict}}"
      [[ -z "${verdict}" ]] || blocked=1
    done
  done
  exit "${blocked}"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE="${BAKE_ROOT}/cutover/${RUN_ID}.jsonl"
mkdir -p "${EVIDENCE%/*}"
# The run's claim on the spark pool (root ADR-0435), for as long as its unit runs; taken off when it
# ends. A claim that cannot be queued stops the run: without it a backfill keeps the pool.
JOB_UNITS="${PLATFORM_ROOT}/scripts/deploy/job_units.py"
JOBS_FILE="${PLATFORM_ROOT}/orchestration/jobs.v1.json"
CLAIM_UNIT="foundation-by-pnu-lane-cutover-g${GENERATION}.service"
python3 -I -B "${JOB_UNITS}" claim "${JOBS_FILE}" by_pnu_lane_cutover "${CLAIM_UNIT}" \
  || refuse "cannot queue the run's claim on the spark pool" 70
trap 'python3 -I -B "${JOB_UNITS}" release by_pnu_lane_cutover || true' EXIT
# An operator's stop (systemctl stop of this unit, root ADR-0433 §1) ends the run between steps: the
# step in progress runs on in its own unit, nothing after it starts.
trap 'progress finished outcome=failed "reason=stopped by an operator: the step in progress finishes in its own unit, nothing after it starts" "run=${RUN_ID}"; exit 143' TERM
record() {  # lane step outcome seconds reason
  progress step "lane=$1" "step=$2" "outcome=$3" "seconds=$4" "reason=$5" "run=${RUN_ID}"
  python3 -I -c 'import json,sys; print(json.dumps({"lane": sys.argv[1], "step": sys.argv[2], "outcome": sys.argv[3], "seconds": int(sys.argv[4]), "reason": sys.argv[5], "generation": int(sys.argv[6])}))' \
    "$@" "${GENERATION}" >>"${EVIDENCE}"
}
fail() {  # lane step seconds reason
  record "$1" "$2" failed "$3" "$4"
  log "${1} ${2}: FAILED after ${3}s: ${4}; the run stops here (evidence ${EVIDENCE})"
  progress finished outcome=failed "reason=${1} ${2}: ${4}" "run=${RUN_ID}"
  exit 1
}
# The longest run limit of the units that run now: nothing starts under the claim, so they end by then.
wait_bound() {  # running lines "<unit> <state>"
  local unit _ longest=0 limit
  while read -r unit _; do
    [[ -n "${unit}" ]] || continue
    limit="${LONGEST_LIMIT}"
    for entry in ${UNIT_LIMITS}; do [[ "${entry%=*}" == "${unit}" ]] && limit="${entry#*=}"; done
    ((limit > longest)) && longest="${limit}"
  done <<<"$1"
  echo "${longest}"
}
wait_for_spark() {  # lane step
  local waited=0 running bound=""
  while :; do
    running="$(spark_running)"
    [[ -n "${running}" ]] || return 0
    [[ -n "${bound}" ]] || bound="${SPARK_WAIT_MINUTES:-$(wait_bound "${running}")}"
    ((waited < bound * 60)) || fail "$1" "$2" 0 "Spark still runs after ${bound} min, the longest run limit of what ran when the wait began, with the pool claimed: $(cut -d' ' -f1 <<<"${running}" | paste -sd' ')"
    log "${1} ${2}: waiting for $(cut -d' ' -f1 <<<"${running}" | paste -sd' ')"
    sleep "${POLL_SECONDS}"
    waited=$((waited + POLL_SECONDS))
  done
}

log "run ${RUN_ID}: generation ${GENERATION}, lanes: $(for e in ${PLAN[@]+"${PLAN[@]}"}; do printf '%s ' "${e%% *}"; done)"
((${#PLAN[@]})) || { log "every lane is cut over already; nothing to do"
  progress finished outcome=passed "reason=every lane is cut over already" "run=${RUN_ID}"; exit 0; }
for entry in "${PLAN[@]}"; do
  read -r lane table unit parity preview path base base_path monitor history gold_budget <<<"${entry}"
  lane_started="$(now)" gold_output=""
  for step in "${STEPS[@]}"; do
    [[ "${step}" == gold || "${step}" == bake ]] && wait_for_spark "${lane}" "${step}"
    verdict="$(window_verdict "$(budget "${step}")")"
    [[ -z "${verdict}" ]] || fail "${lane}" "${step}" 0 "not started: ${verdict}"
    started="$(now)" reason=""
    log "${lane} ${step}: starting"
    case "${step}" in
      history)
        if [[ "${history}" == - ]]; then
          record "${lane}" "${step}" not-declared 0 "the lane line waits for no history"
          log "${lane} history: not-declared"
          continue
        fi
        reason="$( (set -a; [[ ! -r "${ETC}/operations-map.env" ]] || . "${ETC}/operations-map.env"; set +a
                    python3 -I -B "${PLATFORM_ROOT}/scripts/ops/serving_history_readiness.py" "${lane}" 2>&1) )"           || fail "${lane}" "${step}" "$(($(now) - started))" "${reason:-serving_history_readiness.py ${lane} could not tell}"
        ;;
      preview)
        for host_path in "${preview}${path}" "${base}${base_path}"; do
          code="$(preview_answers "${host_path%%/*}" "/${host_path#*/}")"
          [[ "${code}" == 200 ]] || fail "${lane}" "${step}" "$(($(now) - started))" "https://${host_path} answers ${code:-nothing}, not 200"
        done
        reason="both previews answer 200"
        ;;
      parity)
        if [[ "${parity}" == - ]]; then
          record "${lane}" "${step}" not-declared 0 "${table} declares no parity with the bundle Gold"
          log "${lane} parity: not-declared (${table} has no pins_from.parity_argument)"
          continue
        fi
        head="$(python3 -I - "${GOLD_STATE_ROOT}/${unit}/runs" "${parity}" <<'PY'
import json, os, sys
runs, argument = sys.argv[1], sys.argv[2]
names = sorted(os.listdir(runs), reverse=True) if os.path.isdir(runs) else []
for name in names:
    try:
        summary = json.load(open(os.path.join(runs, name, "summary.json"), encoding="utf-8"))
        plan = json.load(open(os.path.join(runs, name, "plan.json"), encoding="utf-8"))
    except (OSError, ValueError):
        continue  # a run that planned "nothing to do" or failed before it wrote
    if summary.get("write_disposition") == "validate_only":
        continue  # a dry run commits nothing
    mode = (summary.get("rebuild") or {}).get("mode", "full")
    parity = argument in plan.get("producer_arguments", [])
    print(f"{'ok' if mode == 'full' and parity else 'no'} run {name}: {mode} rebuild"
          f"{'' if parity else ' without ' + argument}, {summary.get('row_count')} rows")
    sys.exit(0)
print("no run of the table wrote it")
PY
)"
        [[ "${head}" == ok\ * ]] \
          || fail "${lane}" "${step}" 0 "${table}'s head was not committed by a full rebuild with ${parity} (${head#no }); an incremental commit skips the parity with the bundle, so the head it would serve was never checked on every PNU; run a full rebuild of ${table} first"
        reason="${table}'s head: ${head#ok }, whose ${parity} checked each PNU against the bundle Gold before it committed"
        ;;
      gold)
        # What this run appends to the operator's Gold log (by-pnu-pack-operator.sh GOLD_LOG) is the
        # rebuild's own account: its `gold-kind` line and whether it committed.
        offset="$(stat -c %s "${GOLD_LOG}" 2>/dev/null || echo 0)"
        # The exit status is read first: the duration's command substitution would reset $?.
        FOUNDATION_OPERATOR_WAIT=1 "${OPERATOR}" "${lane}" gold-rebuild || { rc=$?
          fail "${lane}" "${step}" "$(($(now) - started))" "gold-panel-rebuild.sh ${unit} failed (exit ${rc}); see ${GOLD_LOG}"; }
        gold_output="$(tail -c +"$((offset + 1))" "${GOLD_LOG}" 2>/dev/null || true)"
        reason="$(grep -E "^gold-kind ${table} " <<<"${gold_output}" | tail -1 || true)"
        reason="${reason:-$(grep -E "nothing to do|committed ${table}:|recorded the new pins" <<<"${gold_output}" | tail -1 || true)}"
        ;;
      *)
        FOUNDATION_OPERATOR_WAIT=1 "${OPERATOR}" "${lane}" "${step}" "${GENERATION}" || { rc=$?
          fail "${lane}" "${step}" "$(($(now) - started))" "by-pnu-pack-operator.sh ${lane} ${step} ${GENERATION} failed (exit ${rc}); see ${BAKE_ROOT}/${lane}-pack-g${GENERATION}/logs/${step}.log"; }
        ;;
    esac
    seconds=$(($(now) - started))
    record "${lane}" "${step}" passed "${seconds}" "${reason}"
    log "${lane} ${step}: passed in ${seconds}s"
  done
  log "${lane}: cut over at generation ${GENERATION} in $(($(now) - lane_started))s"
done
log "run ${RUN_ID}: every lane cut over (evidence ${EVIDENCE})"
progress finished outcome=passed "reason=every lane cut over at generation ${GENERATION}" "run=${RUN_ID}"
