#!/usr/bin/env bash
# A by-PNU lane whose Gold is ready publishes itself (root ADR-0435): the controller of its first
# publish, the way Argo Rollouts' controller promotes a rollout whose analysis passed, with no person
# in the normal path.
#
#   by-pnu-auto-promote.sh            started by foundation-by-pnu-auto-promote.timer, as root
#
# Each run, level-triggered like the asset reconciler (root ADR-0333 §7):
#   - nothing, while a lane cut-over runs (one at a time; it holds the spark pool's claim);
#   - else the first lane not cut over yet (by-pnu-lane-cutover.sh lanes --pending) whose backoff has
#     passed is started, the lanes with the fewest attempts first, then in contract order: by-pnu-pack-operator.sh <lane> cutover <generation> --auto.
#     The runner's steps are the automated analysis: history, preview, gold, parity, bake, equality,
#     latency, publish, monitor-sample, verify (the lane at its production hostname, GET twice). The
#     first that fails stops the run before anything after it, and its unit pages.
#   - a lane still pending after an attempt is started again after a backoff that doubles from one hour
#     (2, 4, ... at most 24): a transient answer (2026-11-05 02:42Z: R2 `service error` in the latency
#     gate) is retried without anyone, a broken lane is not restarted every quarter hour. A lane never
#     tried goes before it: one at a time, a lane that keeps failing would hold back every lane behind
#     it in the contract (2026-11-07 08:58Z: parcel-regulation-zones' Gold crashed after 76 minutes, and
#     its retry was next ahead of three lanes never tried). An attempt whose gold step ran to the panel
#     Gold rebuild's time bound (orchestration/jobs.v1.json gold_panel_rebuild) does not double it: the
#     rebuild resumes from the stages it kept (root ADR-0438, ADR-0440), so the next attempt goes on
#     where it stopped (2026-11-07: parcel-regulation-zones' first national build, longer than the bound).
#     An attempt that ended `waiting` (its history or its Gold's inputs not loaded yet, root ADR-0471)
#     claimed nothing and is asked again at the next run, without a backoff: the readiness it reads
#     comes from loads that run on their own, and a doubled wait would hold the lane hours after its
#     last input landed (2026-11-09: parcel-nearby, whose seven Silver tables wait for one load).
#     An attempt that failed fast counts toward the backoff only under the release that made it (the
#     order counts them all): a new release is new evidence, the way Argo Rollouts analyses a new
#     revision afresh (2026-11-08 16:12Z-17:00Z: a release that broke every gold step in 1 s left each
#     lane three attempts, and the release that fixed it would have waited 4 hours). An attempt whose
#     gold or bake step failed after it held the whole spark pool for COSTLY_SECONDS or more counts
#     under every release (root ADR-0521), as Kubernetes keeps a CrashLoopBackOff across restarts that change nothing
#     the pod runs: the host deploys every few hours, and a Gold that runs out of memory fails again
#     whatever else the release changed (2026-11-11: parcel-real-transaction's first full Gold died of
#     `Java heap space` or a lost driver after 47 to 147 minutes, 03:42Z, 08:15Z, 10:14Z, each new
#     release restarted it within the hour, and its claim kept 18 queued Spark starts out of the pool
#     for up to 3.7 hours).
#   - the generation: the lane's newest bake directory, resumed (its finished shards are kept), or the
#     next one when the last attempt of that generation failed in its bake or its equality gate: a
#     bake pinned to a Gold snapshot that has moved since cannot match the Gold it is compared with
#     (2026-11-07 07:38Z: generation 1, baked 11-05 at 39,396,906 rows, against a Gold of 42,053,586),
#     and a pin Gold no longer holds is not resumed; 1 for a lane never baked.
#
# Every start is recorded in <marks>/auto-promote/<lane>.jsonl (who started what, when) and, by the
# operator script, in the journal (`journalctl -t foundation-operator-start`).
set -euo pipefail
log() { printf 'by-pnu-auto-promote: %s\n' "$*"; }

# Fixed paths on the host; the FOUNDATION_AUTO_PROMOTE_* overrides exist for the tests.
release_root="${FOUNDATION_AUTO_PROMOTE_RELEASE_ROOT:-/opt/foundation-platform/current}"
marks="${FOUNDATION_AUTO_PROMOTE_MARKS:-/var/lib/foundation-run-now/auto-promote}"
bake_root="${FOUNDATION_OPERATOR_BAKE_ROOT:-/data/foundation-platform/by-pnu-bake}"
now="${FOUNDATION_AUTO_PROMOTE_NOW:-$(date +%s)}"
ops="${release_root}/scripts/ops"
release="$(basename "$(readlink -e "${release_root}")")"
mkdir -p "${marks}"

running="$(python3 -I -B "${release_root}/scripts/deploy/job_units.py" running 'foundation-by-pnu-lane-cutover-g*.service')" \
  || { log "systemd cannot tell whether a cut-over runs; nothing started"; exit 70; }
if [[ -n "${running}" ]]; then
  log "a lane cut-over runs ($(cut -d' ' -f1 <<<"${running}" | paste -sd' ')); nothing started"
  exit 0
fi

# Root ADR-0439: refreshing what users read now outranks a lane's first publish, at the Gold rebuild
# lock as in the spark pool. While the asset reconciler says an asset of a `publish` job is owed (a
# served lane's Gold or bake), no cut-over starts: its Gold step would hold the one rebuild lock for
# hours (2026-11-07 13:12Z the third parcel-regulation-zones cut-over of the day took it while the
# parcel lane served a 199-hour-old manifest and its owed panel rebuild could only end `waiting`). A
# reconciler that has not written for half an hour says nothing here (its own alert pages).
asset_metrics="${FOUNDATION_AUTO_PROMOTE_ASSET_METRICS:-/var/lib/foundation-jobs/metrics/asset-reconcile.prom}"
owed="$(python3 -I - "${release_root}/orchestration/jobs.v1.json" "${asset_metrics}" "${now}" <<'PY'
import json, os, re, sys
jobs_file, metrics, now = sys.argv[1], sys.argv[2], int(sys.argv[3])
try:
    if now - os.stat(metrics).st_mtime > 1800:
        sys.exit(0)
    lines = open(metrics, encoding="utf-8").read().splitlines()
except OSError:
    sys.exit(0)
listing = json.load(open(jobs_file, encoding="utf-8"))
publish = {job["id"] for job in listing["jobs"] if job.get("priority_class") == "publish"}
owed = re.compile(r'^foundation_asset_owed\{asset="([^"]+)",job="([^"]+)"\} 1$')
print(" ".join(sorted(found[1] for found in map(owed.match, lines) if found and found[2] in publish)))
PY
)" || { log "cannot read what the asset reconciler owes; nothing started"; exit 70; }
if [[ -n "${owed}" ]]; then
  log "a served lane's refresh is owed (${owed}); no first publish starts before it"
  exit 0
fi

mapfile -t pending < <("${ops}/by-pnu-lane-cutover.sh" lanes --pending)
if ((${#pending[@]} == 0)); then
  log "every lane is cut over; nothing to do"
  exit 0
fi

# The lane to start and its generation, or nothing while every pending lane waits out its backoff.
choice="$(python3 -I - "${marks}" "${bake_root}" "${now}" "${release_root}/orchestration/jobs.v1.json" "${release}" "${pending[@]}" <<'PY'
import datetime, glob, json, os, re, sys
marks, bake_root, now, jobs, release, *lanes = sys.argv[1:]
now = int(now)
HOUR, CAP = 3600, 86400
# A failed Spark step that held the pool this long was not a release's broken start (11-08: 1 s).
COSTLY_SECONDS = 600
with open(jobs, encoding="utf-8") as handle:
    GOLD_LIMIT = 60 * next(job["time_limit_minutes"] for job in json.load(handle)["jobs"] if job["id"] == "gold_panel_rebuild")


def load_runs():
    """Every cut-over run: (its start, %Y%m%dT%H%M%SZ of its id, its steps), newest first."""
    found = []
    for run in sorted(glob.glob(os.path.join(bake_root, "cutover", "*.jsonl")), reverse=True):
        try:
            began = int(datetime.datetime.strptime(os.path.basename(run)[:-6], "%Y%m%dT%H%M%SZ")
                        .replace(tzinfo=datetime.timezone.utc).timestamp())
            with open(run, encoding="utf-8") as handle:
                found.append((began, [json.loads(line) for line in handle if line.strip()]))
        except (OSError, ValueError):
            continue
    return found


RUNS = load_runs()

def attempts(lane, any_release=False):
    try:
        with open(os.path.join(marks, f"{lane}.jsonl"), encoding="utf-8") as handle:
            marks_ = [json.loads(line) for line in handle if line.strip()]
    except FileNotFoundError:
        return []
    return marks_ if any_release else [mark for mark in marks_ if mark.get("release") == release]

def evidence(lane, generation):
    """The steps of the newest cut-over run that worked on this lane and generation."""
    for _, steps in RUNS:
        mine = [step for step in steps if step.get("lane") == lane and step.get("generation") == generation]
        if mine:
            return mine
    return []

def own_run(lane, attempt, later):
    """The steps of the attempt's own run: the newest that worked on its lane and generation and began
    at or after its mark and before the lane's next attempt (`later`), or []."""
    for began, steps in RUNS:
        mine = [step for step in steps if step.get("lane") == lane and step.get("generation") == attempt.get("generation")]
        if mine:
            if attempt["at"] <= began < later:
                return mine
            if began < attempt["at"]:
                return []
    return []

def waited(lane, attempt):
    """The attempt's run ended `waiting`: a readiness step found something not loaded yet."""
    mine = own_run(lane, attempt, float("inf"))
    return bool(mine) and mine[-1].get("outcome") == "waiting"

def costly(steps):
    """The run's gold or bake step failed after it held the spark pool COSTLY_SECONDS or more."""
    return any(step.get("step") in ("gold", "bake") and step.get("outcome") == "failed"
               and step.get("seconds", 0) >= COSTLY_SECONDS for step in steps)

def counted(lane):
    """The attempts the backoff counts: this release's, and any release's whose run failed costly."""
    every = attempts(lane, any_release=True)
    nexts = [mark["at"] for mark in every[1:]] + [float("inf")]
    return [mark for mark, later in zip(every, nexts)
            if mark.get("release") == release or costly(own_run(lane, mark, later))]

def stopped_by_its_bound(lane, attempt):
    """The attempt's gold step ran to the rebuild's time bound: stopped, with its stages kept."""
    return any(step.get("step") == "gold" and step.get("outcome") == "failed" and step.get("seconds", 0) >= GOLD_LIMIT
               for step in evidence(lane, attempt.get("generation")))

# The order counts every attempt, the backoff this release's and the costly ones (`counted`): a deploy
# resets a wait only fast failures made, and the lane at the head of the contract must not go first
# again after each one (2026-11-08 23:12Z: parcel-regulation-zones, five attempts, restarted ahead of
# three lanes tried three times each). The wait runs from the lane's last attempt, whichever release.
for lane in sorted(lanes, key=lambda lane: len(attempts(lane, any_release=True))):  # stable: contract order among equals
    tried = counted(lane)
    if tried:
        last = attempts(lane, any_release=True)[-1]
        if waited(lane, last):
            wait = 0
        elif stopped_by_its_bound(lane, last):
            wait = HOUR
        else:
            wait = min(CAP, HOUR * 2 ** (len(tried) - 1))
        if now < last["at"] + wait:
            print(f"wait {lane} {last['at'] + wait}")
            continue
    generations = [int(m.group(1)) for path in glob.glob(os.path.join(bake_root, f"{lane}-pack-g*"))
                   if (m := re.fullmatch(rf"{re.escape(lane)}-pack-g([1-9][0-9]{{0,3}})", os.path.basename(path)))]
    generation = max(generations, default=1)
    steps = evidence(lane, generation)
    if any(step.get("step") in ("bake", "equality") and step.get("outcome") == "failed" for step in steps):
        generation += 1  # baked from a Gold that moved, or a pin Gold no longer keeps: bake anew
    print(f"start {lane} {generation}")
    break
PY
)"
while read -r verdict lane value; do
  [[ "${verdict}" == wait ]] && log "${lane}: its last attempt did not publish it; it is started again after $(date -u -d "@${value}" +%FT%TZ)"
done <<<"$(grep '^wait ' <<<"${choice}" || true)"
read -r verdict lane generation <<<"$(grep '^start ' <<<"${choice}" || true)"
if [[ "${verdict:-}" != start ]]; then
  log "every lane not cut over waits out its backoff; nothing started"
  exit 0
fi

printf '{"at": %s, "lane": "%s", "generation": %s, "release": "%s"}\n' "${now}" "${lane}" "${generation}" "${release}" >>"${marks}/${lane}.jsonl"
log "${lane}: not cut over and its last attempt (if any) is past its backoff; starting its cut-over at generation ${generation}"
exec "${ops}/by-pnu-pack-operator.sh" "${lane}" cutover "${generation}" --auto
