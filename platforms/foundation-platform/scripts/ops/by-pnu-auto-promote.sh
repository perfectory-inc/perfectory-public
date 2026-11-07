#!/usr/bin/env bash
# A by-PNU lane whose Gold is ready publishes itself (root ADR-0435): the controller of its first
# publish, the way Argo Rollouts' controller promotes a rollout whose analysis passed, with no person
# in the normal path.
#
#   by-pnu-auto-promote.sh            started by foundation-by-pnu-auto-promote.timer, as root
#
# Each run, level-triggered like the asset reconciler (root ADR-0333 §7):
#   - nothing, while a lane cut-over runs (one at a time; it holds the spark pool's claim);
#   - else the first lane not cut over yet (by-pnu-lane-cutover.sh lanes --pending, contract order)
#     whose backoff has passed is started: by-pnu-pack-operator.sh <lane> cutover <generation> --auto.
#     The runner's steps are the automated analysis: history, preview, gold, parity, bake, equality,
#     latency, publish, monitor-sample, verify (the lane at its production hostname, GET twice). The
#     first that fails stops the run before anything after it, and its unit pages.
#   - a lane still pending after an attempt is started again after a backoff that doubles from one hour
#     (2, 4, ... at most 24): a transient answer (2026-11-05 02:42Z: R2 `service error` in the latency
#     gate) is retried without anyone, a broken lane is not restarted every quarter hour.
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
mkdir -p "${marks}"

running="$(python3 -I -B "${release_root}/scripts/deploy/job_units.py" running 'foundation-by-pnu-lane-cutover-g*.service')" \
  || { log "systemd cannot tell whether a cut-over runs; nothing started"; exit 70; }
if [[ -n "${running}" ]]; then
  log "a lane cut-over runs ($(cut -d' ' -f1 <<<"${running}" | paste -sd' ')); nothing started"
  exit 0
fi

mapfile -t pending < <("${ops}/by-pnu-lane-cutover.sh" lanes --pending)
if ((${#pending[@]} == 0)); then
  log "every lane is cut over; nothing to do"
  exit 0
fi

# The lane to start and its generation, or nothing while every pending lane waits out its backoff.
choice="$(python3 -I - "${marks}" "${bake_root}" "${now}" "${pending[@]}" <<'PY'
import glob, json, os, re, sys
marks, bake_root, now, *lanes = sys.argv[1:]
now = int(now)
HOUR, CAP = 3600, 86400

def attempts(lane):
    try:
        with open(os.path.join(marks, f"{lane}.jsonl"), encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
    except FileNotFoundError:
        return []

def evidence(lane, generation):
    """The steps of the newest cut-over run that worked on this lane and generation."""
    runs = sorted(glob.glob(os.path.join(bake_root, "cutover", "*.jsonl")), reverse=True)
    for run in runs:
        try:
            with open(run, encoding="utf-8") as handle:
                steps = [json.loads(line) for line in handle if line.strip()]
        except (OSError, ValueError):
            continue
        mine = [step for step in steps if step.get("lane") == lane and step.get("generation") == generation]
        if mine:
            return mine
    return []

for lane in lanes:
    tried = attempts(lane)
    if tried:
        wait = min(CAP, HOUR * 2 ** (len(tried) - 1))
        if now < tried[-1]["at"] + wait:
            print(f"wait {lane} {tried[-1]['at'] + wait}")
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

printf '{"at": %s, "lane": "%s", "generation": %s}\n' "${now}" "${lane}" "${generation}" >>"${marks}/${lane}.jsonl"
log "${lane}: not cut over and its last attempt (if any) is past its backoff; starting its cut-over at generation ${generation}"
exec "${ops}/by-pnu-pack-operator.sh" "${lane}" cutover "${generation}" --auto
