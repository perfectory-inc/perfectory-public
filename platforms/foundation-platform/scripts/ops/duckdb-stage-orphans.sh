#!/usr/bin/env bash
# Removes the DuckDB stage directories that dead runs of the Gold version lanes left on the SSD
# every stage shares with the runtime Postgres (root ADR-0599). The publisher removes its stage when
# it ends; a run its unit's time limit or a kill stopped leaves it (2026-11-19: a parcel-nearby run
# stopped at its hour left a 24 GB stage.duckdb, and three days later that disk's reserve refused
# the served parcel boundaries' first build after 9 of its 16 시도). Every stage under a dead run is
# removed, at any depth: 2026-11-25 the takeover verdicts' stages under runs/<id>/verdict/ (33 GB from
# 11-23..24, killed at their bound) had never been swept, and a parcel-ownership run killed at its
# hour left 14.6 GB; / fell from 56 to 36 GB free beside the production Postgres.
#
#   duckdb-stage-orphans.sh <versions root> <lane> <run dir>
#   duckdb-stage-orphans.sh <versions root> <lane> --stopped
#
# gold-versions-build.sh runs the first before its publisher opens a stage, for <lane>, whose run is
# <run dir>; the lane's unit runs the second as its ExecStopPost, which systemd runs however the run
# ended (a time limit or an OOM kill skips the publisher's own removal). A stage is removed only from
# a run that is no one's:
#   - the calling lane's earlier runs (systemd runs one run of a lane at a time); <run dir> is never
#     touched. With --stopped the lane's unit has stopped, so its every run is dead, the newest too;
#   - another lane's runs but its newest, and only while that lane's unit does not run. The host's
#     one answer to that is scripts/deploy/job_units.py `running` (root ADR-0184): it names the unit
#     while it runs (a oneshot job is `activating` for its whole run) and exits 2 when systemd could
#     not be asked, which keeps the lane's stages. A run that starts after the runs were listed is
#     not among them, and a running lane's run is its newest.
# One line per removed stage on stdout: `removed <path> bytes=<n> lane=<lane> state=<own|stopped>`,
# then `stage orphans removed=<count> bytes=<total>`.
set -euo pipefail
usage='usage: duckdb-stage-orphans.sh <versions root> <lane> <run dir>|--stopped'
root="${1:?${usage}}"
own="${2:?${usage}}"
current="${3:?${usage}}"
if [[ "${current}" == --stopped ]]; then
  current=""
else
  current="${current%/}"
fi
job_units="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy" && pwd -P)/job_units.py"
count=0
total=0
for lane_dir in "${root}"/*/; do
  lane_dir="${lane_dir%/}"
  lane="${lane_dir##*/}"
  runs=()
  for run in "${lane_dir}"/runs/*/; do
    [[ -d "${run}" ]] && runs+=("${run%/}")
  done
  ((${#runs[@]} > 0)) || continue
  if [[ "${lane}" == "${own}" ]]; then
    state=own
  else
    answer="$(python3 -I -B "${job_units}" running "foundation-gold-versions@${lane}.service" 2>/dev/null)" ||
      continue
    [[ -z "${answer}" ]] || continue
    state=stopped
    # Run directories are named by their UTC start, so the glob's order is their order.
    runs=("${runs[@]:0:${#runs[@]}-1}")
  fi
  for run in "${runs[@]}"; do
    [[ -n "${current}" && "${run}" -ef "${current}" ]] && continue
    # Every stage of the run, the verdict's under verdict/ included (the publisher names them, so no
    # path holds a newline).
    stages="$(find "${run}" -mindepth 1 -type d -name '.duckdb-stage-*' -prune)"
    while IFS= read -r stage; do
      [[ -n "${stage}" ]] || continue
      bytes="$(du -sb -- "${stage}" | cut -f1)"
      rm -rf -- "${stage}"
      printf 'removed %s bytes=%s lane=%s state=%s\n' "${stage}" "${bytes}" "${lane}" "${state}"
      count=$((count + 1))
      total=$((total + bytes))
    done <<<"${stages}"
  done
done
printf 'stage orphans removed=%s bytes=%s\n' "${count}" "${total}"
