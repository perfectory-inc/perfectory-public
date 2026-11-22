#!/usr/bin/env bash
# Removes the DuckDB stage directories that dead runs of the Gold version lanes left on the SSD
# every stage shares with the runtime Postgres (root ADR-0599). The publisher removes its stage when
# it ends; a run its unit's time limit or a kill stopped leaves it (2026-11-19: a parcel-nearby run
# stopped at its hour left a 24 GB stage.duckdb, and three days later that disk's reserve refused
# the served parcel boundaries' first build after 9 of its 16 시도).
#
#   duckdb-stage-orphans.sh <versions root> <lane> <run dir>
#
# gold-versions-build.sh runs it before its publisher opens a stage, for <lane>, whose run is <run
# dir>. A stage is removed only from a run that is no one's:
#   - the calling lane's earlier runs (systemd runs one run of a lane at a time); <run dir> is never
#     touched;
#   - another lane's runs but its newest, and only while that lane's unit does not run. The host's
#     one answer to that is scripts/deploy/job_units.py `running` (root ADR-0184): it names the unit
#     while it runs (a oneshot job is `activating` for its whole run) and exits 2 when systemd could
#     not be asked, which keeps the lane's stages. A run that starts after the runs were listed is
#     not among them, and a running lane's run is its newest.
# One line per removed stage on stdout: `removed <path> bytes=<n> lane=<lane> state=<own|stopped>`,
# then `stage orphans removed=<count> bytes=<total>`.
set -euo pipefail
usage='usage: duckdb-stage-orphans.sh <versions root> <lane> <run dir>'
root="${1:?${usage}}"
own="${2:?${usage}}"
current="${3:?${usage}}"
current="${current%/}"
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
    [[ "${run}" -ef "${current}" ]] && continue
    for stage in "${run}"/.duckdb-stage-*/; do
      [[ -d "${stage}" ]] || continue
      stage="${stage%/}"
      bytes="$(du -sb -- "${stage}" | cut -f1)"
      rm -rf -- "${stage}"
      printf 'removed %s bytes=%s lane=%s state=%s\n' "${stage}" "${bytes}" "${lane}" "${state}"
      count=$((count + 1))
      total=$((total + bytes))
    done
  done
done
printf 'stage orphans removed=%s bytes=%s\n' "${count}" "${total}"
