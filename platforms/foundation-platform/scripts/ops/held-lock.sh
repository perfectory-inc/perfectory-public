#!/usr/bin/env bash
# Sourced by every job script that takes its run lock without waiting: lock_held_by_another_run
# <what> is how such a run ends when another run holds the lock. That is the other run's turn, not
# a failure (2026-10-19: an operator's attended parcel bake held the lane; the scheduled bake beside
# it failed its unit and sent a failure notice).
#
# In a registered job's own service (FOUNDATION_JOB_SERVICE=1, set by the release admission drop-in
# every registered job's unit carries) the run says `foundation-job-outcome waiting <what> is held
# by another run` and exits 0: the unit succeeds, no failure notice, and the gate reports waiting.
# Anywhere else (a terminal, an operator's transient unit, a cut-over chain) it exits 75, so what
# started it stops there. Only this exit waits: every other non-zero exit stays a failure.
#
# A caller that runs several such scripts and decides the outcome itself names a file in
# FOUNDATION_HELD_LOCKS: each held <what> is added to it, so an exit 75 can be told from any other.
lock_held_by_another_run() {
  [[ -z "${FOUNDATION_HELD_LOCKS:-}" ]] || printf '%s\n' "$1" >>"${FOUNDATION_HELD_LOCKS}"
  if [[ "${FOUNDATION_JOB_SERVICE:-}" == 1 ]]; then
    printf 'foundation-job-outcome waiting %s is held by another run\n' "$1"
    exit 0
  fi
  exit 75
}
