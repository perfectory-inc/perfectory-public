#!/usr/bin/env bash
# Sourced by a job script: job_state_root <job id> prints the first of the job's state_paths in the
# release's orchestration/jobs.v1.json, the one place its unit's ReadWritePaths are rendered from
# (root ADR-0266). A state root never comes from an EnvironmentFile: on 2026-10-16 one pointed
# parcel_registry outside its sandbox and the job failed with a read-only file system.
job_state_root() {
  python3 -I -B "$(dirname "${BASH_SOURCE[0]}")/../../orchestration/dags/job_specs.py" --state-root "$1"
}
