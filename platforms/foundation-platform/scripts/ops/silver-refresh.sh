#!/usr/bin/env bash
# One Silver lane refreshes itself from the newest complete release in the Bronze ledger
# (root ADR-0169; the shape is FLOOR's, root ADR-0128).
#
#   silver-refresh.sh <lane>          ExecStart of foundation-silver-refresh@<lane>.service
#   silver-refresh.sh <lane> --plan   read the ledger and the table, say what a run would do, stop
#   silver-refresh.sh cleanup         its ExecStopPost
#
# The lanes and what each runs are not here: the publisher's `run-silver-refresh` knows the lanes,
# and each lane's runner values are the `silver_refresh` block of its source contract
# (infra/lakehouse/contracts/hub-building-register-*-source-objects.json and
# vworld-land-*-source-objects.json). This script binds the run to the admitted release, the runtime
# database connection and this invocation's Compose project, and lets one lane run at a time: each
# takes the compose `spark` service's whole cap.
#
# The publisher's last line is `silver-refresh-outcome lane=… outcome=changed|unchanged reason=… …`;
# a run then ends with `foundation-job-outcome changed|unchanged`, the same word (root ADR-0171).
# `unchanged` means the Silver table's own snapshot summaries already record the release (or a
# newer one): nothing was staged, exported or written.
#
# Spark runs in the Compose project foundation-silver-refresh-<INVOCATION_ID>. A timeout kills this
# script and the compose client, not the daemon-owned container; `cleanup` removes that project's
# one-off containers and networks with the release's publisher, as FLOOR and the Gold rebuild do.
set -euo pipefail

LANE="${1:-}"
if [[ "${LANE}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo 'silver-refresh: cleanup takes no arguments' >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo 'silver-refresh: cleanup needs the systemd INVOCATION_ID' >&2; exit 64; }
  # Any installed release: `current` may have moved since this invocation started.
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-silver-refresh
fi
PLAN=0
[[ "$#" == 2 && "${2}" == --plan ]] && PLAN=1
[[ ("$#" == 1 || "${PLAN}" == 1) && "${LANE}" =~ ^[a-z][a-z0-9-]*$ ]] || {
  echo "silver-refresh: expected one lane name (optionally --plan) or cleanup, got '$*'" >&2
  exit 64
}
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# The lanes' work (staged ZIPs of gigabytes, the handoff, Spark's spill) is on the data disk, which
# the release creates (foundation-release.sh timers); the unit names it in ReadWritePaths.
export FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT="${FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT:-/data/foundation-platform/silver-refresh}"
# The hub exports compose PNUs through the legal-dong pairing's 시군구 projection, which
# lineage_stewardship writes here (root ADR-0143 §5, docs/runbooks/legal-dong-code-changes.md §2).
export FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION="${FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION:-/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json}"
# A run outside systemd (a supervised manual run) names its own project; ExecStopPost is not there
# to clean it, so the operator is told how.
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "silver-refresh: no systemd invocation; this run's Compose project is foundation-silver-refresh-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo 'silver-refresh: INVOCATION_ID is not a systemd invocation id' >&2; exit 64; }

# One lane at a time, whoever started it. Taken without waiting, like the Gold rebuild's lock. A
# plan only reads, so it does not wait for a running lane.
if [[ "${PLAN}" == 0 ]]; then
  lock="${FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT}/refresh.lock"
  [[ -e "${lock}" ]] || : >>"${lock}"
  exec 8<"${lock}"
  if ! flock -n 8; then
    echo "silver-refresh: refused: another lane holds ${lock}; this lane runs after it finishes" >&2
    exit 75
  fi
fi

# systemd supplies the existing runtime credentials. Compose owns the API role, database and
# published host port; no second DATABASE_URL is stored (the FLOOR cycle resolves it the same way).
if [[ -z "${DATABASE_URL:-}" ]]; then
  if ! DATABASE_URL="$(docker compose --project-directory "${RELEASE_ROOT}" \
    --env-file /dev/null -f "${RELEASE_ROOT}/docker-compose.yml" \
    config --format json --no-env-resolution 2>/dev/null \
    | python3 "${RELEASE_ROOT}/scripts/ops/runtime-database-url.py")"; then
    echo 'silver-refresh: cannot resolve the existing runtime database connection' >&2
    exit 78
  fi
  export DATABASE_URL
fi

export INVOCATION_ID
export FOUNDATION_PLATFORM_SILVER_REFRESH_LANE="${LANE}"
export FOUNDATION_PLATFORM_SILVER_REFRESH_PLAN="${PLAN}"
export FOUNDATION_PLATFORM_SILVER_REFRESH_RELEASE_ROOT="${RELEASE_ROOT}"
export FOUNDATION_PLATFORM_SILVER_REFRESH_SPARK_JARS="${SPARK_RELEASE_JARS}"
export FOUNDATION_PLATFORM_SILVER_REFRESH_JARS_DIR="${RELEASE_JARS_DIR}"
# Spark (uid 185) writes the lane's work directory through the service account's group.
FOUNDATION_PLATFORM_LAKEHOUSE_GID="$(id -g)"
export FOUNDATION_PLATFORM_LAKEHOUSE_GID
if [[ "${PLAN}" == 1 ]]; then
  exec "${PUBLISHER_BIN}" run-silver-refresh
fi
# Every line passes through as it comes. The publisher's outcome becomes the job's last line, the
# one every scheduled job ends with (root ADR-0171): `changed` starts the Gold rebuild. A run that
# fails prints none, and pipefail keeps its exit status.
"${PUBLISHER_BIN}" run-silver-refresh | awk '
  { print; fflush() }
  /^silver-refresh-outcome / {
    outcome = ""
    if ($0 ~ / outcome=changed( |$)/) outcome = "changed"
    if ($0 ~ / outcome=unchanged( |$)/) outcome = "unchanged"
  }
  END { if (outcome != "") print "foundation-job-outcome " outcome }'
