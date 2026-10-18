#!/usr/bin/env bash
# The ExecStopPost of a job unit (root ADR-0270 §3): runs `<script> cleanup` from the release the
# run started from, which admitted-writer-runtime.sh recorded in the unit's RuntimeDirectory. A run
# that outlived a deploy is then cleaned up by its own release, not by the one `current` names by
# the time it stops. A run that stopped before it recorded one is cleaned up by this file's release.
#
#   pinned-cleanup.sh <script under scripts/ops>
set -euo pipefail

script="${1:-}"
[[ "$#" == 1 && "${script}" =~ ^[a-z0-9-]+\.sh$ ]] ||
  { echo "pinned-cleanup: expected one scripts/ops/<name>.sh" >&2; exit 64; }
own="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
[[ "${own}" =~ ^(/.+)/releases/[0-9a-f]{40}$ ]] ||
  { echo "pinned-cleanup: not inside an installed release: ${own}" >&2; exit 65; }
base="${BASH_REMATCH[1]}"
release="${own}"
record="${RUNTIME_DIRECTORY%%:*}/release"
if [[ -n "${RUNTIME_DIRECTORY:-}" && -f "${record}" ]]; then
  release="$(<"${record}")"
  [[ "${release}" == "${base}"/releases/* && "${release##*/}" =~ ^[0-9a-f]{40}$ && -d "${release}" ]] ||
    { echo "pinned-cleanup: the recorded release is not an installed one: ${release}" >&2; exit 65; }
fi
exec "${release}/scripts/ops/${script}" cleanup
