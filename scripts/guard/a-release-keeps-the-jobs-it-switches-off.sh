#!/usr/bin/env bash
# Prevents: a pull request deleting a job the running release has enabled. The release activation
# (platforms/foundation-platform/scripts/deploy/foundation-release.sh) refuses a candidate whose job
# list drops a job the release before it has enabled, so the job must first ship as an explicit
# `enabled: false` entry and be deleted by a later release. 2026-11-23: #1662 took over the 건물
# 에너지 lane and deleted the enabled gold_parcel_energy_rebuild; every check passed, the activation
# refused 02001268 three times from 08:40Z, and the host stayed on eedb618d until #1666 put the
# entry back switched off. This guard says so in the pull request: against its base
# (CHANGE_SCOPE_BASE) no job that is enabled there may be absent from the declared definitions.
# Without a base there is nothing to compare and it passes.
set -euo pipefail
cd "${1:-$(dirname "$0")/../..}"
definitions="platforms/foundation-platform/definitions"
base="${CHANGE_SCOPE_BASE:-}"
head="${CHANGE_SCOPE_HEAD:-HEAD}"
if [ -z "$base" ] || [ -z "${base//0/}" ]; then
  echo "OK a-release-keeps-the-jobs-it-switches-off: no pull request base to compare with"
  exit 0
fi
if ! fork="$(git merge-base "$base" "$head")"; then
  echo "FAIL a-release-keeps-the-jobs-it-switches-off: cannot compare $head with base $base" >&2
  exit 1
fi
python3 -I -B - "$definitions" "$fork" "$head" <<'PY'
import json, subprocess, sys

definitions, base, head = sys.argv[1:]


def jobs(rev):
    """Job id -> enabled, of every declared definition at `rev` (none when it has no definitions)."""
    listed = subprocess.run(["git", "ls-tree", "--name-only", f"{rev}:{definitions}"], capture_output=True, text=True)
    found = {}
    for name in listed.stdout.split("\n") if listed.returncode == 0 else []:
        if not name.endswith(".definition.json"):
            continue
        text = subprocess.run(["git", "show", f"{rev}:{definitions}/{name}"], capture_output=True, check=True).stdout
        for job in json.loads(text.decode("utf-8")).get("jobs", []):
            found[job["id"]] = job.get("enabled")
    return found


before, after = jobs(base), jobs(head)
dropped = sorted(name for name, enabled in before.items() if enabled is True and name not in after)
if dropped:
    sys.exit("FAIL a-release-keeps-the-jobs-it-switches-off: " + ", ".join(dropped) + " is enabled on the base and "
             "gone here. The release activation refuses a release that drops a job the running one has enabled "
             "(foundation-release.sh): keep the entry with `enabled: false` and a disabled_reason, and delete it in "
             "a later pull request, once a release that lists it off is live.")
print("OK a-release-keeps-the-jobs-it-switches-off")
PY
