#!/usr/bin/env python3
"""Whether a main commit may be deployed, by its GitHub check runs (root ADR-0159).

    release_checks.py <api-base> <owner/repo> <40-hex commit>

Reads the commit's check runs anonymously (the repository is public, ADR-0136) and prints one line:

    deploy <reason>   every check run completed, and each succeeded, was skipped or was neutral
    wait <reason>     none yet, or one still queued or running: ask again on the next tick
    refuse <reason>   one failed, was cancelled, timed out or needs action: never deploy this commit

Exit 0 for all three; exit 2 when GitHub cannot be read (the caller asks again next tick).
"""

import json
import sys
import urllib.request

PASSING = {"success", "skipped", "neutral"}
PAGE = 100


def verdict(check_runs):
    """('deploy' | 'wait' | 'refuse', reason) for a commit's check runs."""
    if not check_runs:
        return "wait", "no check run has reported yet"
    pending = sorted(run["name"] for run in check_runs if run.get("status") != "completed")
    failed = sorted(
        f"{run['name']}={run.get('conclusion')}"
        for run in check_runs
        if run.get("status") == "completed" and run.get("conclusion") not in PASSING
    )
    if failed:
        return "refuse", "failed: " + ", ".join(failed)
    if pending:
        return "wait", "still running: " + ", ".join(pending)
    return "deploy", f"{len(check_runs)} check runs passed"


def check_runs(api, repository, commit):
    runs, page = [], 1
    while True:
        url = f"{api}/repos/{repository}/commits/{commit}/check-runs?per_page={PAGE}&page={page}"
        request = urllib.request.Request(
            url, headers={"Accept": "application/vnd.github+json", "User-Agent": "foundation-autodeploy"}
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
        runs.extend(body["check_runs"])
        if len(runs) >= body["total_count"] or not body["check_runs"]:
            return runs
        page += 1


def main(argv):
    if len(argv) != 4 or len(argv[3]) != 40:
        print(__doc__, file=sys.stderr)
        return 64
    api, repository, commit = argv[1].rstrip("/"), argv[2], argv[3]
    try:
        runs = check_runs(api, repository, commit)
    except (OSError, ValueError, KeyError) as error:
        print(f"cannot read the check runs of {commit}: {error}", file=sys.stderr)
        return 2
    decision, reason = verdict(runs)
    print(f"{decision} {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
