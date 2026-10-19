#!/usr/bin/env python3
"""Whether a main commit may be deployed, by its GitHub check runs (root ADR-0159, ADR-0167).

    release_checks.py <api-base> <owner/repo> <40-hex commit>

Reads the commit's check runs anonymously (the repository is public, ADR-0136) and prints one line:

    deploy <reason>   every check run completed, and each succeeded, was skipped or was neutral; or
                      every run the merge queue started on this commit did (ADR-0167): main's commit
                      is the tree the queue tested, so main's own second run is not waited for
    wait <reason>     none yet, or one still queued or running: ask again on the next tick
    refuse <reason>   one failed, was cancelled, timed out or needs action: never deploy this commit

A run a schedule or a manual dispatch started (the daily Actions cache budget, advisories, the
daily guard run) reports on the repository at that hour, not on this commit's code: it lands on
whatever commit main's head is, so it never decides a deploy (2026-10-19: a cache budget over its
line refused fc61aa36). A failed one is named in the reason, and still fails loudly where it ran.

Exit 0 for all three; exit 2 when GitHub cannot be read (the caller asks again next tick).
"""

import json
import sys
import urllib.request

PASSING = {"success", "skipped", "neutral"}
REPORT_EVENTS = {"schedule", "workflow_dispatch"}
PAGE = 100


def passed(run):
    return run.get("status") == "completed" and run.get("conclusion") in PASSING


def verdict(check_runs, workflow_runs=()):
    """('deploy' | 'wait' | 'refuse', reason) for a commit's check runs.

    `workflow_runs` are the commit's workflow runs. The check runs of a `schedule` or
    `workflow_dispatch` run are reports and do not count. Among the rest a failure anywhere
    refuses; the `merge_group` runs all passing deploys without waiting for the rest (ADR-0167).
    """
    reports = {run.get("check_suite_id") for run in workflow_runs if run.get("event") in REPORT_EVENTS}
    failed_reports = sorted(
        f"{run['name']}={run.get('conclusion')}"
        for run in check_runs
        if (run.get("check_suite") or {}).get("id") in reports and run.get("status") == "completed"
        and run.get("conclusion") not in PASSING
    )
    check_runs = [run for run in check_runs if (run.get("check_suite") or {}).get("id") not in reports]
    decision, reason = _verdict(check_runs, [run for run in workflow_runs if run.get("event") == "merge_group"])
    if failed_reports:
        reason += "; reports that do not decide a deploy failed: " + ", ".join(failed_reports)
    return decision, reason


def _verdict(check_runs, queue_workflow_runs):
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
    if not pending:
        return "deploy", f"{len(check_runs)} check runs passed"
    suites = {run.get("check_suite_id") for run in queue_workflow_runs}
    queue_checks = [run for run in check_runs if (run.get("check_suite") or {}).get("id") in suites]
    if queue_checks and all(map(passed, queue_workflow_runs)) and all(map(passed, queue_checks)):
        return "deploy", (
            f"the merge queue's {len(queue_checks)} check runs passed; not waiting for " + ", ".join(pending)
        )
    return "wait", "still running: " + ", ".join(pending)


def listed(url, key):
    """Every item of a paged GitHub list, under `key` of each page."""
    items, page = [], 1
    while True:
        request = urllib.request.Request(
            f"{url}&per_page={PAGE}&page={page}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "foundation-autodeploy"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
        items.extend(body[key])
        if len(items) >= body["total_count"] or not body[key]:
            return items
        page += 1


def check_runs(api, repository, commit):
    return listed(f"{api}/repos/{repository}/commits/{commit}/check-runs?filter=latest", "check_runs")


def workflow_runs(api, repository, commit):
    return listed(f"{api}/repos/{repository}/actions/runs?head_sha={commit}", "workflow_runs")


def main(argv):
    if len(argv) != 4 or len(argv[3]) != 40:
        print(__doc__, file=sys.stderr)
        return 64
    api, repository, commit = argv[1].rstrip("/"), argv[2], argv[3]
    try:
        runs = check_runs(api, repository, commit)
        queue = workflow_runs(api, repository, commit)
    except (OSError, ValueError, KeyError) as error:
        print(f"cannot read the check runs of {commit}: {error}", file=sys.stderr)
        return 2
    decision, reason = verdict(runs, queue)
    print(f"{decision} {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
