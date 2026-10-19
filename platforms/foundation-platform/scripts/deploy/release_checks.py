#!/usr/bin/env python3
"""Whether a main commit may be deployed, by its GitHub check runs (root ADR-0159, ADR-0167).

    release_checks.py <api-base> <owner/repo> <40-hex commit> [<registry-base>]
    release_checks.py newest-released <owner/repo> <commit>... [--registry <registry-base>]

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

When release admission installs from GitHub's container registry (tools/release-build.contract.json
artifacts_from: registry, root ADR-0276 R3), the commit's release must be there first: the check run
the contract names (the release build's signing job) has to pass too, and the merge queue's early
answer waits for it; its failure refuses. Then `release-<commit>` must be in the package (read
anonymously from GitHub's container registry, or <registry-base> in a test): the tag admission pulls
first, so a release not there yet is a wait, never a deploy that fails on it (2026-10-19: e6fb99e6's
deploy refused with 404 at step 2 while its release was still building).

Exit 0 for all three; exit 2 when GitHub cannot be read (the caller asks again next tick).

`newest-released` prints the first of the commits given (newest first) whose release is published,
or nothing; without the registry (artifacts_from: server-build) every commit is, so it prints the
first. foundation-autodeploy.sh deploys that one, never waiting for a newer one still building.
"""

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

# The control checkout's own contract: this script runs from it (foundation-autodeploy.sh).
BUILD_CONTRACT = Path(__file__).resolve().parents[4] / "tools" / "release-build.contract.json"

PASSING = {"success", "skipped", "neutral"}
REGISTRY = "https://ghcr.io"
MANIFEST_TYPES = "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
REPORT_EVENTS = {"schedule", "workflow_dispatch"}
PAGE = 100


def passed(run):
    return run.get("status") == "completed" and run.get("conclusion") in PASSING


def required_release_check(contract=BUILD_CONTRACT):
    """The check run a deploy must wait for when releases come from the registry, else None."""
    data = json.loads(contract.read_text(encoding="utf-8"))
    return data["registry"]["check_run"] if data.get("artifacts_from") == "registry" else None


def release_package(contract=BUILD_CONTRACT):
    return json.loads(contract.read_text(encoding="utf-8"))["registry"]["package"]


def release_published(registry, repository, commit):
    """Whether `release-<commit>` is in the public package `repository`, asked anonymously."""
    scope = f"{registry}/token?scope=repository:{repository}:pull"
    with urllib.request.urlopen(urllib.request.Request(scope), timeout=30) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(f"{registry}/v2/{repository}/manifests/release-{commit}", method="HEAD",
                                     headers={"Authorization": f"Bearer {token}", "Accept": MANIFEST_TYPES})
    try:
        with urllib.request.urlopen(request, timeout=30):
            return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise


def verdict(check_runs, workflow_runs=(), release_check=None):
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
    if release_check and decision == "deploy":
        release = [run for run in check_runs if run["name"] == release_check]
        if not release or not all(map(passed, release)):
            decision, reason = "wait", f"the release `{release_check}` has not published this commit yet"
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


def newest_released(argv):
    registry = REGISTRY
    if "--registry" in argv:
        index = argv.index("--registry")
        registry, argv = argv[index + 1].rstrip("/"), argv[:index] + argv[index + 2:]
    repository, commits = argv[2], argv[3:]
    if not commits or not all(len(commit) == 40 for commit in commits):
        print(__doc__, file=sys.stderr)
        return 64
    if required_release_check() is None:
        print(commits[0])
        return 0
    package = f"{repository.split('/')[0].lower()}/{release_package()}"
    try:
        found = next((commit for commit in commits if release_published(registry, package, commit)), None)
    except (OSError, ValueError, KeyError) as error:
        print(f"cannot read which releases are in {package}: {error}", file=sys.stderr)
        return 2
    if found:
        print(found)
    return 0


def main(argv):
    if argv[1:2] == ["newest-released"]:
        return newest_released(argv)
    if len(argv) not in (4, 5) or len(argv[3]) != 40:
        print(__doc__, file=sys.stderr)
        return 64
    api, repository, commit = argv[1].rstrip("/"), argv[2], argv[3]
    registry = argv[4].rstrip("/") if len(argv) == 5 else REGISTRY
    release_check = required_release_check()
    try:
        runs = check_runs(api, repository, commit)
        queue = workflow_runs(api, repository, commit)
    except (OSError, ValueError, KeyError) as error:
        print(f"cannot read the check runs of {commit}: {error}", file=sys.stderr)
        return 2
    decision, reason = verdict(runs, queue, release_check)
    if release_check and decision == "deploy":
        package = f"{repository.split('/')[0].lower()}/{release_package()}"
        try:
            published = release_published(registry, package, commit)
        except (OSError, ValueError, KeyError) as error:
            print(f"cannot read whether release-{commit} is in {package}: {error}", file=sys.stderr)
            return 2
        if not published:
            decision, reason = "wait", f"release-{commit} is not in {package} yet"
    print(f"{decision} {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
