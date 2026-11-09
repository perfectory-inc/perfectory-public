#!/usr/bin/env python3
"""The operations map's service checks (root ADR-0209), written to one JSON file:

- edge: how Korean visitors reach the live catalog, buildings and tiles hostnames. Every live
  hostname of those three gateways in config/r2-connections.contract.json (the public hostname and
  its aliases; nothing is listed here) is asked for `/cdn-cgi/trace` SAMPLES times, each on a new
  connection as a first visit would be: p50/p95 of the full answer time and the Cloudflare colo it
  entered at (CloudFront's PoP for a hostname the Seoul front door answers, root ADR-0461, which
  passes the path through uncached). Cloudflare answers that path at the edge, so no Worker runs and nothing is billed; it
  measures the entry path (where a SEA entry costs seconds, 2026-10-12), not the Worker's or R2's
  time, which the by-PNU serving monitor holds. cloudflare.com's own colo is the reference: when
  it says ICN and ours say SEA, the plan's routing is why, not our code.
- ci: the weekly pull request CI wall-time trend of root ADR-0203, computed by that ADR's own
  scripts/ci/ci_time_budget.py from the control checkout the deploy keeps (it is outside the
  platform's release tree). GitHub's runs API is read anonymously, and the host's 60 requests an
  hour are shared with the autodeploy (scripts/deploy/foundation-autodeploy.sh), so each UTC day's
  runs are fetched once, after the day is over, while the hour's budget stays above the
  autodeploy's share, and kept beside the file; a week is reported once all its days are kept.
  The first fill of CI_WEEKS weeks takes some hours; after that it is a few requests a day.
- security: the repository's open security advisories (root ADR-0278), for foundation-api's
  /metrics and so Alertmanager and #alerts: the open Dependabot alerts (with the host's one
  read-only token), and the advisories the OSV and Cargo ratchets name in their `advisory`
  annotations (anonymous; a run already read is not read again): main's newest run of any event,
  and newer pull request or merge-queue runs whose change touches no lockfile (root ADR-0388). Each source records
  whether it could be read, with a fixed reason when not. This part is not on the console's panel.

Each part records its own error. Exits 1 only when no hostname could be measured: the unit's
OnFailure reports that to Slack. foundation-api serves the file at /data-catalog/v1/service-checks.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import pathlib
import re
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config" / "r2-connections.contract.json"
GATEWAYS = ("parcel_by_pnu_gateway", "building_by_pnu_gateway", "vector_tile_gateway")
REFERENCE_HOST = "cloudflare.com"
SAMPLES = 10
TIMEOUT_SECONDS = 15
CI_WEEKS = 8
# A day's pull request runs are fetched once that day is this far past: CI finishes well within it.
RUN_SETTLE = timedelta(hours=3)
# GitHub's anonymous budget is 60 requests an hour per host, and the autodeploy asks up to 24 of them
# (two every five minutes while a commit waits). A busy day is about 1,600 runs (2026-10-12), 100 to a request, split
# in halves past the API's 1,000-result cap.
API_RESERVE = 30
REQUESTS_PER_DAY = 20
OUT = os.environ.get("FOUNDATION_SERVICE_CHECKS_PATH", "/var/lib/foundation-platform/service-checks/service-checks.json")
CONTROL_ROOT = pathlib.Path(os.environ.get("FOUNDATION_SERVICE_CHECKS_CONTROL_ROOT", "/opt/perfectory-control/current"))
# Security advisories (root ADR-0278). The token reads Dependabot alerts and nothing else; without it
# that source says so and the two ratchets, read anonymously, still report.
SECURITY_TOKEN_ENV = "FOUNDATION_PLATFORM_SECURITY_ALERTS_GITHUB_TOKEN"
# The runs that refuse a new advisory, each naming it in an annotation.
RATCHETS = {"osv-ratchet": "monorepo-guard.yml", "cargo-advisories": "dependency-advisories.yml"}
ADVISORY_TITLE = "advisory"
ADVISORY_MESSAGE = re.compile(r"(\S+) (\S+)")
# Main's verdict is its newest completed run of any of these (root ADR-0388), not only the daily
# one: 2026-10-30 an advisory published at 23:09Z was refused by a pull request at 23:16Z and by
# main's push at 23:24Z, while the next scheduled run started at 00:48Z.
MAIN_EVENTS = ("schedule", "push", "workflow_dispatch")
# A pull request's or merge group's run reads main's lockfiles too: its refusal is main's when the
# change touches no lockfile. Only the gates that run on changes are asked.
CHANGE_EVENTS = ("pull_request", "merge_group")
GATES_CHANGES = {"monorepo-guard.yml"}
LOCKFILES = {"Cargo.lock", "pnpm-lock.yaml"}
QUEUE_BRANCH = re.compile(r"gh-readonly-queue/[^/]+/pr-(\d+)-")
# Main runs looked at for a known verdict before the last one known stands.
MAIN_RUNS_READ = 3
# The most one new run costs: its jobs, a failed job's annotations, its pull request's files.
RUN_REQUESTS = 3
# What a ratchet needs to start: its two run lists and one new run.
SECURITY_REQUESTS = 2 + RUN_REQUESTS


def hostnames(contract: pathlib.Path = CONTRACT) -> list[str]:
    data = json.loads(contract.read_text(encoding="utf-8"))
    names = []
    for gateway in GATEWAYS:
        for name in [data[gateway]["public_hostname"], *data[gateway].get("public_hostname_aliases", [])]:
            if name not in names:
                names.append(name)
    return names


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank, as ci_time_budget.percentile: every reported value was observed."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def colo_of(trace: str) -> str | None:
    for line in trace.splitlines():
        key, _, value = line.partition("=")
        if key == "colo" and value.strip():
            return value.strip()
    return None


def trace(host: str) -> tuple[float, str | None]:
    """One answer of https://<host>/cdn-cgi/trace: (milliseconds, entry). A hostname the Seoul front
    door answers (root ADR-0461) is entered at CloudFront's PoP (x-amz-cf-pop, ICN57-P5); the trace's
    colo would then be where CloudFront entered Cloudflare, not where the visitor entered."""
    started = time.perf_counter()
    request = urllib.request.Request(f"https://{host}/cdn-cgi/trace", headers={"User-Agent": "foundation-service-checks"})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        body = response.read().decode("utf-8", "replace")
        pop = response.headers.get("x-amz-cf-pop")
    return (time.perf_counter() - started) * 1000, pop or colo_of(body)


def measure_host(host: str, ask=trace, samples: int = SAMPLES) -> dict:
    times, colos, errors = [], Counter(), []
    for _ in range(samples):
        try:
            took, colo = ask(host)
        except Exception as error:  # a refused or timed-out sample is counted, not fatal
            errors.append(f"{type(error).__name__}: {error}"[:200])
            continue
        times.append(took)
        if colo:
            colos[colo] += 1
    return {
        "hostname": host,
        "colo": colos.most_common(1)[0][0] if colos else None,
        "p50_ms": round(percentile(times, 0.5)) if times else None,
        "p95_ms": round(percentile(times, 0.95)) if times else None,
        "samples": len(times),
        "failed_samples": len(errors),
        "error": errors[-1] if not times else None,
    }


def measure_edge(hosts: list[str], ask=trace, now: datetime | None = None) -> dict:
    reference = measure_host(REFERENCE_HOST, ask, samples=1)
    return {
        "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
        "reference_colo": reference["colo"],
        "hosts": [measure_host(host, ask) for host in hosts],
    }


def load_ci_budget_module(root: pathlib.Path = CONTROL_ROOT):
    path = root / "scripts" / "ci" / "ci_time_budget.py"
    spec = importlib.util.spec_from_file_location("ci_time_budget", path)
    if spec is None or spec.loader is None or not path.is_file():
        raise FileNotFoundError(f"{path} is not there")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def remaining_requests() -> int:
    """GitHub's anonymous core budget left this hour; asking it is not counted against it."""
    request = urllib.request.Request("https://api.github.com/rate_limit", headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return int(json.load(response)["resources"]["core"]["remaining"])


def ci_days(ci, now: datetime) -> tuple[list[datetime], list[datetime]]:
    """The trend's full weeks and the UTC days whose runs it needs: those weeks and the Monday
    after (an attempt begun late on Sunday finishes then), each only once it is RUN_SETTLE old so
    its runs have completed."""
    current = ci.week_start(now)
    weeks = [current - timedelta(weeks=n) for n in range(CI_WEEKS, 0, -1)]
    days = [weeks[0] + timedelta(days=n) for n in range(CI_WEEKS * 7 + 1)]
    return weeks, [day for day in days if day + timedelta(days=1) + RUN_SETTLE <= now]


def measure_ci(store: pathlib.Path, now: datetime, ci, repository: str, budget: float, remaining=remaining_requests) -> dict:
    """Fetch the days not stored yet, newest first, while the hour's budget stays above
    API_RESERVE (the autodeploy's share), then compute the weeks whose days are all stored."""
    store.mkdir(parents=True, exist_ok=True)
    weeks, days = ci_days(ci, now)
    wanted = {f"{day:%Y-%m-%d}.json" for day in days}
    for old in store.glob("*.json"):
        if old.name not in wanted and old.name < min(wanted, default=""):
            old.unlink()  # a cache of the runs API, recomputable, not a record
    error = None
    for day in sorted(days, reverse=True):
        path = store / f"{day:%Y-%m-%d}.json"
        if path.exists():
            continue
        try:
            if remaining() < API_RESERVE + REQUESTS_PER_DAY:
                break  # the next hour's run carries on
            runs = ci.fetch_runs(repository, day, day + timedelta(days=1), None)
        except Exception as failure:  # the stored days still give a trend; the next run retries
            error = f"{type(failure).__name__}: {failure}"[:300]
            break
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(runs), encoding="utf-8")
        os.replace(temporary, path)
    stored = {path.stem for path in store.glob("*.json")}
    runs = [run for path in sorted(store.glob("*.json")) for run in json.loads(path.read_text(encoding="utf-8"))]
    complete = [
        week for week in weeks if all(f"{week + timedelta(days=n):%Y-%m-%d}" in stored for n in range(8))
    ]
    trend = [{k: v for k, v in row.items() if k != "attempts"} for row in ci.weekly_trend(ci.group_attempts(runs), complete)]
    return {
        "attempted_at": now.isoformat(),
        "budget_minutes": budget,
        "weeks": trend,
        "pending_days": len([day for day in days if f"{day:%Y-%m-%d}" not in stored]),
        "error": error,
    }


def measure_ci_from_control(store: pathlib.Path, now: datetime, root: pathlib.Path = CONTROL_ROOT) -> dict:
    try:
        ci = load_ci_budget_module(root)
        repository = json.loads((root / "tools" / "github" / "repository-identity.json").read_text(encoding="utf-8"))["full_name"]
        budget = ci.load_budget(root / "tools" / "ci" / "ci-time-budget.contract.json")
    except Exception as error:
        return {"attempted_at": now.isoformat(), "budget_minutes": None, "weeks": [], "pending_days": None, "error": f"{type(error).__name__}: {error}"[:300]}
    return measure_ci(store, now, ci, repository, budget)


def github(path: str, token: str | None = None) -> tuple[object, dict]:
    """One GitHub API answer and its headers (lower-cased); with the token when there is one."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "foundation-service-checks",
               "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(f"https://api.github.com{path}", headers=headers)
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.load(response), {key.lower(): value for key, value in response.headers.items()}


def source_error(reason: str, error: Exception | str) -> dict:
    """A source that could not be read: a fixed reason for the alert, the detail for the log."""
    detail = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
    return {"readable": False, "reason": reason, "error": detail[:300], "findings": []}


def dependabot(repository: str, token: str | None, ask=github) -> tuple[dict, str | None]:
    """The open Dependabot alerts, one finding per advisory and package; and the token's expiry."""
    if not token:
        return source_error("no-token", f"{SECURITY_TOKEN_ENV} is not set on this host"), None
    alerts, headers = ask(f"/repos/{repository}/dependabot/alerts?state=open&per_page=100", token)
    # shortcut: one page of 100; past that the source says so instead of paging (cursor `after`).
    if len(alerts) >= 100:
        return source_error("too-many", "100 or more open Dependabot alerts; read them on GitHub"), None
    findings = {}
    for alert in sorted(alerts, key=lambda alert: alert["number"]):
        package = alert["dependency"]["package"]
        key = (alert["security_advisory"]["ghsa_id"], f"{package['ecosystem']}/{package['name']}")
        findings.setdefault(key, {"advisory": key[0], "package": key[1],
                                  "severity": alert["security_advisory"]["severity"], "link": alert["html_url"]})
    readable = {"readable": True, "reason": None, "error": None, "findings": list(findings.values())}
    return readable, headers.get("github-authentication-token-expiration")


def run_advisories(repository: str, run_id: int, link: str, ask=github) -> list[dict]:
    """The `<advisory> <package>` pairs a run's failed jobs name in `advisory` annotations."""
    jobs, _ = ask(f"/repos/{repository}/actions/runs/{run_id}/jobs?per_page=100", None)
    findings = {}
    for job in jobs["jobs"]:
        if job["conclusion"] in ("success", "skipped"):
            continue  # the ratchets annotate only when they refuse
        notes, _ = ask(f"/repos/{repository}/check-runs/{job['id']}/annotations?per_page=100", None)
        for note in notes:
            match = ADVISORY_MESSAGE.fullmatch((note.get("message") or "").strip())
            if note.get("title") == ADVISORY_TITLE and match:
                findings.setdefault(match.groups(), {"advisory": match[1], "package": match[2], "severity": "", "link": link})
    return list(findings.values())


def changes_a_lockfile(repository: str, run: dict, ask=github) -> bool:
    """Whether the change a pull request or merge-queue run tested touches a lockfile. Unknown is
    yes, so a refusal that may be the change's own never pages as main's."""
    queued = QUEUE_BRANCH.match(run.get("head_branch") or "")
    numbers = [int(queued[1])] if queued else [pull["number"] for pull in run.get("pull_requests") or []]
    if not numbers:
        return True  # a fork's pull request: the runs API does not name it
    files, _ = ask(f"/repos/{repository}/pulls/{numbers[0]}/files?per_page=100", None)
    # shortcut: one page; a change of 100+ files counts as touching a lockfile rather than paging.
    return len(files) >= 100 or any(pathlib.PurePosixPath(f["filename"]).name in LOCKFILES for f in files)


def ratchet(repository: str, workflow: str, token: str | None, previous: dict | None, ask=github,
            remaining=lambda: API_RESERVE + RUN_REQUESTS) -> dict:
    """The advisories main is exposed to by one ratchet (root ADR-0278, ADR-0388): those the newest
    completed run on main whose verdict is known names, and those a newer failed pull request or
    merge-queue run names when its change touches no lockfile. Read anonymously (the token reads
    Dependabot alerts only); a run already read is not read again, and a run past the hour's budget
    is read next hour."""
    del token
    # A link that is the same every day: a run's own changes and would re-announce.
    link = f"https://github.com/{repository}/actions/workflows/{workflow}?query=branch%3Amain"
    known = ({row["run_id"]: row["findings"] for row in previous.get("verdicts", [])}
             if previous and previous.get("readable") else {})
    verdicts = {}

    def verdict(run: dict, of_change: bool) -> list[dict] | None:
        """None while unknown: a run on main that failed without naming an advisory says nothing."""
        if run["id"] in known:
            verdicts[run["id"]] = known[run["id"]]
        elif run["conclusion"] == "success":
            verdicts[run["id"]] = []
        elif remaining() >= API_RESERVE + RUN_REQUESTS:
            found = run_advisories(repository, run["id"], link, ask)
            if found and of_change and changes_a_lockfile(repository, run, ask):
                found = []  # the change's own dependency: its pull request is already red
            verdicts[run["id"]] = found if found or of_change else None
        return verdicts.get(run["id"])

    listed, _ = ask(f"/repos/{repository}/actions/workflows/{workflow}/runs"
                    "?branch=main&status=completed&per_page=10", None)
    main_runs = [run for run in listed["workflow_runs"]
                 if run.get("event") in MAIN_EVENTS and run.get("conclusion") in ("success", "failure")]
    if not main_runs:
        return source_error("no-run", f"{workflow} has no completed run on main")
    base, findings = None, {}
    for run in main_runs[:MAIN_RUNS_READ]:
        found = verdict(run, of_change=False)
        if found is not None:
            base, findings = run, {(f["advisory"], f["package"]): f for f in found}
            break
    if base is None:
        if previous and previous.get("readable"):
            return previous  # main's newest runs failed before the ratchet spoke: what was known stands
        return source_error("no-verdict", f"{workflow}'s newest runs on main failed without naming an advisory")
    if workflow in GATES_CHANGES:
        failed, _ = ask(f"/repos/{repository}/actions/workflows/{workflow}/runs?status=failure&per_page=20", None)
        for run in failed["workflow_runs"]:
            # Only evidence newer than main's verdict: a later green run on main supersedes it.
            if run.get("event") in CHANGE_EVENTS and run["created_at"] > base["created_at"]:
                for finding in verdict(run, of_change=True) or []:
                    findings.setdefault((finding["advisory"], finding["package"]), finding)
    return {"readable": True, "reason": None, "error": None, "findings": list(findings.values()),
            "run_id": base["id"], "verdicts": [{"run_id": run_id, "findings": found} for run_id, found in verdicts.items()]}


def measure_security(repository: str, token: str | None, previous: dict | None, now: datetime,
                     ask=github, remaining=remaining_requests) -> dict:
    """Every source on its own: one that cannot be read says so and the others still report."""
    previous_sources = (previous or {}).get("sources", {})

    def unread(name: str, reason: str, error: Exception | str) -> dict:
        # What was last known stays: an advisory resolves only when a read finds it gone, so a
        # source that cannot be read is not announced as fixed and then as new again.
        return {**source_error(reason, error), "findings": previous_sources.get(name, {}).get("findings", [])}

    sources, expires = {}, None
    try:
        sources["dependabot"], expires = dependabot(repository, token, ask)
    except Exception as error:
        sources["dependabot"] = unread("dependabot", "github-error", error)
    for name, workflow in RATCHETS.items():
        try:
            # Always anonymous, so the token never needs more than Dependabot alerts. The budget is the
            # host's (shared with the autodeploy): when it is short, last hour's answer stands.
            if remaining() < API_RESERVE + SECURITY_REQUESTS:
                sources[name] = previous_sources.get(name) or unread(name, "github-error", "no anonymous budget left")
                continue
            sources[name] = ratchet(repository, workflow, None, previous_sources.get(name), ask, remaining)
        except Exception as error:
            sources[name] = unread(name, "github-error", error)
    return {"checked_at": now.isoformat(), "token_expires_at": expires, "sources": sources}


def measure_security_from_control(previous: dict | None, now: datetime, root: pathlib.Path = CONTROL_ROOT) -> dict:
    token = os.environ.get(SECURITY_TOKEN_ENV, "").strip() or None
    try:
        repository = json.loads((root / "tools" / "github" / "repository-identity.json").read_text(encoding="utf-8"))["full_name"]
    except Exception as error:
        unread = source_error("github-error", error)
        return {"checked_at": now.isoformat(), "token_expires_at": None,
                "sources": {name: unread for name in ("dependabot", *RATCHETS)}}
    return measure_security(repository, token, previous, now)


def write_atomically(path: pathlib.Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o644)  # foundation-api reads it from its own container user
    os.replace(temporary, path)


def main() -> int:
    out = pathlib.Path(OUT)
    now = datetime.now(timezone.utc)
    edge = measure_edge(hostnames(), now=now)
    ci = measure_ci_from_control(out.parent / "ci-runs", now)
    try:
        previous = json.loads(out.read_text(encoding="utf-8")).get("security")
    except (OSError, ValueError, AttributeError):
        previous = None
    security = measure_security_from_control(previous, now)
    write_atomically(out, {"schema_version": 1, "checked_at": now.isoformat(), "edge": edge, "ci": ci, "security": security})
    for name, source in security["sources"].items():
        print(f"security {name}: findings={len(source['findings'])} reason={source['reason']}")
    for host in edge["hosts"]:
        print(f"{host['hostname']}: colo={host['colo']} p50={host['p50_ms']}ms p95={host['p95_ms']}ms samples={host['samples']}")
    print(f"reference {REFERENCE_HOST}: colo={edge['reference_colo']}; ci error={ci.get('error')}")
    if not any(host["samples"] for host in edge["hosts"]):
        print("service-checks: no hostname answered", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
