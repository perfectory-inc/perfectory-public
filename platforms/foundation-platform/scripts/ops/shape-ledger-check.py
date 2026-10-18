#!/usr/bin/env python3
"""The committed production-shape ledgers still describe production (root ADR-0259 §5).

CI gates every pull request on two ledgers the release ships: the lakehouse's
(infra/lakehouse/contracts/lakehouse-production-shape.json) and Postgres's
(config/postgres-production-shape.json). Production moves away from them when a deploy migrates
a table, a load creates a table or its first rows, or a rewrite retires a file shape. This job
captures both again, read-only, right after each deploy (started_once_after_deploy) and daily
after the loads, and names each table that differs from the release's. Only the shape is
compared, not when it was captured. A stale ledger is a pull request to take, not a broken
production: every run puts both captures in the private ledger bucket (shape-ledger/ in
foundation-platform-shape-ledger-prod, with the shape-ledger group's write-only-there key), where
the shape ledger refresh workflow reads and compares them and prepares the pull request; the host
holds no GitHub token (root ADR-0279). A stale run says it is waiting (root ADR-0260); stale for
more than 48 hours since first seen (first-stale.json), it fails and alerts. The fresh ledgers
also stay in /var/lib/foundation-shape-ledger/ (the unit's StateDirectory).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

RELEASE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_RELEASE", "/opt/foundation-platform/current"))
STATE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_STATE", "/var/lib/foundation-shape-ledger"))
LAKEHOUSE = "infra/lakehouse/contracts/lakehouse-production-shape.json"
POSTGRES = "config/postgres-production-shape.json"


def differences(committed: dict, live: dict, key: str) -> list[str]:
    """Each table whose shape differs between the two ledgers, by name. `key` is what holds the
    shapes: a mapping of table to shape, or a list of table names."""
    old, new = committed[key], live[key]
    if isinstance(old, list):
        return ([f"{table}: now holds rows" for table in sorted(set(new) - set(old))]
                + [f"{table}: no longer holds rows" for table in sorted(set(old) - set(new))])
    return ([f"{table}: new in production" for table in sorted(set(new) - set(old))]
            + [f"{table}: gone from production" for table in sorted(set(old) - set(new))]
            + [f"{table}: shape changed" for table in sorted(set(old) & set(new)) if old[table] != new[table]])


def release_module(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, RELEASE / "scripts/ops" / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_capture():
    return release_module("lakehouse_production_shape", "lakehouse-production-shape.py")


# The ledger bucket (root ADR-0279): its name, endpoint host and write-only-there key.
PUBLISH_PREFIX = "shape-ledger/"
PUBLISH_ENV = ("FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET", "FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_ENDPOINT_HOST",
               "FOUNDATION_PLATFORM_SHAPE_LEDGER_WRITER_ACCESS_KEY_ID",
               "FOUNDATION_PLATFORM_SHAPE_LEDGER_WRITER_SECRET_ACCESS_KEY")


def publish(files: dict[str, str]) -> str:
    """Puts each capture in the ledger bucket under its ledger's file name; says what happened. A
    failed or impossible upload never fails the run: the 48-hour escalation still stands."""
    bucket, host, key, secret = (os.environ.get(name, "") for name in PUBLISH_ENV)
    if not all((bucket, host, key, secret)):
        return f"not published (no ledger bucket key on this host); copy {STATE}/*.json into a pull request"
    r2 = release_module("r2_object", "r2_object.py")
    try:
        for name, text in files.items():
            r2.put(f"https://{host}", bucket, PUBLISH_PREFIX + name, key, secret, text.encode("utf-8"))
    except OSError as error:
        return f"not published ({error}); copy {STATE}/*.json into a pull request"
    return f"published to {bucket}/{PUBLISH_PREFIX}; the shape ledger refresh workflow prepares the pull request"


STALE_GRACE_HOURS = 48
FIRST_SEEN = "first-stale.json"
BRANCH = "shape-ledger/refresh"


def verdict(now: datetime, first_seen: datetime, stale: int, pull_request: str) -> tuple[str, int]:
    """The run's outcome line and exit code for `stale` tables first seen stale at `first_seen`
    (root ADR-0260): waiting on the refresh pull request, then failed once that has waited 48 hours."""
    hours = (now - first_seen).total_seconds() / 3600
    since = first_seen.strftime("%Y-%m-%dT%H:%MZ")
    if hours < STALE_GRACE_HOURS:
        return f"foundation-job-outcome waiting shape ledgers stale for {stale} tables since {since}; {pull_request}", 0
    return (f"foundation-job-outcome failed shape ledgers stale for {stale} tables since {since}, "
            f"over {STALE_GRACE_HOURS}h; {pull_request}", 1)


def first_seen_at(now: datetime, stale: bool) -> datetime | None:
    """When the ledgers were first seen stale, recorded in the state directory; a run that finds
    them current clears it."""
    path = STATE / FIRST_SEEN
    if not stale:
        path.unlink(missing_ok=True)
        return None
    if path.exists():
        return datetime.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["first_seen"])
    path.write_text(json.dumps({"first_seen": now.isoformat()}) + "\n", encoding="utf-8")
    return now


def github(token: str, method: str, path: str, body: dict | None = None) -> dict | list | None:
    request = urllib.request.Request(
        f"https://api.github.com{path}", method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                 "User-Agent": "foundation-shape-ledger-check"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(f"GitHub {method} {path}: {error.code} {error.read()[:300]!r}") from error


def refresh_pull_request(token: str, repository: str, files: dict[str, str], found: list[str]) -> str:
    """Opens, or brings up to date, the one pull request that carries the captured ledgers: branch
    shape-ledger/refresh is main plus those files, rewritten only when their content changes. The
    shape ledger refresh workflow calls it with its App's token (root ADR-0279); the host does not."""
    repo = f"/repos/{repository}"
    main_sha = github(token, "GET", f"{repo}/git/ref/heads/main")["object"]["sha"]
    base_tree = github(token, "GET", f"{repo}/git/commits/{main_sha}")["tree"]["sha"]
    tree = github(token, "POST", f"{repo}/git/trees", {"base_tree": base_tree, "tree": [
        {"path": path, "mode": "100644", "type": "blob", "content": content} for path, content in files.items()]})
    branch = github(token, "GET", f"{repo}/git/ref/heads/{BRANCH}")
    current_tree = branch and github(token, "GET", f"{repo}/git/commits/{branch['object']['sha']}")["tree"]["sha"]
    if current_tree != tree["sha"]:
        commit = github(token, "POST", f"{repo}/git/commits", {
            "message": "chore(foundation): refresh the production shape ledgers the shape-ledger check captured\n\n"
                       + "\n".join(found), "tree": tree["sha"], "parents": [main_sha]})
        if branch:
            github(token, "PATCH", f"{repo}/git/refs/heads/{BRANCH}", {"sha": commit["sha"], "force": True})
        else:
            github(token, "POST", f"{repo}/git/refs", {"ref": f"refs/heads/{BRANCH}", "sha": commit["sha"]})
    body = ("The shape ledger check (root ADR-0259 §5) found production's shape differs from the ledgers CI "
            "gates on. This branch is main plus the job's own capture, unedited:\n\n"
            + "\n".join(f"- {line}" for line in found)
            + "\n\nA new Postgres table whose CHECKs tie columns together may need synthetic values in "
              "config/postgres-seed-hints.json before CI passes; that part stays human.")
    owner = repository.split("/", 1)[0]
    open_prs = github(token, "GET", f"{repo}/pulls?state=open&head={owner}:{BRANCH}") or []
    if open_prs:
        github(token, "PATCH", f"{repo}/pulls/{open_prs[0]['number']}", {"body": body})
        return open_prs[0]["html_url"]
    created = github(token, "POST", f"{repo}/pulls", {
        "title": "chore(foundation): refresh the production shape ledgers the shape-ledger check captured",
        "head": BRANCH, "base": "main", "body": body})
    return created["html_url"]


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    capture = load_capture()
    lakehouse = capture.capture()
    lakehouse_text = capture.render(lakehouse)
    (STATE / Path(LAKEHOUSE).name).write_text(lakehouse_text, encoding="utf-8")
    postgres_text = subprocess.run(["bash", str(RELEASE / "scripts/ops/postgres-production-shape.sh")],
                                   capture_output=True, text=True, check=True, timeout=600).stdout
    (STATE / Path(POSTGRES).name).write_text(postgres_text, encoding="utf-8")
    found = [f"lakehouse {line}" for line in differences(
        json.loads((RELEASE / LAKEHOUSE).read_text(encoding="utf-8")), lakehouse, "tables")]
    found += [f"postgres {line}" for line in differences(
        json.loads((RELEASE / POSTGRES).read_text(encoding="utf-8")), json.loads(postgres_text), "tables_with_rows")]
    # Current or not: CI compares what production holds now, and says so either way.
    published = publish({Path(LAKEHOUSE).name: lakehouse_text, Path(POSTGRES).name: postgres_text})
    print(f"shape-ledger-check: captures {published}")
    now = datetime.now(timezone.utc)
    first_seen = first_seen_at(now, bool(found))
    if first_seen is None:
        print("shape-ledger-check: production holds the shapes the release's ledgers record")
        print("foundation-job-outcome unchanged")  # root ADR-0260: the run's last line
        return 0
    for line in found:
        print(f"shape-ledger-check: STALE {line}")
    line, status = verdict(now, first_seen, len(found), published)
    print(line)
    return status


if __name__ == "__main__":
    sys.exit(main())
