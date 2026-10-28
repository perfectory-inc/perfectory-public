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

It also reads, from the same metadata tables and no data file, how each table its readers prune by
key is laid out and was last written (root ADR-0314): a one-region read that opens most of the
table's files, or a commit that added far more of its bytes than the share of keys it says it
changed, fails the run at once. A rewrite that does not say what it changed is named.
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


# Layout and write amplification (root ADR-0314 §3, §4). A table its readers prune by key is one
# whose contract writes it range-distributed by its sort order: the leading sort column is its
# cluster key. For `pnu` a reader's unit is one 시도, the key's first two digits (a by-PNU bake
# shard, root ADR-0164). On 2026-10-28 every one of gold.parcel_panel's 9 files spanned PNUs 11..52,
# so each shard read the whole 4 GB table.
CONTRACTS = "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
REGION_DIGITS = {"pnu": 2}
# A one-region read may open at most this share of the table's data files. Range-written tables
# measured 2026-10-28 read 0.16-0.33; the three whose files all span the country read 1.00.
REGION_READ_SHARE_LIMIT = 0.5
# A commit that changed a share of the keys may add at most this many times that share of the
# table's bytes; the share is floored, so a small table or change is not judged on noise. On
# 2026-10-26 a 0.12% change added 100% of gold.parcel_panel's bytes.
AMPLIFICATION_LIMIT = 10
KEY_SHARE_FLOOR = 0.01
# The commit's own count of the keys it changed, a snapshot summary property its writer sets
# (root ADR-0314 §4); a commit that lacks it cannot be judged and is named instead.
KEYS_CHANGED = "foundation.keys-changed"
# A derived change log beside its table (root ADR-0191 §1): exempt from the layout judgment.
CHANGE_LOG_SUFFIX = "__changes"


def clustered_tables(contracts: dict) -> dict[str, str]:
    """Each table written range-distributed by a cluster key the check knows a region of, with
    that key. Only contract tables: a derived change log (`gold.<table>__changes`, root ADR-0191
    §1) has no contract and is not judged — it is appended one commit per change set and read by
    snapshot, never pruned by key range, so neither its layout nor its byte share says anything."""
    return {name: spec["sort_order"][0] for name, spec in sorted(contracts.items())
            if not name.endswith(CHANGE_LOG_SUFFIX)
            if spec.get("write_distribution") == "range" and spec.get("sort_order")
            and spec["sort_order"][0] in REGION_DIGITS}


def region_read_share(bounds: list[tuple[str, str]], digits: int) -> float:
    """The mean share of data files a read of one region opens, by each file's lower and upper key
    bound: a region is the key's first `digits` characters, and a file holds every region between
    its bounds' regions. The regions are those the bounds name."""
    spans = [(low[:digits], high[:digits]) for low, high in bounds]
    regions = sorted({region for span in spans for region in span})
    if len(spans) < 2 or not regions:
        return 0.0
    return sum(sum(low <= region <= high for low, high in spans) for region in regions) / (len(regions) * len(spans))


def amplification(summary: dict) -> tuple[float, float | None]:
    """(share of the table's bytes the commit added, share of its keys it says it changed)."""
    added, total = int(summary.get("added-files-size", 0)), int(summary.get("total-files-size", 0))
    byte_share = added / total if total else 0.0
    keys, records = summary.get(KEYS_CHANGED), int(summary.get("total-records", 0))
    return byte_share, (int(keys) / records if keys is not None and records else None)


def layout_findings(table: str, key: str, bounds: list[tuple[str, str]], summary: dict | None) -> tuple[list[str], list[str]]:
    """(failures, notes) for one clustered table: its files' key ranges, and its last data commit
    (`summary`, None for a table with no commit after its first)."""
    failures, notes = [], []
    share = region_read_share(bounds, REGION_DIGITS[key])
    if share > REGION_READ_SHARE_LIMIT:
        failures.append(f"{table}: a one-region read of {key} opens {share:.0%} of its {len(bounds)} data files "
                        f"(limit {REGION_READ_SHARE_LIMIT:.0%}); its files are not ordered by {key}")
    if summary is not None:
        byte_share, key_share = amplification(summary)
        if key_share is None:
            if byte_share > REGION_READ_SHARE_LIMIT:
                notes.append(f"{table}: its last commit added {byte_share:.0%} of its bytes and does not say "
                             f"how many keys it changed ({KEYS_CHANGED})")
        elif byte_share > AMPLIFICATION_LIMIT * max(key_share, KEY_SHARE_FLOOR):
            failures.append(f"{table}: its last commit changed {key_share:.2%} of its keys and added "
                            f"{byte_share:.0%} of its bytes (limit {AMPLIFICATION_LIMIT}x the key share, "
                            f"floored at {KEY_SHARE_FLOOR:.0%})")
    return failures, notes


def layout(trino_rows, trino_url: str, contracts: dict, lakehouse: dict) -> tuple[list[str], list[str]]:
    """(failures, notes) for every clustered table production holds rows in, read from Trino's
    `$files` and `$snapshots` metadata tables; no data file is read."""
    failures, notes = [], []
    for table, key in clustered_tables(contracts).items():
        shape = lakehouse["tables"].get(table)
        if not shape or not shape["has_rows"]:
            continue
        field = next(field[0] for field in shape["fields"] if field[1] == key)
        namespace, name = table.split(".", 1)
        bounds = [(low, high) for low, high in trino_rows(trino_url, (
            f"SELECT element_at(lower_bounds, {field}), element_at(upper_bounds, {field}) "
            f'FROM r2.{namespace}."{name}$files" WHERE content = 0')) if low is not None and high is not None]
        last = trino_rows(trino_url, (
            f'SELECT summary FROM r2.{namespace}."{name}$snapshots" '
            "WHERE operation <> 'replace' AND parent_id IS NOT NULL ORDER BY committed_at DESC LIMIT 1"))
        found, noted = layout_findings(table, key, bounds, dict(last[0][0]) if last else None)
        failures += found
        notes += noted
    return failures, notes


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
    trino_url = os.environ.get("FOUNDATION_PLATFORM_TRINO_URL", "http://127.0.0.1:18081").rstrip("/")
    misplaced, notes = layout(capture.trino_rows, trino_url,
                              json.loads((RELEASE / CONTRACTS).read_text(encoding="utf-8"))["contracts"], lakehouse)
    for line in notes:
        print(f"shape-ledger-check: NOTE {line}")
    for line in misplaced:
        print(f"shape-ledger-check: LAYOUT {line}")
    now = datetime.now(timezone.utc)
    first_seen = first_seen_at(now, bool(found))
    if first_seen is None:
        print("shape-ledger-check: production holds the shapes the release's ledgers record")
        if misplaced:
            print(f"foundation-job-outcome failed {len(misplaced)} tables are laid out or written against "
                  "their cluster key (root ADR-0314)")
            return 1
        print("foundation-job-outcome unchanged")  # root ADR-0260: the run's last line
        return 0
    for line in found:
        print(f"shape-ledger-check: STALE {line}")
    line, status = verdict(now, first_seen, len(found), published)
    if misplaced:
        # A layout finding fails the run at once; the ledgers' 48 hours do not apply to it.
        line = line.replace("foundation-job-outcome waiting", "foundation-job-outcome failed", 1)
        line += f"; {len(misplaced)} tables are laid out or written against their cluster key (root ADR-0314)"
        status = 1
    print(line)
    return status


if __name__ == "__main__":
    sys.exit(main())
