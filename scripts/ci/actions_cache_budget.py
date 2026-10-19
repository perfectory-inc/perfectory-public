#!/usr/bin/env python3
"""Hold the repository's Actions cache under one budget (root ADR-0269).

    check    the contract is well formed (unique prefixes, positive limits, a protected entry).
    measure  read the live cache list (GitHub API, paginated), print a Markdown table (also to
             $GITHUB_STEP_SUMMARY), and exit 1 when the total passes the cap, an entry passes its
             max_mb, a protected entry is missing, or a key belongs to no entry.
    prune    delete what a newer key replaced: for an entry marked one_key, every main entry but
             the newest of its family (the key without its trailing content hash). Needs a token
             that may delete caches (the scheduled cleanup job's actions: write).

The cap is cap_percent_of_limit of max_cache_size_gb in tools/github/actions-cache-policy.json, the
one place the limit is written. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "tools" / "ci" / "actions-cache-budget.contract.json"
POLICY = ROOT / "tools" / "github" / "actions-cache-policy.json"
MB = 10**6


def load(contract: Path = CONTRACT, policy: Path = POLICY) -> tuple[dict, int]:
    """The contract and the cap in bytes."""
    data = json.loads(contract.read_text(encoding="utf-8"))
    limit_gb = json.loads(policy.read_text(encoding="utf-8"))["max_cache_size_gb"]
    return data, int(limit_gb * 10**9 * data["cap_percent_of_limit"] / 100)


def check(data: dict) -> list[str]:
    errors = []
    if data.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    percent = data.get("cap_percent_of_limit")
    if isinstance(percent, bool) or not isinstance(percent, (int, float)) or not 0 < percent <= 100:
        errors.append("cap_percent_of_limit must be in (0, 100]")
    prefixes = [entry.get("key_prefix", "") for entry in data.get("entries", [])]
    if len(prefixes) != len(set(prefixes)):
        errors.append("key_prefix values must be unique")
    for entry in data.get("entries", []):
        if not entry.get("key_prefix") or not str(entry.get("why", "")).strip():
            errors.append(f"{entry}: every entry names its key_prefix and why")
        size = entry.get("max_mb")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            errors.append(f"{entry.get('key_prefix')}: max_mb must be a positive integer")
        if not isinstance(entry.get("one_key", False), bool) or (entry.get("one_key") and entry.get("protected")):
            errors.append(f"{entry.get('key_prefix')}: one_key is true or false, and never on a protected entry")
    if not any(entry.get("protected") for entry in data.get("entries", [])):
        errors.append("at least one entry is protected")
    return errors


def owner(entries: list[dict], key: str) -> dict | None:
    """The entry with the longest key_prefix the key starts with."""
    matches = [entry for entry in entries if key.startswith(entry["key_prefix"])]
    return max(matches, key=lambda entry: len(entry["key_prefix"])) if matches else None


# A key's content hash: what a new lockfile or pin set changes while the family stays the same.
CONTENT_HASH = re.compile(r"-[0-9a-f]{40,}$")


def superseded(data: dict, caches: list[dict]) -> list[dict]:
    """main's entries a newer key of the same family replaced, for entries marked one_key.

    A new pin set or lockfile saves a new key and leaves the old one, which nothing restores again
    (2026-10-19: two pinned sets, 571 MB against 400). The newest by creation stays.
    """
    families: dict[str, list[dict]] = {}
    for cache in caches:
        entry = owner(data["entries"], cache["key"])
        if entry is None or not entry.get("one_key") or cache.get("ref") != "refs/heads/main":
            continue
        families.setdefault(CONTENT_HASH.sub("", cache["key"]), []).append(cache)
    stale = []
    for members in families.values():
        members.sort(key=lambda cache: cache["created_at"])
        stale += members[:-1]
    return stale


def delete_cache(repository: str, cache_id: int) -> None:
    request = urllib.request.Request(f"https://api.github.com/repos/{repository}/actions/caches/{cache_id}",
                                     method="DELETE", headers={"Accept": "application/vnd.github+json"})
    request.add_header("Authorization", f"Bearer {os.environ['GH_TOKEN']}")
    with urllib.request.urlopen(request, timeout=20):
        pass


def judge(data: dict, cap: int, caches: list[dict]) -> tuple[list[str], list[tuple[str, int, int, int]]]:
    """(breaches, rows of prefix, bytes used, entry count, max bytes)."""
    used: dict[str, list[int]] = {entry["key_prefix"]: [0, 0] for entry in data["entries"]}
    breaches = []
    for cache in caches:
        entry = owner(data["entries"], cache["key"])
        if entry is None:
            breaches.append(f"`{cache['key']}` ({cache['size_in_bytes'] // MB} MB, {cache['ref']}) belongs to no entry")
            continue
        used[entry["key_prefix"]][0] += cache["size_in_bytes"]
        used[entry["key_prefix"]][1] += 1
    rows = []
    for entry in data["entries"]:
        size, count = used[entry["key_prefix"]]
        rows.append((entry["key_prefix"], size, count, entry["max_mb"] * MB))
        if size > entry["max_mb"] * MB:
            breaches.append(f"`{entry['key_prefix']}` holds {size // MB} MB, over its {entry['max_mb']} MB")
        if entry.get("protected") and count == 0:
            breaches.append(f"protected `{entry['key_prefix']}` is missing: evicted, or main has not saved it")
    total = sum(cache["size_in_bytes"] for cache in caches)
    if total > cap:
        breaches.append(f"the cache holds {total // MB} MB, over the {cap // MB} MB cap")
    return breaches, rows


def table(rows, total: int, cap: int, breaches: list[str]) -> str:
    lines = [f"### Actions cache: {total // MB} MB of {cap // MB} MB", "", "| key prefix | MB | entries | max MB |", "|---|---:|---:|---:|"]
    lines += [f"| `{prefix}` | {size // MB} | {count} | {limit // MB} |" for prefix, size, count, limit in rows]
    lines += [""] + [f"- FAIL {breach}" for breach in breaches]
    return "\n".join(lines) + "\n"


def fetch_caches(repository: str) -> list[dict]:
    caches, page = [], 1
    while True:
        request = urllib.request.Request(
            f"https://api.github.com/repos/{repository}/actions/caches?per_page=100&page={page}",
            headers={"Accept": "application/vnd.github+json"},
        )
        token = os.environ.get("GH_TOKEN")
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(request, timeout=20) as response:
            batch = json.load(response)["actions_caches"]
        caches += batch
        if len(batch) < 100:
            return caches
        page += 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    for name in ("measure", "prune"):
        sub.add_parser(name).add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    args = parser.parse_args(argv)
    data, cap = load()
    errors = check(data)
    for error in errors:
        print(f"FAIL actions-cache-budget: {error}", file=sys.stderr)
    if errors or args.command == "check":
        if not errors:
            print(f"OK actions-cache-budget: {len(data['entries'])} entries, cap {cap // MB} MB")
        return 1 if errors else 0
    if not args.repository:
        print("FAIL actions-cache-budget: no --repository and no GITHUB_REPOSITORY", file=sys.stderr)
        return 2
    caches = fetch_caches(args.repository)
    if args.command == "prune":
        stale = superseded(data, caches)
        for cache in stale:
            delete_cache(args.repository, cache["id"])
            print(f"deleted `{cache['key']}` ({cache['size_in_bytes'] // MB} MB, created {cache['created_at']})")
        print(f"OK actions-cache-budget prune: {len(stale)} superseded entries deleted")
        return 0
    breaches, rows = judge(data, cap, caches)
    text = table(rows, sum(c["size_in_bytes"] for c in caches), cap, breaches)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
