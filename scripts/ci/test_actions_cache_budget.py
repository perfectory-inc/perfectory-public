#!/usr/bin/env python3
"""The Actions cache budget (root ADR-0269)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import actions_cache_budget as acb  # noqa: E402

MB = acb.MB
CONTRACT = {
    "schema_version": 1,
    "cap_percent_of_limit": 90,
    "entries": [
        {"key_prefix": "v0-rust-rust-quality-dependencies-", "max_mb": 2000, "protected": True, "why": "deps"},
        {"key_prefix": "v0-rust-rust-", "max_mb": 1000, "why": "other rust jobs"},
        {"key_prefix": "node-cache-", "max_mb": 500, "why": "pnpm"},
    ],
}
CAP = 3000 * MB


def cache(key: str, mb: int, ref: str = "refs/heads/main") -> dict:
    return {"key": key, "size_in_bytes": mb * MB, "ref": ref}


class JudgeTest(unittest.TestCase):
    def test_a_cache_inside_every_budget_passes(self) -> None:
        breaches, rows = acb.judge(CONTRACT, CAP, [cache("v0-rust-rust-quality-dependencies-Linux-x", 1800), cache("v0-rust-rust-Linux-y", 900)])
        self.assertEqual(breaches, [])
        self.assertEqual(rows[0][1:3], (1800 * MB, 1))
        self.assertEqual(rows[1][1:3], (900 * MB, 1), "the longest prefix owns a key")

    def test_a_total_over_the_cap_fails(self) -> None:
        caches = [cache("v0-rust-rust-quality-dependencies-a", 1900), cache("v0-rust-rust-b", 900), cache("node-cache-c", 400)]
        self.assertTrue(any("over the 3000 MB cap" in b for b in acb.judge(CONTRACT, CAP, caches)[0]))

    def test_an_entry_over_its_max_fails(self) -> None:
        caches = [cache("v0-rust-rust-quality-dependencies-a", 1000), cache("node-cache-a", 300), cache("node-cache-b", 300, "refs/heads/gh-readonly-queue/main/pr-1-x")]
        self.assertTrue(any("`node-cache-` holds 600 MB, over its 500 MB" in b for b in acb.judge(CONTRACT, CAP, caches)[0]))

    def test_an_unbudgeted_key_fails(self) -> None:
        caches = [cache("v0-rust-rust-quality-dependencies-a", 100), cache("v0-postgres-integration-x", 10)]
        self.assertTrue(any("belongs to no entry" in b for b in acb.judge(CONTRACT, CAP, caches)[0]))

    def test_a_missing_protected_entry_fails(self) -> None:
        breaches, _ = acb.judge(CONTRACT, CAP, [cache("v0-rust-rust-x", 10)])
        self.assertTrue(any("protected `v0-rust-rust-quality-dependencies-` is missing" in b for b in breaches))


class ContractTest(unittest.TestCase):
    def test_the_repository_contract_is_well_formed_and_reads_the_one_limit(self) -> None:
        data, cap = acb.load()
        self.assertEqual(acb.check(data), [])
        self.assertEqual(cap, 9 * 10**9, "90% of the 10 GB in tools/github/actions-cache-policy.json")

    def test_a_malformed_contract_is_refused(self) -> None:
        bad = {"schema_version": 1, "cap_percent_of_limit": 120, "entries": [{"key_prefix": "a-", "max_mb": 0, "why": "x"}, {"key_prefix": "a-", "max_mb": 5, "why": ""}]}
        errors = acb.check(bad)
        for expected in ("cap_percent_of_limit", "unique", "max_mb", "why", "protected"):
            self.assertTrue(any(expected in e for e in errors), (expected, errors))




class PruneTest(unittest.TestCase):
    """A new pin set or lockfile replaces its family's old key (2026-10-19: 571 MB against 400)."""

    CONTRACT = {**CONTRACT, "entries": CONTRACT["entries"] + [
        {"key_prefix": "pinned-release-binaries-v1-", "max_mb": 300, "one_key": True, "why": "pins"},
        {"key_prefix": "node-cache-gongzzang-", "max_mb": 450, "one_key": True, "why": "pnpm"},
    ]}

    def entry(self, key: str, created: str, ref: str = "refs/heads/main") -> dict:
        return {**cache(key, 100, ref), "id": key, "created_at": created}

    def test_only_the_newest_of_each_family_stays(self) -> None:
        old, new = "a" * 64, "b" * 64
        caches = [
            self.entry(f"pinned-release-binaries-v1-Linux-{old}", "2026-10-18T04:23:00Z"),
            self.entry(f"pinned-release-binaries-v1-Linux-{new}", "2026-10-18T21:57:00Z"),
            self.entry(f"pinned-release-binaries-v1-Windows-{old}", "2026-10-18T04:23:00Z"),
            self.entry(f"pinned-release-binaries-v1-Windows-{new}", "2026-10-18T21:58:00Z"),
            self.entry(f"node-cache-gongzzang-Linux-pnpm-{new}", "2026-10-18T06:31:00Z"),
        ]
        stale = {cache["key"] for cache in acb.superseded(self.CONTRACT, caches)}
        self.assertEqual(stale, {f"pinned-release-binaries-v1-Linux-{old}", f"pinned-release-binaries-v1-Windows-{old}"})

    def test_an_entry_not_marked_one_key_and_other_refs_are_never_pruned(self) -> None:
        caches = [
            self.entry("v0-rust-rust-quality-dependencies-Linux-" + "a" * 64, "2026-10-17T00:00:00Z"),
            self.entry("v0-rust-rust-quality-dependencies-Linux-" + "b" * 64, "2026-10-18T00:00:00Z"),
            self.entry("pinned-release-binaries-v1-Linux-" + "a" * 64, "2026-10-17T00:00:00Z", "refs/pull/1/merge"),
            self.entry("pinned-release-binaries-v1-Linux-" + "b" * 64, "2026-10-18T00:00:00Z"),
        ]
        self.assertEqual(acb.superseded(self.CONTRACT, caches), [])

    def test_one_key_on_a_protected_entry_is_refused(self) -> None:
        bad = {**CONTRACT, "entries": [{**CONTRACT["entries"][0], "one_key": True}] + CONTRACT["entries"][1:]}
        self.assertTrue(any("one_key" in error for error in acb.check(bad)))


if __name__ == "__main__":
    unittest.main()
