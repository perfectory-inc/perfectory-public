#!/usr/bin/env python3
"""Self-test of the OSV change verdict (root ADR-0568): each refused shape is planted and must fail."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import osv_change_verdict as verdict_module  # noqa: E402
from osv_change_verdict import (  # noqa: E402
    Finding,
    findings,
    fixes_for,
    load_contract,
    relative_lockfile,
    severity_class,
    verdict,
)

CONTRACT = load_contract()
NOW = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)
LOCK = "products/example/pnpm-lock.yaml"


def finding(severity: str = "8.7", lockfile: str = LOCK) -> Finding:
    return Finding(
        lockfile=lockfile,
        package="npm/picomatch@4.0.7",
        ids=("GHSA-fc5q-39xc-vjpf",),
        aliases=("CVE-2026-112366", "GHSA-fc5q-39xc-vjpf"),
        severity=severity,
    )


def pull(
    number: int = 7,
    created: str = "2026-11-18T06:00:00Z",
    body: str = "fixes CVE-2026-112366",
    label: str = "security",
    state: str = "OPEN",
    files: tuple[str, ...] = (LOCK,),
) -> dict:
    return {
        "number": number,
        "title": "fix(security): picomatch",
        "body": body,
        "createdAt": created,
        "labels": [{"name": label}],
        "state": state,
        "files": [{"path": path} for path in files],
    }


class VerdictTest(unittest.TestCase):
    def test_a_touched_lockfile_fails_even_with_a_fix_open(self) -> None:
        passes, why = verdict(finding(), {LOCK}, [pull()], CONTRACT, NOW)
        self.assertFalse(passes)
        self.assertIn("touches the lockfile", why)

    def test_a_lockfile_path_that_could_not_be_told_apart_counts_as_touched(self) -> None:
        self.assertFalse(verdict(finding(lockfile=""), set(), [pull(files=("osv-scanner.toml",))], CONTRACT, NOW)[0])

    def test_an_untouched_lockfile_without_a_fix_fails(self) -> None:
        passes, why = verdict(finding(), set(), [], CONTRACT, NOW)
        self.assertFalse(passes)
        self.assertIn("no open 'security' pull request", why)

    def test_an_untouched_lockfile_with_a_young_fix_is_a_warning(self) -> None:
        passes, why = verdict(finding(), {"README.md"}, [pull()], CONTRACT, NOW)
        self.assertTrue(passes, why)
        self.assertIn("#7", why)
        self.assertIn("2026-11-21T06:00:00Z", why)  # high: 72 hours after the fix opened

    def test_an_allow_list_entry_is_a_fix_too(self) -> None:
        self.assertTrue(verdict(finding(), set(), [pull(files=("osv-scanner.toml",))], CONTRACT, NOW)[0])

    def test_a_body_that_names_the_id_without_changing_the_lockfile_is_not_a_fix(self) -> None:
        self.assertFalse(verdict(finding(), set(), [pull(files=("README.md",))], CONTRACT, NOW)[0])
        self.assertFalse(verdict(finding(), set(), [pull(files=())], CONTRACT, NOW)[0])

    def test_the_grace_ends_and_the_gate_fails_again(self) -> None:
        passes, why = verdict(finding(), set(), [pull(created="2026-11-15T11:59:59Z")], CONTRACT, NOW)
        self.assertFalse(passes)
        self.assertIn("grace (high) ended", why)

    def test_critical_and_unscored_get_one_day(self) -> None:
        for score in ("9.8", ""):
            self.assertFalse(verdict(finding(score), set(), [pull(created="2026-11-17T11:00:00Z")], CONTRACT, NOW)[0])
            self.assertTrue(verdict(finding(score), set(), [pull(created="2026-11-17T13:00:00Z")], CONTRACT, NOW)[0])

    def test_the_first_fix_runs_the_clock_even_when_it_was_closed(self) -> None:
        # Closing the first fix and opening another must not restart the grace.
        pulls = [pull(8, created="2026-11-18T11:00:00Z"), pull(7, created="2026-11-14T00:00:00Z", state="CLOSED")]
        passes, why = verdict(finding(), set(), pulls, CONTRACT, NOW)
        self.assertFalse(passes)
        self.assertIn("#7", why)

    def test_a_merged_or_closed_fix_alone_gives_no_grace(self) -> None:
        self.assertFalse(verdict(finding(), set(), [pull(state="MERGED")], CONTRACT, NOW)[0])
        self.assertFalse(verdict(finding(), set(), [pull(state="CLOSED")], CONTRACT, NOW)[0])

    def test_a_pull_without_the_label_is_not_a_fix(self) -> None:
        self.assertFalse(verdict(finding(), set(), [pull(label="dependencies")], CONTRACT, NOW)[0])

    def test_a_pull_naming_another_advisory_is_not_a_fix(self) -> None:
        self.assertFalse(verdict(finding(), set(), [pull(body="fixes CVE-2026-1123660")], CONTRACT, NOW)[0])
        self.assertFalse(verdict(finding(), set(), [pull(body="fixes GHSA-fc5q-39xc-vjpfx")], CONTRACT, NOW)[0])

    def test_a_range_of_ids_names_only_its_first(self) -> None:
        # 2026-11-19, heapless RUSTSEC-2026-0574..0582: a fix must name every id it covers.
        def rustsec(number: str) -> Finding:
            return Finding(LOCK, "crates.io/heapless@0.8.0", (f"RUSTSEC-2026-{number}",), (), "")

        body = "fixes RUSTSEC-2026-0574..0582"
        self.assertTrue(fixes_for(rustsec("0574"), [pull(body=body)], CONTRACT))
        self.assertFalse(fixes_for(rustsec("0578"), [pull(body=body)], CONTRACT))

    def test_any_alias_names_it_case_ignored(self) -> None:
        self.assertTrue(fixes_for(finding(), [pull(body="Closes ghsa-fc5q-39xc-vjpf.")], CONTRACT))

    def test_an_unreadable_base_fails(self) -> None:
        passes, why = verdict(finding(), None, [pull()], CONTRACT, NOW)
        self.assertFalse(passes)
        self.assertIn("base could not be read", why)

    def test_severity_classes(self) -> None:
        self.assertEqual(severity_class("9.0", CONTRACT), "critical")
        self.assertEqual(severity_class("7.0", CONTRACT), "high")
        self.assertEqual(severity_class("6.3", CONTRACT), "other")
        self.assertEqual(severity_class("", CONTRACT), "unscored")
        self.assertEqual(severity_class("CVSS:4.0/AV:N", CONTRACT), "unscored")

    def test_a_path_outside_the_repository_is_not_named(self) -> None:
        self.assertEqual(relative_lockfile("/elsewhere/pnpm-lock.yaml", Path(__file__).resolve().parents[2]), "")
        self.assertEqual(relative_lockfile("x/pnpm-lock.yaml", None), "")


class ReportShapeTest(unittest.TestCase):
    def test_a_report_shape_it_does_not_know_raises(self) -> None:
        with self.assertRaises(KeyError):
            findings({"results": [{"source": {"path": "x"}, "packages": [{"package": {"name": "a", "ecosystem": "npm"}}]}]}, None)
        with self.assertRaises(KeyError):
            findings({}, None)

    def test_findings_but_none_read_fails(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            report = Path(scratch) / "r.json"
            report.write_text(json.dumps({"results": []}), encoding="utf-8")
            pulls = Path(scratch) / "p.json"
            pulls.write_text("[]", encoding="utf-8")
            self.assertEqual(verdict_module.main([str(report), "--root", scratch, "--fix-pulls", str(pulls)]), 1)


class ContractTest(unittest.TestCase):
    def refused(self, change) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            bad = json.loads(verdict_module.CONTRACT.read_text(encoding="utf-8"))
            change(bad)
            path = Path(scratch) / "c.json"
            path.write_text(json.dumps(bad), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_contract(path)

    def test_a_grace_longer_than_a_week_is_refused(self) -> None:
        self.refused(lambda c: c["grace_hours"].update(other=169))

    def test_a_missing_class_is_refused(self) -> None:
        self.refused(lambda c: c["grace_hours"].pop("unscored"))

    def test_unscored_longer_than_critical_is_refused(self) -> None:
        self.refused(lambda c: c["grace_hours"].update(unscored=72))

    def test_inverted_thresholds_are_refused(self) -> None:
        self.refused(lambda c: c.update(critical_from=6.0))

    def test_a_missing_key_is_refused(self) -> None:
        self.refused(lambda c: c.pop("allow_list"))


class EndToEndTest(unittest.TestCase):
    """A real repository: base commit, a change that touches one lockfile and not the other."""

    def test_only_the_untouched_lockfile_is_graced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "repo"
            root.mkdir()

            def git(*args: str) -> str:
                return subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True).stdout

            git("init", "-q")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "test")
            for name in ("a/pnpm-lock.yaml", "b/pnpm-lock.yaml"):
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text("lockfileVersion: '9.0'\n", encoding="utf-8")
            git("add", "-A")
            git("commit", "-q", "-m", "base")
            base = git("rev-parse", "HEAD").strip()
            (root / "a/pnpm-lock.yaml").write_text("lockfileVersion: '9.0'\n# changed\n", encoding="utf-8")
            git("commit", "-q", "-am", "change")

            def result(lock: str) -> dict:
                return {
                    "source": {"path": str(root / lock), "type": "lockfile"},
                    "packages": [
                        {
                            "package": {"name": "picomatch", "version": "4.0.7", "ecosystem": "npm"},
                            "groups": [{"ids": ["GHSA-fc5q-39xc-vjpf"], "aliases": ["CVE-2026-112366"], "max_severity": "8.7"}],
                        }
                    ],
                }

            report = Path(scratch) / "report.json"
            pulls = Path(scratch) / "pulls.json"
            pulls.write_text(json.dumps([pull(files=("osv-scanner.toml",))]), encoding="utf-8")
            args = ["--root", str(root), "--base", base, "--fix-pulls", str(pulls), "--now", "2026-11-18T12:00:00Z"]
            report.write_text(json.dumps({"results": [result("b/pnpm-lock.yaml")]}), encoding="utf-8")
            self.assertEqual(verdict_module.main([str(report), *args]), 0)
            report.write_text(json.dumps({"results": [result("a/pnpm-lock.yaml"), result("b/pnpm-lock.yaml")]}), encoding="utf-8")
            self.assertEqual(verdict_module.main([str(report), *args]), 1)
            self.assertEqual(
                [f.lockfile for f in findings(json.loads(report.read_text(encoding="utf-8")), root)],
                ["a/pnpm-lock.yaml", "b/pnpm-lock.yaml"],
            )
            # No base: everything counts as touched.
            self.assertEqual(verdict_module.main([str(report), "--root", str(root), "--fix-pulls", str(pulls)]), 1)


if __name__ == "__main__":
    unittest.main()
