"""The 법정동 collection job's own command lines (scripts/ops/legal-dong-code-collect.sh, root ADR-0143).

The steward runs `legal-dong-code-collect.sh steward ...` at a terminal. These tests run that real
script from an installed release layout (root ADR-0134, as in test_gold_panel_rebuild.py), so the
interpreter flags the script passes are the ones exercised: `legal_dong_code_change_pairs.py`
imports its sibling `code_go_kr_legal_dong`, and `python3 -I` drops the script's own directory
from `sys.path`, so a call through `main()` alone could not see the command die.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

PLATFORM = job_specs.PLATFORM_ROOT
RELEASE_ID = "f" * 40
JOBS = "infra/lakehouse/spark/jobs"
CONTRACTS = "infra/lakehouse/contracts"


class StewardCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="legal-dong-collect-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("legal-dong-code-collect.sh", "admitted-writer-runtime.sh", "job-journal.sh", "database-url.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        shutil.copytree(PLATFORM / JOBS, release / JOBS, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(PLATFORM / CONTRACTS, release / CONTRACTS)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text("#!/bin/sh\nexit 1\n")
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        self.script = base / "current/scripts/ops/legal-dong-code-collect.sh"
        self.state = root / "state"
        self.state.mkdir()
        # The list the last pairing run wrote: one synthetic change waiting for a steward.
        (self.state / "steward-review.json").write_text(json.dumps({"review": [
            {"kind": "pair", "old_code": "9811010100", "level": "eupmyeondong", "reason": "no_candidate",
             "candidates": [], "as_of": "20990701", "name": "합성도 가구 갑동"}]}), encoding="utf-8")
        self.env = {"PATH": os.environ["PATH"], "FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT": str(self.state)}

    def steward(self, *args):
        return subprocess.run(["bash", str(self.script), "steward", "--steward", "steward-a", "--reason", "공지 확인", *args],
                              env=self.env, capture_output=True, text=True, timeout=120)

    def staged(self):
        pending = self.state / "steward" / "pending"
        return sorted(pending.glob("*.json")) if pending.exists() else []

    def test_a_listed_approval_is_staged_by_the_real_command_line(self):
        result = self.steward("--approve", "9811010100=9911010100")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("ModuleNotFoundError", result.stderr)
        [decision] = self.staged()
        self.assertEqual(json.loads(decision.read_text(encoding="utf-8"))["approve"], ["9811010100=9911010100"])
        self.assertIn("steward decision staged", (self.state / "journal.log").read_text(encoding="utf-8"))
        self.assertIn("legal-dong-code steward decision staged", result.stdout, "the unit's journal has it too")

    def test_an_unlisted_approval_is_refused_at_the_terminal_and_stages_nothing(self):
        result = self.steward("--approve", "9811010200=9911010200")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not on the steward list", result.stderr)
        self.assertEqual(self.staged(), [])


# The collector stand-in: writes the table and its manifest as collect-code-go-kr-legal-dong does, and
# logs whether the run stored it (`live`), only fetched it (`dry`), or asked the changed-since probe
# (`probe`, root ADR-0322 §2: ..._CHANGED_SINCE set, never stored).
FAKE_COLLECTOR = r"""#!/usr/bin/env python3
import hashlib, json, os, pathlib, sys
assert sys.argv[1] == "collect-code-go-kr-legal-dong", sys.argv
prefix = "FOUNDATION_PLATFORM_CODE_GO_KR_LEGAL_DONG"
out = pathlib.Path(os.environ[prefix + "_OUTPUT_DIR"])
live = os.environ.get(prefix + "_LIVE_WRITE") == "1"
since = os.environ.get(prefix + "_CHANGED_SINCE")
assert not (live and since), "a probe is never stored"
calls = pathlib.Path(os.environ["FAKE_CALLS"])
name = "regcode-2099070%dT051500Z" % len(calls.read_text().split())
with calls.open("a") as log:
    log.write(("probe" if since else "live" if live else "dry") + "\n")
raw = pathlib.Path(os.environ["FAKE_PROBE" if since else "FAKE_TABLE"]).read_bytes()
(out / "objects").mkdir(parents=True, exist_ok=True)
(out / "objects" / (name + ".html")).write_bytes(raw)
(out / "manifest.json").write_text(json.dumps({"collection_date": "2099-07-02", "changed_since": since, "objects": [{
    "role": "changed_since_probe" if since else "full_table",
    "object_key": "bronze/source=codegokr__legal_dong_code_table/" + name + ".html",
    "local_path": "objects/" + name + ".html", "checksum_sha256": hashlib.sha256(raw).hexdigest(),
    "size_bytes": len(raw), "written": live}]}))
"""


def synthetic_table(rows, first=0):
    """A page the parser reads: the contract's headers and synthetic 시도 97 codes `first`..`rows`."""

    headers = json.loads((PLATFORM / CONTRACTS / "code-go-kr-legal-dong.contract.json").read_text(encoding="utf-8"))
    head = "".join(f"<td>{h}</td>" for h in headers["full_table"]["headers"])
    body = "".join(f"<tr><td>97{i:08d}</td><td>합성 {i}동</td><td>현존</td><td>9700000000</td><td>01</td>"
                   f"<td>20000101</td><td>&nbsp</td><td>&nbsp</td><td>{i}동</td><td>97{i:08d}</td><td>97{i:08d}</td>"
                   for i in range(first, rows))
    return f"<html><body><table><thead><tr>{head}</tr></thead>{body}</table></body></html>"


class DailyCollection(unittest.TestCase):
    """Only a table whose rows changed is stored: 2026-10-04..28 stored the same table 283 times (15 GB),
    because each copy's sha256 differs and the comparison came after the write."""

    def setUp(self):
        StewardCommand.setUp(self)
        publisher = pathlib.Path(self.temp.name) / "opt/foundation-platform/artifacts" / RELEASE_ID / "foundation-outbox-publisher"
        publisher.chmod(0o755)
        publisher.write_text(FAKE_COLLECTOR)
        publisher.chmod(0o555)
        build = publisher.parent / "build.json"
        manifest = json.loads(build.read_text())
        manifest["files"]["foundation-outbox-publisher"] = hashlib.sha256(publisher.read_bytes()).hexdigest()
        build.write_text(json.dumps(manifest))
        self.table = pathlib.Path(self.temp.name) / "table.html"
        self.probe = pathlib.Path(self.temp.name) / "probe.html"
        self.calls = pathlib.Path(self.temp.name) / "calls.log"
        self.calls.write_text("")
        self.env.update({"DATABASE_URL": "postgres://fixture/fixture", "FAKE_TABLE": str(self.table),
                         "FAKE_PROBE": str(self.probe), "FAKE_CALLS": str(self.calls)})
        self.min_rows = json.loads((PLATFORM / CONTRACTS / "code-go-kr-legal-dong.contract.json")
                                   .read_text(encoding="utf-8"))["full_table"]["bounds"]["min_rows"]

    def collect(self, rows, probe_rows=2):
        """One run against a table of `rows` codes whose probe window lists the last `probe_rows`."""

        self.table.write_text(synthetic_table(rows), encoding="utf-8")
        self.probe.write_text(synthetic_table(rows, first=rows - probe_rows), encoding="utf-8")
        before = self.calls.read_text().split()
        result = subprocess.run(["bash", str(self.script)], env=self.env, capture_output=True, text=True, timeout=300)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        return result.stdout.splitlines()[-1], self.calls.read_text().split()[len(before):]

    def pending(self):
        return sorted(p.name for p in (self.state / "pending").iterdir())

    def accepted(self):
        return json.loads((self.state / "accepted.json").read_text())

    def test_an_unchanged_table_is_confirmed_by_the_probe_and_stores_nothing(self):
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome changed", ["probe", "dry", "live"]))
        [first] = self.pending()
        anchored = self.accepted()
        self.assertEqual(anchored["confirmed_by"], "full")
        self.assertIn("since", anchored["probe"])

        # The 2026-10 runs fetched 58 MB about ten times a day; the probe answers instead (ADR-0322 §2).
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome unchanged", ["probe"]))
        self.assertEqual(self.pending(), [first])
        accepted = self.accepted()
        self.assertGreaterEqual(accepted["checked_at_utc"], anchored["checked_at_utc"], "the hub's freshness check still sees it")
        self.assertEqual((accepted["confirmed_by"], accepted["probe"]), ("probe", anchored["probe"]))
        self.assertEqual(list((self.state / "runs").glob("*/probe")), [], "the probe's copy is not kept")

        # A code the site dates inside the window: the table is fetched, stored and the window re-anchored.
        self.assertEqual(self.collect(self.min_rows + 1), ("foundation-job-outcome changed", ["probe", "dry", "live"]))
        self.assertEqual(len(self.pending()), 2)
        self.assertNotEqual(self.accepted()["probe"]["digest"], anchored["probe"]["digest"])

    def test_the_full_table_is_still_read_when_the_reconcile_interval_passed(self):
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome changed", ["probe", "dry", "live"]))
        stale = self.accepted()
        stale["full_checked_at_utc"] = "2000-01-01T00:00:00+00:00"
        (self.state / "accepted.json").write_text(json.dumps(stale))
        # A change the probe cannot see (no new date) is caught here; an equal table stores nothing.
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome unchanged", ["probe", "dry"]))
        self.assertEqual(self.accepted()["confirmed_by"], "full")
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome unchanged", ["probe"]))

    def test_a_table_accepted_before_the_probe_is_read_whole_once_to_anchor_it(self):
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome changed", ["probe", "dry", "live"]))
        legacy = {k: v for k, v in self.accepted().items() if k in ("table_digest", "row_count", "table_object_key",
                                                                       "handoff", "checked_at_utc")}
        (self.state / "accepted.json").write_text(json.dumps(legacy))
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome unchanged", ["probe", "dry"]))
        self.assertEqual(self.collect(self.min_rows), ("foundation-job-outcome unchanged", ["probe"]))


if __name__ == "__main__":
    unittest.main()
