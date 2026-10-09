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
        for name in ("legal-dong-code-collect.sh", "admitted-writer-runtime.sh", "job-journal.sh"):
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


if __name__ == "__main__":
    unittest.main()
