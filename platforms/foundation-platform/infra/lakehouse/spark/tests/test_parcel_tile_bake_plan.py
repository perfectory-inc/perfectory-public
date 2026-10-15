"""The parcel tile bake's plan (scripts/ops/parcel-tile-bake.sh): the edition, its snapshot id and
its 시도 come from the source contract and the Bronze ledger's editions (root ADR-0224), never from
a list in the contract. The script's own Python is run here, as the script runs it."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))

import vworld_parcel_editions  # noqa: E402
from test_lakehouse_lineage_migration import ledger_editions  # noqa: E402

SERVED = vworld_parcel_editions.served(vworld_parcel_editions.load())


def plan_source() -> str:
    script = (PLATFORM / "scripts/ops/parcel-tile-bake.sh").read_text(encoding="utf-8")
    found = re.search(r'"\$\{which_edition\}" <<\'PY\'\n(.*?)\nPY\n', script, re.S)
    if found is None:
        raise AssertionError("parcel-tile-bake.sh has no plan heredoc")
    return found.group(1)


def plan(which: str, document: dict | None) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k != "VWORLD_PARCEL_EDITIONS"}
    with tempfile.TemporaryDirectory() as tmp:
        if document is not None:
            path = Path(tmp) / "editions.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            env["VWORLD_PARCEL_EDITIONS"] = str(path)
        return subprocess.run([sys.executable, "-I", "-", str(PLATFORM), which], input=plan_source(),
                              capture_output=True, text=True, env=env, check=False, encoding="utf-8")


class TileBakePlan(unittest.TestCase):
    def test_a_rehearsal_takes_the_ledgers_newest_edition_and_its_sido(self) -> None:
        done = plan("newest", ledger_editions(SERVED, "209909"))
        self.assertEqual(done.returncode, 0, done.stderr)
        edition, snapshot, sido, _work_root = done.stdout.splitlines()
        self.assertEqual((edition, sido), ("209909", "97"))
        self.assertEqual(snapshot, vworld_parcel_editions.snapshot_id(vworld_parcel_editions.load(), "209909"))

    def test_the_served_bake_takes_the_served_edition(self) -> None:
        done = plan("served", ledger_editions(SERVED, "209909"))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.splitlines()[0], SERVED)

    def test_the_script_hands_its_plan_the_ledgers_editions(self) -> None:
        script = (PLATFORM / "scripts/ops/parcel-tile-bake.sh").read_text(encoding="utf-8")
        before_plan = script[:script.index(plan_source())]
        self.assertIn('vworld-parcel-editions > "${STATE_ROOT}/parcel-editions.json"', before_plan)
        self.assertIn('export VWORLD_PARCEL_EDITIONS="${STATE_ROOT}/parcel-editions.json"', before_plan)

    def test_without_the_ledgers_editions_the_plan_refuses(self) -> None:
        done = plan("newest", None)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("ADR-0224", done.stderr)


if __name__ == "__main__":
    unittest.main()
