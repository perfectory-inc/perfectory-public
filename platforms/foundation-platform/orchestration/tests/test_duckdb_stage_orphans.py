"""A Gold version lane's run removes the DuckDB stages dead runs left, and only theirs (root ADR-0599).

The publisher removes its stage when it ends. On 2026-11-19 a parcel-nearby run stopped at its
unit's hour and left a 24 GB stage.duckdb on the SSD every stage shares with the runtime Postgres;
on 2026-11-22 that disk's reserve refused the served parcel boundaries' first build after 9 of its
16 시도. scripts/ops/duckdb-stage-orphans.sh, which gold-versions-build.sh runs before its publisher
opens a stage, removes such stages. These tests run it over a planted state root, with a `systemctl`
that answers each lane's unit state.
"""

import os
import pathlib
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/duckdb-stage-orphans.sh"
BUILD = PLATFORM / "scripts/ops/gold-versions-build.sh"

# What `systemctl show -p ActiveState --value foundation-gold-versions@<lane>.service` answers.
FAKE_SYSTEMCTL = """#!/usr/bin/env bash
unit="${@: -1}"
lane="${unit#foundation-gold-versions@}"
lane="${lane%.service}"
state="$(sed -n "s/^${lane}=//p" "$STATES")"
[[ -n "${state}" ]] || exit 1
printf '%s\\n' "${state}"
"""


class TheOrphans(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="stage-orphans-")
        self.addCleanup(temp.cleanup)
        self.temp = pathlib.Path(temp.name)
        self.root = self.temp / "foundation-gold-versions"
        bin_dir = self.temp / "bin"
        bin_dir.mkdir()
        systemctl = bin_dir / "systemctl"
        systemctl.write_text(FAKE_SYSTEMCTL, encoding="utf-8", newline="\n")
        systemctl.chmod(0o755)
        self.states = self.temp / "states"
        self.path = f"{bin_dir}{os.pathsep}{os.environ['PATH']}"

    def stage(self, lane, run, size=1000):
        """A run of `lane` with a log and a stage holding `size` bytes; the stage's path."""
        directory = self.root / lane / "runs" / run
        stage = directory / ".duckdb-stage-AbC123"
        (stage / "spill").mkdir(parents=True)
        (stage / "stage.duckdb").write_bytes(b"\0" * size)
        (directory / "run.log").write_text("ran\n", encoding="utf-8")
        return stage

    def run_script(self, lane, run, states):
        self.states.write_text("".join(f"{name}={state}\n" for name, state in states.items()), encoding="utf-8")
        current = self.root / lane / "runs" / run
        current.mkdir(parents=True, exist_ok=True)
        return subprocess.run(
            ["bash", str(SCRIPT), str(self.root), lane, str(current)],
            env={"PATH": self.path, "STATES": str(self.states)},
            capture_output=True, text=True, timeout=60, check=False)

    def test_dead_stages_go_and_live_ones_stay(self):
        own_dead = self.stage("parcel-boundary-served", "20261122T204006Z", 1000)
        own_current = self.stage("parcel-boundary-served", "20261123T113000Z", 10)
        # The 2026-11-19 orphan: an earlier run of an inactive lane, which ran again since.
        nearby_dead = self.stage("parcel-nearby", "20261119T072003Z", 24_000)
        nearby_newest = self.stage("parcel-nearby", "20261121T162627Z", 10)
        # A oneshot unit that runs is `activating`: none of its runs is touched.
        energy_old = self.stage("parcel-energy", "20261120T104500Z", 10)
        energy_running = self.stage("parcel-energy", "20261123T104500Z", 10)
        # No answer about a lane's unit keeps its stages.
        unknown_old = self.stage("parcel-demolitions", "20261120T104000Z", 10)
        self.stage("parcel-demolitions", "20261121T104000Z", 10)
        failed_old = self.stage("parcel-factories", "20261120T101500Z", 500)
        self.stage("parcel-factories", "20261121T101500Z", 10)

        result = self.run_script("parcel-boundary-served", "20261123T113000Z", {
            "parcel-nearby": "inactive", "parcel-energy": "activating", "parcel-factories": "failed"})

        self.assertEqual(result.returncode, 0, result.stderr)
        removed = [own_dead, nearby_dead, failed_old]
        for stage in removed:
            self.assertFalse(stage.exists(), stage)
            self.assertTrue((stage.parent / "run.log").exists(), "a dead run keeps its log")
        for stage in (own_current, nearby_newest, energy_old, energy_running, unknown_old):
            self.assertTrue((stage / "stage.duckdb").exists(), stage)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 4, result.stdout)
        for stage, lane, state, size in ((own_dead, "parcel-boundary-served", "own", 1000),
                                         (nearby_dead, "parcel-nearby", "inactive", 24_000),
                                         (failed_old, "parcel-factories", "failed", 500)):
            line = next(line for line in lines if line.startswith(f"removed {stage} "))
            fields = dict(field.split("=", 1) for field in line.split()[2:])
            self.assertEqual((fields["lane"], fields["state"]), (lane, state))
            self.assertGreaterEqual(int(fields["bytes"]), size)
        last = dict(field.split("=", 1) for field in lines[-1].split()[2:])
        self.assertEqual(lines[-1].split()[:2], ["stage", "orphans"])
        self.assertEqual(last["removed"], "3")
        self.assertGreaterEqual(int(last["bytes"]), 25_500)

    def test_nothing_dead_removes_nothing(self):
        current = self.stage("parcel-boundary-served", "20261123T113000Z", 10)
        newest = self.stage("parcel-nearby", "20261121T162627Z", 10)
        result = self.run_script("parcel-boundary-served", "20261123T113000Z", {"parcel-nearby": "inactive"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "stage orphans removed=0 bytes=0\n")
        self.assertTrue((current / "stage.duckdb").exists())
        self.assertTrue((newest / "stage.duckdb").exists())

    def test_the_lane_script_clears_them_before_its_publisher_runs(self):
        text = BUILD.read_text(encoding="utf-8")
        self.assertLess(text.index("duckdb-stage-orphans.sh"), text.index('"${PUBLISHER_BIN}" "${command}"'))
        self.assertIn('"${STATE_ROOT%/*}" "${lane}" "${work}"', text)


if __name__ == "__main__":
    unittest.main()
