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
UNIT = PLATFORM / "infra/systemd/foundation-gold-versions@.service"

# systemd as scripts/deploy/job_units.py asks it whether a lane's unit runs: `systemctl list-units
# --all --full --plain --no-legend --type=service <unit>`. $STATES holds `<lane>=<ActiveState>`; a
# lane it does not name is a unit systemd has not loaded (listed nothing), and `unanswerable` is a
# systemd that cannot be asked.
FAKE_SYSTEMCTL = """#!/usr/bin/env bash
[[ "$1" == list-units ]] || exit 64
unit="${@: -1}"
lane="${unit#foundation-gold-versions@}"
lane="${lane%.service}"
state="$(sed -n "s/^${lane}=//p" "$STATES")"
[[ -n "${state}" ]] || exit 0
[[ "${state}" != unanswerable ]] || exit 1
printf '%s loaded %s running Gold of %s\\n' "${unit}" "${state}" "${lane}"
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

    def stage(self, lane, run, size=1000, under=""):
        """A run of `lane` with a log and a stage holding `size` bytes (in the run's `under`
        directory, e.g. `verdict`); the stage's path."""
        directory = self.root / lane / "runs" / run
        stage = directory / under / ".duckdb-stage-AbC123"
        (stage / "spill").mkdir(parents=True)
        (stage / "stage.duckdb").write_bytes(b"\0" * size)
        (directory / "run.log").write_text("ran\n", encoding="utf-8")
        return stage

    def run_script(self, lane, run, states):
        self.states.write_text("".join(f"{name}={state}\n" for name, state in states.items()), encoding="utf-8")
        if run == "--stopped":
            current = run
        else:
            current = self.root / lane / "runs" / run
            current.mkdir(parents=True, exist_ok=True)
        return subprocess.run(
            ["bash", str(SCRIPT), str(self.root), lane, str(current)],
            env={"PATH": self.path, "STATES": str(self.states)},
            capture_output=True, text=True, timeout=60, check=False)

    def test_dead_stages_go_and_live_ones_stay(self):
        own_dead = self.stage("parcel-boundary-served", "20261122T204006Z", 1000)
        own_current = self.stage("parcel-boundary-served", "20261123T113000Z", 10)
        # The 2026-11-19 orphan: an earlier run of a stopped lane, which ran again since.
        nearby_dead = self.stage("parcel-nearby", "20261119T072003Z", 24_000)
        nearby_newest = self.stage("parcel-nearby", "20261121T162627Z", 10)
        # A oneshot unit is `activating` for its whole run: none of its runs is touched.
        energy_old = self.stage("parcel-energy", "20261120T104500Z", 10)
        energy_running = self.stage("parcel-energy", "20261123T104500Z", 10)
        # A systemd that cannot be asked keeps the lane's stages (job_units.py exits 2).
        unanswered_old = self.stage("parcel-demolitions", "20261120T104000Z", 10)
        self.stage("parcel-demolitions", "20261121T104000Z", 10)
        failed_old = self.stage("parcel-factories", "20261120T101500Z", 500)
        self.stage("parcel-factories", "20261121T101500Z", 10)
        # A unit systemd has not loaded does not run.
        unloaded_old = self.stage("parcel-sub-parcels", "20261120T105000Z", 300)
        self.stage("parcel-sub-parcels", "20261121T105000Z", 10)

        result = self.run_script("parcel-boundary-served", "20261123T113000Z", {
            "parcel-nearby": "inactive", "parcel-energy": "activating", "parcel-demolitions": "unanswerable",
            "parcel-factories": "failed"})

        self.assertEqual(result.returncode, 0, result.stderr)
        for stage in (own_dead, nearby_dead, failed_old, unloaded_old):
            self.assertFalse(stage.exists(), stage)
            self.assertTrue((stage.parent / "run.log").exists(), "a dead run keeps its log")
        for stage in (own_current, nearby_newest, energy_old, energy_running, unanswered_old):
            self.assertTrue((stage / "stage.duckdb").exists(), stage)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 5, result.stdout)
        for stage, lane, state, size in ((own_dead, "parcel-boundary-served", "own", 1000),
                                         (nearby_dead, "parcel-nearby", "stopped", 24_000),
                                         (failed_old, "parcel-factories", "stopped", 500),
                                         (unloaded_old, "parcel-sub-parcels", "stopped", 300)):
            line = next(line for line in lines if line.startswith(f"removed {stage} "))
            fields = dict(field.split("=", 1) for field in line.split()[2:])
            self.assertEqual((fields["lane"], fields["state"]), (lane, state))
            self.assertGreaterEqual(int(fields["bytes"]), size)
        self.assertEqual(lines[-1].split()[:2], ["stage", "orphans"])
        last = dict(field.split("=", 1) for field in lines[-1].split()[2:])
        self.assertEqual(last["removed"], "4")
        self.assertGreaterEqual(int(last["bytes"]), 25_800)

    def test_nothing_dead_removes_nothing(self):
        current = self.stage("parcel-boundary-served", "20261123T113000Z", 10)
        newest = self.stage("parcel-nearby", "20261121T162627Z", 10)
        result = self.run_script("parcel-boundary-served", "20261123T113000Z", {"parcel-nearby": "inactive"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "stage orphans removed=0 bytes=0\n")
        self.assertTrue((current / "stage.duckdb").exists())
        self.assertTrue((newest / "stage.duckdb").exists())

    def test_a_dead_runs_verdict_stage_goes_too(self):
        # 2026-11-25: three killed takeover verdicts of parcel-building-permits and one of the served
        # boundaries had left 33 GB under runs/<id>/verdict/, which the sweep never looked into.
        verdict = self.stage("parcel-building-permits", "20261123T153928Z", 2000, under="verdict")
        current_verdict = self.stage("parcel-building-permits", "20261125T160855Z", 10, under="verdict")
        result = self.run_script("parcel-building-permits", "20261125T160855Z", {})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(verdict.exists(), result.stdout)
        self.assertTrue(verdict.parent.exists(), "the run's verdict record stays")
        self.assertTrue((current_verdict / "stage.duckdb").exists())
        self.assertIn(f"removed {verdict} ", result.stdout)

    def test_a_stopped_lanes_every_run_is_dead(self):
        # 2026-11-25: a parcel-ownership run killed at its hour left a 14.6 GB stage; the unit's
        # ExecStopPost sweeps with --stopped, its newest run included. Another lane's newest stays.
        killed = self.stage("parcel-ownership", "20261125T165420Z", 3000)
        killed_verdict = self.stage("parcel-ownership", "20261125T165420Z", 300, under="verdict")
        other_newest = self.stage("parcel-nearby", "20261125T120000Z", 10)
        result = self.run_script("parcel-ownership", "--stopped", {"parcel-nearby": "inactive"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(killed.exists(), result.stdout)
        self.assertFalse(killed_verdict.exists(), result.stdout)
        self.assertTrue((other_newest / "stage.duckdb").exists())
        last = dict(field.split("=", 1) for field in result.stdout.splitlines()[-1].split()[2:])
        self.assertEqual(last["removed"], "2")

    def test_the_lane_unit_sweeps_its_runs_however_they_end(self):
        # The unit's ExecStopPost is `gold-versions-build.sh cleanup` of the release its run started
        # with (pinned-cleanup.sh, root ADR-0270 §3): the --stopped sweep of the lane its
        # RuntimeDirectory names.
        text = UNIT.read_text(encoding="utf-8")
        self.assertIn("ExecStopPost=/opt/foundation-platform/current/scripts/ops/pinned-cleanup.sh "
                      "gold-versions-build.sh\n", text)
        self.assertIn("RuntimeDirectory=%N\n", text)
        killed = self.stage("parcel-ownership", "20261125T165420Z", 3000)
        other_newest = self.stage("parcel-nearby", "20261125T120000Z", 10)
        self.states.write_text("parcel-nearby=inactive\n", encoding="utf-8")
        env = {"PATH": self.path, "STATES": str(self.states), "FOUNDATION_GOLD_VERSIONS_STATE_ROOT": str(self.root)}

        def cleanup(runtime):
            return subprocess.run(["bash", str(BUILD), "cleanup"], env={**env, **runtime},
                                  capture_output=True, text=True, timeout=60, check=False)

        result = cleanup({"RUNTIME_DIRECTORY": str(self.temp / "foundation-gold-versions@parcel-ownership")})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(killed.exists(), result.stdout)
        self.assertTrue((other_newest / "stage.duckdb").exists())
        self.assertEqual(cleanup({}).returncode, 64, "a cleanup outside the lane's unit names no lane")
        self.assertEqual(cleanup({"RUNTIME_DIRECTORY": str(self.temp / "foundation-gold-versions@no-such-lane")}).returncode,
                         64, "a lane the script does not build is refused")

    def test_the_lane_script_clears_them_before_its_publisher_runs(self):
        text = BUILD.read_text(encoding="utf-8")
        self.assertLess(text.index("duckdb-stage-orphans.sh"), text.index('"${PUBLISHER_BIN}" "${command}"'))
        self.assertIn('"${STATE_ROOT%/*}" "${lane}" "${work}"', text)


if __name__ == "__main__":
    unittest.main()
