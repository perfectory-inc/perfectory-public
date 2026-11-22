"""Each unit's runs measured against its limits from systemd's journal (root ADR-0574).

What real incident does failing this prevent? 2026-11-13..18 onbid-silver-load's Spark died at its
cap 32 times, parcel-number-change was killed at its 256M cap 6 times and court-auction-collect ran
at 98% of its 2 GiB, and nothing said a unit was near its limit before it failed: a wrong run
boundary (a unit killed at its cap writes no resource line) hid every one of those kills.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
PLATFORM = ORCHESTRATION.parent
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import unit_limits  # noqa: E402

MIB = 1 << 20
UNIT = "foundation-example.service"


def event(at, kind, unit=UNIT, message="", **fields):
    line = {"__REALTIME_TIMESTAMP": str(int(at * 1e6)), "MESSAGE_ID": kind, "UNIT": unit, "MESSAGE": message}
    line.update(fields)
    return json.dumps(line)


def ok_run(start, seconds, peak_mib, unit=UNIT):
    return [event(start, unit_limits.STARTING, unit),
            event(start + seconds, unit_limits.DEACTIVATED, unit),
            event(start + seconds, unit_limits.RESOURCES, unit, MEMORY_PEAK=str(peak_mib * MIB))]


def oom_run(start, seconds, unit=UNIT):
    """A unit killed at its cap: systemd writes the OOM and failure lines and no resource line."""
    return [event(start, unit_limits.STARTING, unit),
            event(start + seconds, unit_limits.OUT_OF_MEMORY, unit),
            event(start + seconds, unit_limits.FAILED, unit, "Failed with result 'oom-kill'.", UNIT_RESULT="oom-kill")]


DECLARED = {UNIT: {"Id": UNIT, "MemoryMax": str(512 * MIB), "MemoryHigh": str(460 * MIB),
                   "TimeoutStartUSec": "40min", "RuntimeMaxUSec": "infinity", "Type": "oneshot"}}


class RunsAreReadFromSystemdsLines(unittest.TestCase):
    def test_a_run_killed_at_its_cap_counts_though_it_wrote_no_resource_line(self):
        lines = ok_run(0, 60, 300) + oom_run(100, 30) + ok_run(200, 60, 310)
        line = unit_limits.row(UNIT, unit_limits.attribute_scopes(unit_limits.runs(lines))[UNIT], DECLARED[UNIT])
        self.assertEqual((line["runs"], line["oom"], line["state"]), (3, 1, "failing"))

    def test_the_newest_run_killed_at_its_cap_counts_before_the_unit_starts_again(self):
        lines = ok_run(0, 60, 300) + oom_run(100, 30) + [event(500, unit_limits.STARTING)]
        line = unit_limits.row(UNIT, unit_limits.runs(lines[:-1])[UNIT], DECLARED[UNIT])
        running = unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT])
        self.assertEqual((line["runs"], line["oom"], running["runs"]), (2, 1, 2))

    def test_a_timeout_is_a_failure_at_the_time_limit(self):
        lines = [event(0, unit_limits.STARTING), event(2400, unit_limits.FAILED, UNIT_RESULT="timeout"),
                 event(2400, unit_limits.RESOURCES, MEMORY_PEAK=str(200 * MIB))]
        line = unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT])
        self.assertEqual((line["timeout"], line["time_share"], line["state"]), (1, 1.0, "failing"))

    def test_a_container_held_by_one_run_counts_against_that_unit(self):
        scope = "docker-" + "a" * 64 + ".scope"
        lines = (ok_run(0, 600, 300)[:1]
                 + [event(10, unit_limits.STARTED, scope), event(500, unit_limits.OUT_OF_MEMORY, scope),
                    event(500, unit_limits.RESOURCES, scope, MEMORY_PEAK=str(2560 * MIB))]
                 + ok_run(0, 600, 300)[1:])
        line = unit_limits.row(UNIT, unit_limits.attribute_scopes(unit_limits.runs(lines))[UNIT], DECLARED[UNIT])
        self.assertEqual((line["container_peak_max"], line["oom"]), (2560 * MIB, 1))

    def test_a_container_two_runs_hold_is_left_out(self):
        other = "foundation-other.service"
        scope = "docker-" + "b" * 64 + ".scope"
        lines = (ok_run(0, 600, 300)[:1] + ok_run(0, 600, 300, other)[:1]
                 + [event(10, unit_limits.STARTED, scope), event(500, unit_limits.OUT_OF_MEMORY, scope),
                    event(500, unit_limits.RESOURCES, scope, MEMORY_PEAK=str(MIB))]
                 + ok_run(0, 600, 300)[1:] + ok_run(0, 600, 300, other)[1:])
        units = unit_limits.attribute_scopes(unit_limits.runs(lines))
        self.assertEqual([run.containers for run in units[UNIT] + units[other]], [[], []])


# One charge batch per CPU on a 20-CPU host with 4 KiB pages: how far past MemoryHigh reclaim lets a peak go.
SLACK = unit_limits.reclaim_slack(cpus=20, page=4096)


class NearIsMeasured(unittest.TestCase):
    def test_three_newest_runs_held_past_memory_high_are_a_streak_an_older_low_one_is_not(self):
        lines = ok_run(0, 60, 100) + ok_run(100, 60, 470) + ok_run(200, 60, 480) + ok_run(300, 60, 490)
        line = unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT], SLACK)
        self.assertEqual((line["near_memory_runs"], line["state"]), (3, "near"))

    def test_a_low_newest_run_ends_the_streak(self):
        lines = ok_run(0, 60, 470) + ok_run(100, 60, 480) + ok_run(200, 60, 100)
        line = unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT], SLACK)
        self.assertEqual(line["near_memory_runs"], 0)

    def test_a_peak_the_kernel_was_reclaiming_at_memory_high_is_not_near(self):
        # 2026-11-22: 20 units' peaks sat 0.5-3.4 MiB above MemoryHigh on every run: page cache given back.
        lines = ok_run(0, 60, 461) + ok_run(100, 60, 462) + ok_run(200, 60, 463)
        line = unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT], SLACK)
        self.assertEqual((line["near_memory_runs"], line["state"]), (0, "fits"))
        self.assertGreater(line["memory_share"], 0.9)
        self.assertIsNone(line["held_memory_share"])
        self.assertEqual(SLACK, 5 * MIB)

    def test_the_textfile_names_each_limit_with_its_streak_and_newest_share(self):
        by_unit = unit_limits.runs(ok_run(0, 1200, 470) + ok_run(2000, 1200, 480) + ok_run(4000, 2100, 490))
        rows = [unit_limits.row(UNIT, by_unit[UNIT], DECLARED[UNIT], SLACK)]
        text = unit_limits.textfile(rows, by_unit, 5000)
        self.assertIn(f'foundation_unit_runs_near_limit{{unit="{UNIT}",limit="memory"}} 3\n', text)
        self.assertIn(f'foundation_unit_runs_near_limit{{unit="{UNIT}",limit="time"}} 1\n', text)
        self.assertIn(f'foundation_unit_limit_share{{unit="{UNIT}",limit="memory"}} 0.9570\n', text)
        self.assertIn("foundation_unit_limits_measured_seconds 5000\n", text)


@unittest.skipUnless(os.name == "posix", "a fake journalctl is a shell script")
class TheJournalIsStreamed(unittest.TestCase):
    def test_lines_arrive_one_at_a_time_and_a_failed_journalctl_fails(self):
        # 2026-11-22: holding 324k journal lines took the script to 780 MB, past its unit's 512M cap.
        with tempfile.TemporaryDirectory() as directory:
            fake = pathlib.Path(directory) / "journalctl"
            fake.write_text("#!/bin/sh\nprintf '%s\\n' one two three\n", encoding="utf-8")
            fake.chmod(0o755)
            lines = unit_limits.journal("-1d", str(fake))
            self.assertNotIsInstance(lines, list)
            self.assertEqual([line.strip() for line in lines], ["one", "two", "three"])
            broken = pathlib.Path(directory) / "broken"
            broken.write_text("#!/bin/sh\nexit 3\n", encoding="utf-8")
            broken.chmod(0o755)
            with self.assertRaises(unit_limits.subprocess.CalledProcessError):
                list(unit_limits.journal("-1d", str(broken)))


class LimitsAreReadAsSystemdPrintsThem(unittest.TestCase):
    def test_durations_and_infinity(self):
        self.assertEqual(unit_limits.limit_seconds("1h 55min"), 6900)
        self.assertEqual(unit_limits.limit_seconds("2min 30s"), 150)
        self.assertIsNone(unit_limits.limit_seconds("infinity"))
        self.assertIsNone(unit_limits.limit_bytes("infinity"))

    def test_a_service_unit_is_held_to_its_runtime_maximum(self):
        self.assertEqual(unit_limits.time_limit({"Type": "simple", "TimeoutStartUSec": "1min30s",
                                                 "RuntimeMaxUSec": "10min"}), 600)

    def test_show_output_parses_by_unit(self):
        shown = unit_limits.declared(f"Id={UNIT}\nMemoryMax=536870912\n\nId=foundation-b.service\nMemoryMax=infinity\n")
        self.assertEqual(sorted(shown), ["foundation-b.service", UNIT])


class ContainersAreMeasuredToo(unittest.TestCase):
    def test_a_standing_container_killed_at_its_cap_is_failing(self):
        """2026-11-14..18: foundation-api, 512m, killed 69 times (root ADR-0573)."""
        identifier = "c" * 64
        kernel = [f"oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg=/system.slice/docker-{identifier}.scope,task=foundation-api"] * 2
        rows = unit_limits.container_rows({identifier: "foundation-api-1"}, {identifier: 511 * MIB},
                                          {identifier: 512 * MIB}, unit_limits.container_kills(kernel))
        self.assertEqual((rows[0]["oom"], rows[0]["state"]), (2, "failing"))


class ItsOwnTimerWritesTheTextfile(unittest.TestCase):
    """The 14-day journal read takes seconds, so it runs in its own unit, not in the asset
    reconciler, which already timed out at its three minutes seven times in the window."""

    def test_the_unit_writes_where_node_exporter_reads_and_the_release_enables_its_timer(self):
        unit = (PLATFORM / "infra/systemd/foundation-unit-limits.service").read_text(encoding="utf-8")
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/ops/unit_limits.py --write /var/lib/foundation-jobs/metrics\n", unit)
        self.assertIn("ReadWritePaths=/var/lib/foundation-jobs/metrics\n", unit)
        self.assertIn("systemctl enable --now foundation-unit-limits.timer\n", release)
        self.assertNotIn("unit_limits", (PLATFORM / "scripts/ops/asset_reconcile.py").read_text(encoding="utf-8"))

    def test_write_replaces_the_textfile_whole(self):
        lines = ok_run(0, 60, 420)
        with tempfile.TemporaryDirectory() as directory:
            original = unit_limits.measure
            unit_limits.measure = lambda now, since: unit_limits.textfile(
                [unit_limits.row(UNIT, unit_limits.runs(lines)[UNIT], DECLARED[UNIT])], unit_limits.runs(lines), now)
            try:
                unit_limits.write(directory, 5000)
            finally:
                unit_limits.measure = original
            self.assertEqual(os.listdir(directory), [unit_limits.METRICS_NAME])
            text = (pathlib.Path(directory) / unit_limits.METRICS_NAME).read_text(encoding="utf-8")
            self.assertTrue(text.endswith("foundation_unit_limits_measured_seconds 5000\n"))


if __name__ == "__main__":
    unittest.main()
