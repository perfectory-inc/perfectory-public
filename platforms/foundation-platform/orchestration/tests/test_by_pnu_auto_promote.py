"""A by-PNU lane whose Gold is ready publishes itself (scripts/ops/by-pnu-auto-promote.sh, root ADR-0435).

The 2026-11-05..07 replay: parcel-ownership's Gold, bake and equality were done at 11-05 01:54Z, its latency
gate met one R2 `service error`, and nothing published it for more than 53 hours, through a mistyped sudo
password, a 45-minute Spark wait and a clock window. The controller starts the first lane not cut over,
retries a failed one after a doubling backoff, resumes the lane's newest bake and never starts beside a
running cut-over. Stand-ins answer for the runner, the operator script and systemctl.
"""

import datetime
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

from test_job_units import FAKE_SYSTEMCTL

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/by-pnu-auto-promote.sh"

# `lanes --pending` prints the PENDING file.
FAKE_RUNNER = r'''#!/usr/bin/env bash
here="$(cd "$(dirname "$0")" && pwd)"
if [[ "$1 $2" == "lanes --pending" ]]; then cat "${here}/PENDING"; exit 0; fi
exit 64
'''
FAKE_OPERATOR = r'''#!/usr/bin/env bash
here="$(cd "$(dirname "$0")" && pwd)"
printf '%s\n' "$*" >>"${here}/operator.log"
'''


def at(day, hour, minute=0):
    return int(datetime.datetime(2026, 11, day, hour, minute, tzinfo=datetime.timezone.utc).timestamp())


# 11-07 22:12Z, a quarter-hour tick.
NIGHT = at(7, 22, 12)


class AutoPromotion(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="auto-promote-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        self.release = self.root / "release"
        ops = self.release / "scripts/ops"
        ops.mkdir(parents=True)
        (self.release / "scripts/deploy").mkdir()
        (self.release / "orchestration").mkdir()
        shutil.copy(PLATFORM / "scripts/deploy/job_units.py", self.release / "scripts/deploy/job_units.py")
        shutil.copy(PLATFORM / "orchestration/jobs.v1.json", self.release / "orchestration/jobs.v1.json")
        for name, body in (("by-pnu-lane-cutover.sh", FAKE_RUNNER), ("by-pnu-pack-operator.sh", FAKE_OPERATOR)):
            (ops / name).write_text(body)
            (ops / name).chmod(0o755)
        self.ops = ops
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "systemctl").write_text(FAKE_SYSTEMCTL)
        (bin_dir / "systemctl").chmod(0o755)
        self.bake = self.root / "by-pnu-bake"
        (self.bake / "cutover").mkdir(parents=True)
        self.marks = self.root / "auto-promote"
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FAKE_LOG": str(self.root / "systemctl.log"),
                    "FOUNDATION_AUTO_PROMOTE_RELEASE_ROOT": str(self.release),
                    "FOUNDATION_AUTO_PROMOTE_MARKS": str(self.marks),
                    "FOUNDATION_OPERATOR_BAKE_ROOT": str(self.bake)}
        self.pending("parcel-ownership", "parcel-regulation-zones")

    def pending(self, *lanes):
        (self.ops / "PENDING").write_text("".join(f"{lane}\n" for lane in lanes))

    def run_controller(self, now=NIGHT, units=()):
        result = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, timeout=60,
                                env={**self.env, "FOUNDATION_AUTO_PROMOTE_NOW": str(now),
                                     "FAKE_UNITS": ",".join(f"{unit}=activating" for unit in units)})
        log = self.ops / "operator.log"
        started = log.read_text().splitlines() if log.exists() else []
        if log.exists():
            log.unlink()
        return result, started

    def test_the_first_lane_not_cut_over_starts_with_no_person(self):
        result, started = self.run_controller()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(started, ["parcel-ownership cutover 1 --auto"])
        (attempt,) = [json.loads(line) for line in (self.marks / "parcel-ownership.jsonl").read_text().splitlines()]
        self.assertEqual(attempt, {"at": NIGHT, "lane": "parcel-ownership", "generation": 1, "release": "release"})

    def test_nothing_starts_beside_a_running_cut_over_or_when_every_lane_is_served(self):
        result, started = self.run_controller(units=["foundation-by-pnu-lane-cutover-g1.service"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(started, [])
        self.assertIn("a lane cut-over runs", result.stdout)
        self.pending()
        result, started = self.run_controller()
        self.assertEqual((result.returncode, started), (0, []))
        self.assertIn("every lane is cut over", result.stdout)

    def test_no_first_publish_starts_while_a_served_lanes_refresh_is_owed(self):
        # Root ADR-0439: 2026-11-07 13:12Z a cut-over's Gold step took the one rebuild lock while the
        # parcel lane's owed panel rebuild waited behind it.
        metrics = self.root / "asset-reconcile.prom"
        self.env["FOUNDATION_AUTO_PROMOTE_ASSET_METRICS"] = str(metrics)
        metrics.write_text('foundation_asset_owed{asset="iceberg://gold.parcel_panel",job="gold_panel_rebuild"} 1\n'
                           'foundation_asset_owed{asset="iceberg://gold.unit_price",job="gold_unit_price_rebuild"} 1\n')
        os.utime(metrics, (NIGHT, NIGHT))
        result, started = self.run_controller()
        self.assertEqual((result.returncode, started), (0, []), result.stderr)
        self.assertIn("a served lane's refresh is owed (iceberg://gold.parcel_panel)", result.stdout)
        # A default job owed (the unit price Gold) or a reconciler silent for half an hour holds nothing.
        metrics.write_text('foundation_asset_owed{asset="iceberg://gold.unit_price",job="gold_unit_price_rebuild"} 1\n')
        os.utime(metrics, (NIGHT, NIGHT))
        self.assertEqual(self.run_controller()[1], ["parcel-ownership cutover 1 --auto"])
        metrics.write_text('foundation_asset_owed{asset="iceberg://gold.parcel_panel",job="gold_panel_rebuild"} 1\n')
        os.utime(metrics, (NIGHT - 1801, NIGHT - 1801))
        self.assertEqual(len(self.run_controller(now=NIGHT + 3600 * 25)[1]), 1, "a silent reconciler holds nothing")

    def test_a_lane_that_failed_is_tried_again_after_a_doubling_backoff_and_the_next_lane_goes_first(self):
        self.assertEqual(self.run_controller()[1], ["parcel-ownership cutover 1 --auto"])
        # A quarter hour later the ownership run failed (11-05 02:42Z: R2 `service error` in latency):
        # it waits an hour, and the next lane not cut over starts meanwhile.
        self.assertEqual(self.run_controller(NIGHT + 900)[1], ["parcel-regulation-zones cutover 1 --auto"])
        self.assertEqual(self.run_controller(NIGHT + 1800)[1], [], "both wait out their backoff")
        self.assertEqual(self.run_controller(NIGHT + 3600)[1], ["parcel-ownership cutover 1 --auto"])
        # Its second failure doubles the wait: two hours after its second start, not one.
        self.assertEqual(self.run_controller(NIGHT + 3600 + 3600 + 900)[1], ["parcel-regulation-zones cutover 1 --auto"])
        self.assertEqual(self.run_controller(NIGHT + 3600 + 3600 + 1800)[1], [])
        self.assertEqual(self.run_controller(NIGHT + 3600 + 7200)[1], ["parcel-ownership cutover 1 --auto"])

    def test_a_lane_never_tried_goes_before_one_that_failed(self):
        # 2026-11-07 08:58Z: parcel-regulation-zones' Gold crashed after 76 minutes; its retry was next,
        # ahead of three lanes never tried, one cut-over at a time.
        self.pending("parcel-ownership", "parcel-regulation-zones", "parcel-sub-parcels")
        self.assertEqual(self.run_controller()[1], ["parcel-ownership cutover 1 --auto"])
        self.assertEqual(self.run_controller(NIGHT + 900)[1], ["parcel-regulation-zones cutover 1 --auto"])
        self.assertEqual(self.run_controller(NIGHT + 7200)[1], ["parcel-sub-parcels cutover 1 --auto"],
                         "both failed lanes are past their backoff, the untried one goes first")
        self.assertEqual(self.run_controller(NIGHT + 7200 + 900)[1], ["parcel-ownership cutover 1 --auto"],
                         "then contract order among lanes tried as often")

    def test_attempts_by_an_earlier_release_hold_no_backoff(self):
        # 2026-11-08 16:12Z-17:00Z: a release whose transient gold unit systemd-run refused failed every
        # lane's gold step in 1 s, three times each; the release that fixed it would have waited 4 hours.
        self.pending("parcel-sub-parcels")
        self.marks.mkdir(parents=True)
        earlier = [{"at": NIGHT - 900 * n, "lane": "parcel-sub-parcels", "generation": 1, "release": "broken"}
                   for n in (3, 2)] + [{"at": NIGHT - 900, "lane": "parcel-sub-parcels", "generation": 1}]
        (self.marks / "parcel-sub-parcels.jsonl").write_text("".join(json.dumps(m) + "\n" for m in earlier))
        self.assertEqual(self.run_controller()[1], ["parcel-sub-parcels cutover 1 --auto"])
        # The attempt by this release counts: the next waits its hour.
        self.assertEqual(self.run_controller(NIGHT + 900)[1], [])
        self.assertEqual(self.run_controller(NIGHT + 3600)[1], ["parcel-sub-parcels cutover 1 --auto"])

    def test_after_a_deploy_the_lane_tried_least_still_goes_first(self):
        # 2026-11-08 23:12Z: a deploy reset every wait, and parcel-regulation-zones (five attempts, first
        # in the contract) restarted ahead of parcel-sub-parcels (three).
        self.pending("parcel-regulation-zones", "parcel-sub-parcels")
        self.marks.mkdir(parents=True)
        for lane, count in (("parcel-regulation-zones", 5), ("parcel-sub-parcels", 3)):
            earlier = [{"at": NIGHT - 3600 * n, "lane": lane, "generation": 1, "release": "earlier"} for n in range(count)]
            (self.marks / f"{lane}.jsonl").write_text("".join(json.dumps(m) + "\n" for m in earlier))
        self.assertEqual(self.run_controller()[1], ["parcel-sub-parcels cutover 1 --auto"])

    def test_a_gold_step_stopped_by_its_time_bound_is_tried_again_after_an_hour(self):
        # 2026-11-07: parcel-regulation-zones' first national build is longer than the rebuild's 160
        # minutes; it resumes from its staged shards (root ADR-0440), so a doubled wait only delays it.
        self.pending("parcel-regulation-zones")
        marks = [{"at": NIGHT - 6 * 3600, "lane": "parcel-regulation-zones", "generation": 1, "release": "release"},
                 {"at": NIGHT, "lane": "parcel-regulation-zones", "generation": 1, "release": "release"}]
        self.marks.mkdir(parents=True)
        (self.marks / "parcel-regulation-zones.jsonl").write_text("".join(json.dumps(m) + "\n" for m in marks))

        def gold_failed(seconds):
            step = {"lane": "parcel-regulation-zones", "step": "gold", "outcome": "failed", "seconds": seconds,
                    "reason": "", "generation": 1}
            (self.bake / "cutover/20261107T102724Z.jsonl").write_text(json.dumps(step) + "\n")

        gold_failed(160 * 60 + 5)
        self.assertEqual(self.run_controller(NIGHT + 3600)[1], ["parcel-regulation-zones cutover 1 --auto"])
        # A gold step that failed before its bound (08:58Z: Spark stopped itself at 76 minutes) doubles it.
        (self.marks / "parcel-regulation-zones.jsonl").write_text("".join(json.dumps(m) + "\n" for m in marks))
        gold_failed(4553)
        self.assertEqual(self.run_controller(NIGHT + 3600)[1], [])
        self.assertEqual(self.run_controller(NIGHT + 7200)[1], ["parcel-regulation-zones cutover 1 --auto"])

    def test_a_lane_whose_last_attempt_waited_is_asked_again_without_a_backoff(self):
        # Root ADR-0471: a readiness step (history, or the Gold's inputs) that ends `waiting` claimed
        # nothing; a doubled wait would hold parcel-nearby hours after the one load its seven Silver
        # tables wait for (2026-11-09).
        self.pending("parcel-nearby")
        marks = [{"at": NIGHT - 3600, "lane": "parcel-nearby", "generation": 1, "release": "release"},
                 {"at": NIGHT, "lane": "parcel-nearby", "generation": 1, "release": "release"}]
        self.marks.mkdir(parents=True)
        (self.marks / "parcel-nearby.jsonl").write_text("".join(json.dumps(m) + "\n" for m in marks))
        run = datetime.datetime.fromtimestamp(NIGHT + 5, datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        steps = [{"lane": "parcel-nearby", "step": "history", "outcome": "not-declared", "seconds": 0, "reason": "",
                  "generation": 1},
                 {"lane": "parcel-nearby", "step": "inputs", "outcome": "waiting", "seconds": 1, "reason": "",
                  "generation": 1}]
        (self.bake / f"cutover/{run}.jsonl").write_text("".join(json.dumps(s) + "\n" for s in steps))
        self.assertEqual(self.run_controller(NIGHT + 900)[1], ["parcel-nearby cutover 1 --auto"])
        # A run older than the last attempt is not that attempt's: its failure still doubles the wait.
        earlier = datetime.datetime.fromtimestamp(NIGHT - 3595, datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        (self.bake / f"cutover/{run}.jsonl").rename(self.bake / f"cutover/{earlier}.jsonl")
        (self.marks / "parcel-nearby.jsonl").write_text("".join(json.dumps(m) + "\n" for m in marks))
        self.assertEqual(self.run_controller(NIGHT + 900)[1], [])
        self.assertEqual(self.run_controller(NIGHT + 7200)[1], ["parcel-nearby cutover 1 --auto"])

    def test_the_lanes_newest_bake_is_resumed_and_a_failed_bake_moves_to_the_next_generation(self):
        # 11-05: generation 1 baked and passed equality; latency failed. It is resumed, not baked anew.
        self.pending("parcel-ownership")
        (self.bake / "parcel-ownership-pack-g1/summaries").mkdir(parents=True)
        steps = [{"lane": "parcel-ownership", "step": step, "outcome": outcome, "seconds": 0, "reason": "",
                  "generation": 1}
                 for step, outcome in (("gold", "passed"), ("bake", "passed"), ("equality", "passed"),
                                       ("latency", "failed"))]
        (self.bake / "cutover/20261105T005745Z.jsonl").write_text("".join(json.dumps(s) + "\n" for s in steps))
        self.assertEqual(self.run_controller()[1], ["parcel-ownership cutover 1 --auto"])
        # A bake that failed may hold a pin Gold no longer keeps: the next attempt bakes generation 2.
        failed = [{"lane": "parcel-ownership", "step": "bake", "outcome": "failed", "seconds": 0, "reason": "",
                   "generation": 1}]
        (self.bake / "cutover/20261108T000000Z.jsonl").write_text(json.dumps(failed[0]) + "\n")
        self.assertEqual(self.run_controller(NIGHT + 3600)[1], ["parcel-ownership cutover 2 --auto"])

    def test_a_generation_whose_equality_failed_is_baked_anew(self):
        # 2026-11-07 07:38Z, the first automatic run: the gold step committed a new snapshot, the bake
        # resumed generation 1 pinned to 11-05's (39,396,906 rows), and equality compared 42,053,586.
        (self.bake / "parcel-ownership-pack-g1/summaries").mkdir(parents=True)
        steps = [{"lane": "parcel-ownership", "step": step, "outcome": outcome, "seconds": 0, "reason": "",
                  "generation": 1}
                 for step, outcome in (("gold", "passed"), ("bake", "passed"), ("equality", "failed"))]
        (self.bake / "cutover/20261107T071226Z.jsonl").write_text("".join(json.dumps(s) + "\n" for s in steps))
        self.assertEqual(self.run_controller()[1], ["parcel-ownership cutover 2 --auto"])

    def test_a_lane_starts_at_any_hour(self):
        # 2026-11-06 03:46Z the clock window refused the cut-over; capacity, not the time, decides
        # (root ADR-0435): the run waits for the spark pool by its claim.
        for hour in (4, 12, 18, 21):
            with self.subTest(hour=hour):
                shutil.rmtree(self.marks, ignore_errors=True)
                result, started = self.run_controller(at(8, hour))
                self.assertEqual((result.returncode, started), (0, ["parcel-ownership cutover 1 --auto"]))

if __name__ == "__main__":
    unittest.main()
