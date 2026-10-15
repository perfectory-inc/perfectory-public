"""The supervised cut-over of pack-only lanes (scripts/ops/by-pnu-lane-cutover.sh, root ADR-0190, ADR-0193 §6).

It runs each lane's Gold, the parity with the bundle, bake, equality, latency, publish and
monitor-sample through the granted operator script, waits for each, stops at the first failure,
records every step, never starts beside Spark and never in the daily scheduled window. The fake
systemd-run of the operator's harness records each unit and fails the ones FAKE_FAIL names.
"""

import datetime
import json
import os
import pathlib
import shutil
import subprocess
import sys
import unittest

from test_by_pnu_pack_operator import CONTRACT, PLATFORM, OperatorHarness

sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
import by_pnu_kinds  # noqa: E402

LANE = "unit-price"
JOBS = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]


def at(hour, minute=0):
    return str(int(datetime.datetime(2026, 10, 13, hour, minute, tzinfo=datetime.timezone.utc).timestamp()))


# Clear of every daily scheduled Spark start with the longest step budget to spare.
NIGHT = at(2)


class LaneCutover(OperatorHarness):
    def setUp(self):
        super().setUp()
        self.env["FOUNDATION_CUTOVER_POLL_SECONDS"] = "0"

    def runner(self, *args, now=NIGHT, **env):
        """The release's runner, as the cut-over unit runs it (root, no sudo)."""
        result = subprocess.run(["bash", str(self.release / "scripts/ops/by-pnu-lane-cutover.sh"), *args],
                                env={**self.env, "FOUNDATION_CUTOVER_NOW": now, **env},
                                capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def gates_ready(self):
        # What bake, equality and latency leave behind for the steps after them (their units are fakes).
        work = self.work(LANE)
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        (work / "latency.json").write_text(json.dumps({"passed": True}))
        return work

    def mark_cut_over(self, *lanes):
        """Write each lane's monitor sample where monitor-sample would: its own file, or the kinds' one
        file a line each (root ADR-0193 §9)."""
        for lane in lanes:
            path = self.etc / f"{by_pnu_kinds.lane_fields(lane)['monitor_group']}.env"
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"FOUNDATION_PLATFORM_{lane.replace('-', '_').upper()}_BY_PNU_SERVING_MONITOR_SAMPLE_PATH=/x\n")

    def evidence(self):
        (path,) = (self.bake / "cutover").glob("*.jsonl")
        return [json.loads(line) for line in path.read_text().splitlines()]

    def test_the_operator_starts_one_root_unit_that_runs_the_release_runner(self):
        result, calls = self.run_operator(LANE, "cutover", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        self.assertIn("--unit=foundation-by-pnu-lane-cutover-g1", args)
        self.assertNotIn("--wait", args)
        self.assertFalse([prop for prop in props if prop.startswith("User=")], props)
        self.assertEqual(args[-3:], [str((self.release / "scripts/ops/by-pnu-lane-cutover.sh").resolve()), LANE, "1"])
        self.assertIn(f"StandardOutput=append:{self.bake}/cutover/logs/cutover-g1.log", props)
        # One cut-over at a time.
        self.log.unlink()
        result, calls = self.run_operator("lanes", "cutover", "1", active=["foundation-by-pnu-lane-cutover-g1"])
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertEqual(calls, [])

    def test_the_operator_takes_only_a_lane_or_lanes_a_generation_and_dry_run(self):
        for args in (("lanes", "bake", "1"), (LANE, "cutover"), (LANE, "cutover", "0"), (LANE, "cutover", "1;id"),
                     (LANE, "cutover", "1", "--force"), (LANE, "cutover", "1", "--dry-run", "x"),
                     ("parcel", "cutover", "1"), ("nope", "cutover", "1")):
            with self.subTest(args=args):
                result, calls = self.run_operator(*args)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(calls, [])

    def test_a_lane_gold_rebuild_is_its_own_table_and_waits_only_for_the_runner(self):
        table = by_pnu_kinds.lane_fields(LANE)["gold_table"]
        contract = json.loads((PLATFORM / "infra/lakehouse/contracts/gold-panel-rebuild.contract.json")
                              .read_text(encoding="utf-8"))
        unit = contract["tables"][table]["unit"]
        result, calls = self.run_operator(LANE, "gold-rebuild")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, _, _ = self.unit(calls)
        # Only the lane's table, only when an input moved: no `all`, no --unconditional.
        self.assertEqual(args[-2:], ["/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh", unit])
        self.assertIn("--no-block", args)
        self.log.unlink()
        result, calls = self.run_operator(LANE, "gold-rebuild", FOUNDATION_OPERATOR_WAIT="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, _, _ = self.unit(calls)
        self.assertIn("--wait", args)
        self.assertNotIn("--no-block", args)
        # A failed rebuild is the step's failure.
        self.log.unlink()
        result, _ = self.run_operator(LANE, "gold-rebuild", FOUNDATION_OPERATOR_WAIT="1", FAKE_FAIL="gold-panel-rebuild")
        self.assertNotEqual(result.returncode, 0)

    def test_a_cut_over_runs_every_step_in_order_waiting_for_each(self):
        work = self.gates_ready()
        result, calls = self.runner(LANE, "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        units = [next(arg[len("--unit="):] for arg in call["args"] if arg.startswith("--unit="))
                 for call in calls if call["tool"] == "systemd-run"]
        self.assertEqual(units, ["foundation-gold-panel-rebuild-unconditional", "foundation-unit-price-pack-bake-g1",
                                 "foundation-unit-price-pack-equality-g1", "foundation-unit-price-pack-latency-g1",
                                 "foundation-unit-price-pack-publish-g1"])
        for call in calls:
            if call["tool"] == "systemd-run":
                self.assertIn("--wait", call["args"])
        # The previews were asked first, the lane's and its latency baseline's.
        previews = [call["args"][0] for call in calls if call["tool"] == "curl"]
        self.assertEqual(len(previews), 2)
        self.assertLess(calls.index(next(c for c in calls if c["tool"] == "curl")),
                        calls.index(next(c for c in calls if c["tool"] == "systemd-run")))
        # The last step switched the lane's monitor on: from now on the lane counts as cut over.
        self.assertIn({"tool": "systemctl", "args": ["enable", "--now", f"foundation-by-pnu-serving-monitor@{LANE}.timer"]},
                      calls)
        self.assertIn(str(work / "equality.json"), (self.etc / f"{LANE}-serving-monitor.env").read_text())
        steps = self.evidence()
        self.assertEqual([step["step"] for step in steps],
                         ["preview", "gold", "parity", "bake", "equality", "latency", "publish", "monitor-sample"])
        self.assertTrue(all(isinstance(step["seconds"], int) and step["generation"] == 1 for step in steps))
        # A table with no declared parity is never reported as a passed parity.
        parity = next(step for step in steps if step["step"] == "parity")
        table = by_pnu_kinds.lane_fields(LANE)["gold_table"]
        rebuild = json.loads((PLATFORM / "infra/lakehouse/contracts/gold-panel-rebuild.contract.json")
                             .read_text(encoding="utf-8"))["tables"][table]
        declared = (rebuild.get("pins_from") or {}).get("parity_argument")
        self.assertEqual(parity["outcome"], "passed" if declared else "not-declared")
        self.assertEqual({step["outcome"] for step in steps if step["step"] != "parity"}, {"passed"})
        # `lanes` now finds nothing left to cut over and starts nothing.
        self.mark_cut_over(*[lane for lane in CONTRACT["by_pnu_lanes"] if lane != LANE])
        self.log.unlink()
        result, calls = self.runner("lanes", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("every lane is cut over already", result.stderr)
        self.assertFalse([call for call in calls if call["tool"] == "systemd-run"])

    def test_a_declared_parity_passes_only_on_a_head_a_full_rebuild_with_parity_committed(self):
        # An incremental commit skips the producer's parity with the bundle (ADR-0193 §6-2), so a
        # head it made was never checked on every PNU: the cut-over must not serve it. The head is
        # read from the rebuild's own run directories, so a head the scheduled kind rebuild made
        # earlier (this run's gold then says "nothing to do", 2026-10-13) passes on its evidence.
        contract_path = self.release / "infra/lakehouse/contracts/gold-panel-rebuild.contract.json"
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        table = by_pnu_kinds.lane_fields(LANE)["gold_table"]
        contract["tables"][table].setdefault("pins_from", {})["parity_argument"] = "--parity-panel-snapshot-id"
        unit = contract["tables"][table]["unit"]
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        self.gates_ready()
        state = self.root / "gold-state"
        parity = ["--lane", LANE, "--parity-panel-snapshot-id", "999990000000000001"]

        def run(name, mode=None, arguments=parity, disposition="iceberg_overwrite"):
            directory = state / unit / "runs" / name
            directory.mkdir(parents=True)
            (directory / "plan.json").write_text(json.dumps({"producer_arguments": arguments}))
            if mode is not None:  # a run that planned "nothing to do" writes no summary
                (directory / "summary.json").write_text(json.dumps(
                    {"write_disposition": disposition, "rebuild": {"mode": mode}, "row_count": 5}))

        full = ("20261012T230000Z", "full")
        for kind, runs, passes in (
            ("this run committed full", [full], True),
            ("an earlier full head, this run nothing to do", [full, ("20261013T010000Z", None)], True),
            ("a dry run after the full head", [full, ("20261013T010000Z", "full", parity, "validate_only")], True),
            ("an incremental head", [full, ("20261013T010000Z", "incremental")], False),
            ("a full head without the parity argument", [("20261012T230000Z", "full", ["--lane", LANE])], False),
            ("no run wrote the table", [("20261013T010000Z", None)], False),
        ):
            with self.subTest(kind=kind):
                shutil.rmtree(state, ignore_errors=True)
                for spec in runs:
                    run(*spec)
                for old in [self.log, *(self.bake / "cutover").glob("*.jsonl")] if (self.bake / "cutover").exists() else [self.log]:
                    if old.exists():
                        old.unlink()
                result, calls = self.runner(LANE, "1", FOUNDATION_GOLD_REBUILD_STATE_ROOT=str(state))
                steps = {step["step"]: step for step in self.evidence()}
                if passes:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(steps["parity"]["outcome"], "passed")
                    self.assertIn("full rebuild", steps["parity"]["reason"])
                else:
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertEqual(steps["parity"]["outcome"], "failed")
                    self.assertNotIn("bake", steps)
                    self.assertFalse([c for c in calls if c["tool"] == "systemd-run" and "pack-bake" in " ".join(c["args"])])
                (self.etc / f"{LANE}-serving-monitor.env").unlink(missing_ok=True)

    def test_gold_rebuild_of_one_table_is_any_unit_of_the_rebuild_contract(self):
        # The zoning kind's first full build is run and timed outside the scheduled job (ADR-0193 §7).
        contract = json.loads((PLATFORM / "infra/lakehouse/contracts/gold-panel-rebuild.contract.json")
                              .read_text(encoding="utf-8"))
        unit = list(contract["tables"].values())[0]["unit"]
        result, calls = self.run_operator("gold", "rebuild", unit)
        self.assertEqual(result.returncode, 0, result.stderr)
        args, _, _ = self.unit(calls)
        self.assertEqual(args[-2:], ["/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh", unit])
        self.log.unlink()
        # The measured run of an input listed under unmeasured_inputs: the same unit, nothing committed.
        result, calls = self.run_operator("gold", "rebuild", unit, "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, _, _ = self.unit(calls)
        self.assertEqual(args[-3:], ["/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh", unit, "--dry-run"])
        self.log.unlink()
        for args in (("gold", "rebuild"), ("gold", "rebuild", "all"), ("gold", "rebuild", unit, "x"),
                     ("gold", "rebuild", unit, "--dry-run", "x"), ("gold", "rebuild", "--dry-run"),
                     ("gold", "rebuild", "../x"), ("gold", "bake", "1")):
            with self.subTest(args=args):
                result, calls = self.run_operator(*args)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(calls, [])
        result, calls = self.run_operator("gold", "rebuild", unit, active=["foundation-by-pnu-serving-bake"])
        self.assertEqual(result.returncode, 75, result.stderr)

    def test_the_first_failing_step_stops_the_run_with_its_reason(self):
        self.gates_ready()
        result, calls = self.runner(LANE, "1", FAKE_FAIL="pack-equality")
        self.assertEqual(result.returncode, 1, result.stderr)
        units = [arg for call in calls if call["tool"] == "systemd-run" for arg in call["args"] if arg.startswith("--unit=")]
        self.assertEqual(units[-1], "--unit=foundation-unit-price-pack-equality-g1")
        self.assertFalse([call for call in calls if call["tool"] == "systemctl" and call["args"][0] == "enable"])
        last = self.evidence()[-1]
        self.assertEqual((last["step"], last["outcome"]), ("equality", "failed"))
        self.assertIn("equality 1 failed (exit 3)", last["reason"])
        self.assertIn("equality.log", last["reason"])

    def test_a_preview_that_does_not_answer_stops_the_run_before_any_spark(self):
        # 2026-10-13: the unit-price preview answered 521 until its Worker was deployed.
        result, calls = self.runner(LANE, "1", FAKE_CURL_CODE="521")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertFalse([call for call in calls if call["tool"] == "systemd-run"])
        self.assertIn("answers 521", self.evidence()[-1]["reason"])

    def test_spark_running_holds_the_gold_and_the_bake_back(self):
        for unit in ("foundation-map-edit-fold@admin.service", "foundation-gold-panel-rebuild.service",
                     "foundation-parcel-pack-bake-g3.service"):
            with self.subTest(unit=unit):
                if self.log.exists():
                    self.log.unlink()
                for old in (self.bake / "cutover").glob("*.jsonl") if (self.bake / "cutover").exists() else ():
                    old.unlink()
                result, calls = self.runner(LANE, "1", FAKE_ACTIVATING=unit, FOUNDATION_CUTOVER_SPARK_WAIT_MINUTES="0")
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertFalse([call for call in calls if call["tool"] == "systemd-run"])
                last = self.evidence()[-1]
                self.assertEqual(last["step"], "gold")
                self.assertIn(unit, last["reason"])

    def test_no_step_starts_in_or_runs_into_the_scheduled_window(self):
        # Every daily start of a spark-pool job is inside the window the runner keeps out of.
        daily = sorted({(int(job["schedule"].split()[1]), int(job["schedule"].split()[0])) for job in JOBS
                        if job.get("pool") == "spark" and job["schedule"].split()[1].isdigit()})
        self.assertTrue(daily)
        for hour, minute in daily:
            with self.subTest(start=f"{hour:02d}:{minute:02d}"):
                result, calls = self.runner(LANE, "1", "--dry-run", now=at(hour, minute))
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("inside the scheduled window", result.stderr)
        # Twenty minutes before the first one, a Gold of its budget would run into it: nothing starts.
        first_hour, first_minute = daily[0]
        early = at(first_hour, first_minute)
        early = str(int(early) - 20 * 60)
        if self.log.exists():
            self.log.unlink()
        result, calls = self.runner(LANE, "1", now=early)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertFalse([call for call in calls if call["tool"] == "systemd-run"])
        self.assertIn("would run into the scheduled window", self.evidence()[-1]["reason"])

    def test_a_dry_run_says_every_step_and_starts_nothing(self):
        result, calls = self.run_operator(LANE, "cutover", "1", "--dry-run", FOUNDATION_CUTOVER_NOW=NIGHT)
        self.assertEqual(result.returncode, 0, result.stderr)
        for step in ("preview", "gold", "parity", "bake", "equality", "latency", "publish", "monitor-sample"):
            self.assertIn(f"dry run {LANE} {step}:", result.stderr)
        self.assertFalse([call for call in calls if call["tool"] in ("systemd-run", "systemctl")])
        self.assertFalse((self.bake / "cutover").exists())
        self.assertFalse((self.etc / f"{LANE}-serving-monitor.env").exists())
        # A preview that does not answer is a step that could not start.
        result, _ = self.run_operator(LANE, "cutover", "1", "--dry-run", FOUNDATION_CUTOVER_NOW=NIGHT,
                                      FAKE_CURL_CODE="521")
        self.assertEqual(result.returncode, 1, result.stderr)
        # `lanes` is every lane of the contract, a cut-over one left out.
        result, _ = self.run_operator("lanes", "cutover", "1", "--dry-run", FOUNDATION_CUTOVER_NOW=NIGHT)
        self.assertEqual(result.returncode, 0, result.stderr)
        for lane in CONTRACT["by_pnu_lanes"]:
            self.assertIn(f"dry run {lane} gold:", result.stderr)
        self.mark_cut_over(LANE)
        result, _ = self.run_operator("lanes", "cutover", "1", "--dry-run", FOUNDATION_CUTOVER_NOW=NIGHT)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(f"dry run {LANE} gold:", result.stderr)
        # The kinds share one monitor file: one kind's line there leaves the others to cut over.
        kinds = list(by_pnu_kinds.kinds())
        self.mark_cut_over(kinds[0])
        result, _ = self.run_operator("lanes", "cutover", "1", "--dry-run", FOUNDATION_CUTOVER_NOW=NIGHT)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(f"dry run {kinds[0]} gold:", result.stderr)
        self.assertIn(f"dry run {kinds[1]} gold:", result.stderr)


if __name__ == "__main__":
    unittest.main()
