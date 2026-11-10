"""An approved by-PNU cut-over is its attended start, used once, and reports its progress (root ADR-0433).

2026-11-06: the owner typed `ssh -tt ai-server "sudo by-pnu-pack-operator.sh parcel-ownership cutover 1
--requested-at ... --reason ..."` at 00:37:35Z after a failed sudo password, and again at 03:46:38Z, when
the gold step refused to run into the 04:30 scheduled window (a clock window ADR-0435 removed). A lane's
first publish now needs no approval (the auto-promotion starts it); the console's approval
(switches/cutover.d/<id>, written by foundation-api) now stands in for that request: the run-now
dispatcher starts `cutover <generation> --approved <id>` as root, and the runner reports each step to
the approval's progress, which the console shows.
"""

import datetime
import json
import os
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_by_pnu_lane_cutover as cutover  # noqa: E402
from test_by_pnu_pack_operator import OperatorHarness  # noqa: E402

LANE, JOBS = cutover.LANE, cutover.JOBS

APPROVAL = "0b0c0d0e-1111-4222-8333-444455556666"


def stamp(minutes):
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)).isoformat()


class ApprovedCutover(OperatorHarness):
    # The cut-over tests' helpers, not their tests (a subclass would run them twice).
    runner = cutover.LaneCutover.runner
    gates_ready = cutover.LaneCutover.gates_ready

    def setUp(self):
        super().setUp()
        self.env["FOUNDATION_CUTOVER_POLL_SECONDS"] = "0"
        # The verify step's edge probe, a stand-in (test_by_pnu_lane_cutover.FAKE_PROBE).
        probe = self.root / "probe"
        probe.mkdir()
        (probe / "by-pnu-edge-probe.py").write_text(cutover.FAKE_PROBE)
        self.env["FOUNDATION_CUTOVER_EDGE_PROBE"] = str(probe / "by-pnu-edge-probe.py")
        self.env["FOUNDATION_CUTOVER_VERIFY_PAUSE_SECONDS"] = "0"
        self.switches = self.root / "switches"
        self.used = self.root / "run-now/cutover-used"
        self.progress_dir = self.root / "releases"
        self.env.update({"FOUNDATION_OPERATOR_SWITCHES": str(self.switches),
                         "FOUNDATION_OPERATOR_APPROVALS_USED": str(self.used),
                         "FOUNDATION_RELEASE_PROGRESS_DIR": str(self.progress_dir)})

    def approve(self, lanes=LANE, generation=1, expires_minutes=48 * 60, job="by_pnu_lane_cutover", approval=APPROVAL):
        """What foundation-api's switch board writes for an approve_cutover (job_switch_board.rs)."""
        requests = self.switches / "cutover.d"
        requests.mkdir(parents=True, exist_ok=True)
        (requests / approval).write_text(
            f"request={approval}\nactor=staff-owner\nrequested_at={stamp(0)}\nreason=the first publish of the lane\n"
            f"job={job}\nexpires_at={stamp(expires_minutes)}\nlanes={lanes}\ngeneration={generation}\n")

    def approved(self, *args, **env):
        return self.run_operator(LANE, "cutover", "1", "--approved", APPROVAL, *args, flags=False, attended=False, **env)

    def progress(self, approval=APPROVAL):
        path = self.progress_dir / f"{approval}.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_an_approval_starts_the_runner_once_without_a_terminal_or_sudo(self):
        self.approve()
        result, calls = self.approved()
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, _ = self.unit(calls)
        self.assertIn("--unit=foundation-by-pnu-lane-cutover-g1", args)
        self.assertEqual(env, {"FOUNDATION_CUTOVER_APPROVAL": APPROVAL})
        self.assertEqual(args[-2:], [LANE, "1"], "the runner is not handed the approval flag")
        # Who and why are the approval's, in the journal like every operator start.
        (record,) = self.journal
        self.assertIn(f"start {LANE} cutover 1 who=staff-owner approved={APPROVAL} reason=the first publish", " ".join(record))
        (started,) = self.progress()
        self.assertEqual((started["state"], started["unit"], started["lanes"], started["generation"]),
                         ("started", "foundation-by-pnu-lane-cutover-g1", LANE, 1))
        # Used once: the same approval started again (a timer that fired twice) starts nothing.
        self.log.unlink()
        result, calls = self.approved()
        self.assertEqual(result.returncode, 77, result.stderr)
        self.assertIn("was used already", result.stderr)
        self.assertEqual(calls, [])
        self.assertEqual(self.progress()[-1]["state"], "refused")

    def test_a_granted_sudo_run_cannot_use_an_approval(self):
        # The owner's sudo grant runs this script too; an approval is the dispatcher's alone.
        self.approve()
        result, calls = self.approved(SUDO_USER="perfectory")
        self.assertEqual(result.returncode, 77, result.stderr)
        self.assertIn("not through sudo", result.stderr)
        self.assertEqual(calls, [])
        self.assertFalse(self.used.exists(), "a refused start used the approval")

    def test_an_approval_of_other_lanes_another_generation_or_expired_starts_nothing(self):
        for case, approve in (("missing", None),
                              ("other lanes", dict(lanes="lanes")),
                              ("other generation", dict(generation=2)),
                              ("not a cut-over", dict(job="shape_ledger_check")),
                              ("expired", dict(expires_minutes=-1))):
            with self.subTest(case=case):
                for path in (self.switches / "cutover.d").glob("*") if (self.switches / "cutover.d").exists() else ():
                    path.unlink()
                if approve is not None:
                    self.approve(**approve)
                self.log.unlink(missing_ok=True)
                result, calls = self.approved()
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(calls, [])
                self.assertEqual(self.progress()[-1]["state"], "refused")
        for args in (("--approved",), ("--approved", "not-a-uuid")):
            with self.subTest(args=args):
                result, calls = self.run_operator(LANE, "cutover", "1", *args, flags=False, attended=False)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(calls, [])

    def test_the_runner_reports_every_step_and_its_end_to_the_approval(self):
        self.gates_ready()
        result, _ = self.runner(LANE, "1", FOUNDATION_CUTOVER_APPROVAL=APPROVAL)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = self.progress()
        self.assertEqual([line["step"] for line in lines if line["state"] == "step"],
                         ["history", "inputs", "preview", "gold", "parity", "bake", "equality", "latency", "publish",
                          "monitor-sample", "verify"])
        self.assertEqual((lines[-1]["state"], lines[-1]["outcome"]), ("finished", "passed"))
        self.assertEqual(len({line["run"] for line in lines}), 1)

    def test_the_eleven_o_six_late_start_runs_every_step(self):
        # 03:46:38Z: the gold step's 60 minutes "would have run into the 04:30 window" and the run was
        # refused. No time of day refuses a step now (root ADR-0435): the run publishes.
        self.gates_ready()
        late = str(int(datetime.datetime(2026, 11, 6, 3, 46, 38, tzinfo=datetime.timezone.utc).timestamp()))
        result, _ = self.runner(LANE, "1", now=late, FOUNDATION_CUTOVER_APPROVAL=APPROVAL)
        self.assertEqual(result.returncode, 0, result.stderr)
        end = self.progress()[-1]
        self.assertEqual((end["state"], end["outcome"]), ("finished", "passed"))

if __name__ == "__main__":
    unittest.main()
