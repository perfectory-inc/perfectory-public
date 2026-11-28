"""A collector that moved to AWS is held there, not here (root ADR-0616 §7, ADR-0614; job_specs.on_aws).

A lane of the AWS contract (collect.lanes) whose job is off in the host's list moved: the AWS location runs it,
reads its freshness and judges its outcomes. The host stops holding it, or it would page for a collector that is
running elsewhere. A job that is off and is no lane stays held here: ADR-0614's switched-off collector of a source
that drops what ended (2026-10-21..11-05 the court auction collector was off for 16 nights and judged by nothing)
must still page. The one switch is the job's `enabled`: the AWS lane of a handoff source runs exactly while the
host job is off (aws/collect_lane.py lanes_here), so the two never run at once and never both stop.
"""

import json
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster" / "aws"))
import collect_lane  # noqa: E402
import job_specs  # noqa: E402

CONTRACT = {"collect": {"lanes": {"moved": {"handover": "handoff"}, "overlapping": {"handover": "overlap"}}}}


def jobs(**enabled):
    return {"jobs": [{"id": job_id, "enabled": on} for job_id, on in enabled.items()]}


class OnAws(unittest.TestCase):
    def test_a_lane_whose_job_is_off_here_moved_and_nothing_else_did(self):
        listed = jobs(moved=False, overlapping=True, court_auction_collect=False, other=True)
        self.assertEqual(job_specs.on_aws(listed, CONTRACT), {"moved"})
        held = [job["id"] for job in job_specs.held_on_host(listed, CONTRACT)["jobs"]]
        self.assertEqual(held, ["overlapping", "court_auction_collect", "other"],
                         "a switched-off job that is no lane is still held here (root ADR-0614)")

    def test_turning_the_job_on_again_brings_it_back_here(self):
        self.assertEqual(job_specs.on_aws(jobs(moved=True, overlapping=True), CONTRACT), set())

    def test_the_real_lists_agree_the_host_and_aws_never_both_run_a_handoff_lane(self):
        listed = collect_lane.jobs()
        hosted = collect_lane.on_host(listed)
        here = collect_lane.lanes_here(job_list=listed)
        for job_id, lane in collect_lane.contract()["collect"]["lanes"].items():
            if lane.get("paused"):
                # Root ADR-0626 §8 N2: a paused lane runs nowhere, and its host job stays off.
                self.assertFalse(job_id in hosted or job_id in here, f"{job_id} is paused and runs nowhere")
            elif lane["handover"] == collect_lane.HANDOFF:
                self.assertNotEqual(job_id in hosted, job_id in here, f"{job_id} runs in exactly one place")
                self.assertEqual(job_id in job_specs.on_aws(listed), job_id in here, job_id)

    def test_onbid_and_the_sale_notice_component_moved_and_rt_molit_is_back_on_the_host(self):
        # rt.molit.go.kr drops TCP connections from AWS addresses (2026-11-27, root ADR-0616 section 7). The
        # industrial land sale notice moved whole with its Lakekeeper switch (W109e): three collectors and its load.
        moved = job_specs.on_aws(collect_lane.jobs())
        self.assertEqual(moved, {"onbid_public_auction_collect", "industrial_land_sale_notice_factoryon_collect",
                                 "industrial_land_sale_notice_ilis_collect", "industrial_land_sale_notice_kicox_collect",
                                 "industrial_land_sale_notice_silver"})


class OnbidOnADailyBeat(unittest.TestCase):
    def test_one_daily_run_has_the_host_s_day_and_never_more_than_the_daily_limits(self):
        onbid = json.loads((PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json").read_text(encoding="utf-8"))
        day = onbid["daily_call_limit_per_api"]
        for api, hourly in (("item_bid_result", 1000), ("item_detail", 600), ("item_bid_info", 300),
                            ("announcement_detail", 200)):
            self.assertEqual(onbid[api]["per_run_limit"], min(24 * hourly, day[api]), api)
        self.assertEqual(onbid["media"]["per_run_limit"], min(24 * 3000, onbid["media"]["daily_download_limit"]))
        job = collect_lane.job("onbid_public_auction_collect")
        self.assertGreaterEqual(job["expected_outcome"]["landed_within_hours"], 24, "a daily read meets its bound")


if __name__ == "__main__":
    unittest.main()
