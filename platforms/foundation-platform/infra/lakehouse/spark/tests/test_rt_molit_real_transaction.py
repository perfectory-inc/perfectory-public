"""실거래가 Silver 적재(root ADR-0348, ADR-0349)에서 Spark 에 남은 것을 합성 자료로 본다.

적재는 발행기의 DuckDB 로 갔고(root ADR-0559) 그 시험은 `rt_molit_silver_stage` 에 있다. 여기는 실거래 Gold 의 Spark
작업이 아직 읽는 주소 읽기(`dong_index`)와, 한 실행의 객체 수가 유닛의 시간에 드는지를 본다. 지역은 합성 99 시도다.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import rt_molit_real_transaction as job  # noqa: E402


class LinksPlaces(unittest.TestCase):
    def test_full_addresses_resolve_to_one_code_or_none(self):
        dongs = job.dong_index([
            ("9999900000", "합성도 합성시", "존재"),
            ("9999933021", "합성도  합성시 합성면   가상리", "존재"),
            ("9999933022", "합성도 합성시 합성면 겹친리", "존재"),
            ("9999933023", "합성도 합성시 합성면 겹친리", "존재"),
            ("9999933024", "합성도 합성시 합성면 옛리", "폐지"),
            ("9999933025", "합성도 합성시 합성면 바뀐리", "폐지"),
            ("9999933026", "합성도 합성시 합성면 바뀐리", "존재"),
            ("99999330", "짧은 코드", "존재"),
        ])
        self.assertEqual(dongs, {
            "합성도 합성시 합성면 가상리": "9999933021",   # whitespace made one space
            "합성도 합성시 합성면 옛리": "9999933024",     # an abolished code still names its 법정동
            "합성도 합성시 합성면 바뀐리": "9999933026",   # a current code wins over an abolished one
        })


class FitsItsUnit(unittest.TestCase):
    """ADR-0300 H: one run's objects fit the unit's time limit."""

    def test_the_per_run_bound_fits_the_unit(self):
        load = job.load_source_contract()["silver_load"]
        unit = (SPARK_DIR.parents[1] / "systemd" / "foundation-rt-molit-silver-load.service").read_text(encoding="utf-8")
        limit = int(re.search(r"^TimeoutStartSec=(\d+)m$", unit, re.M).group(1)) * 60
        measured = load["measured_object_seconds"]
        # The objects a run takes, at p90, after the start, fit the budget at which it stops starting more.
        self.assertLessEqual(load["run_start_seconds"] + load["max_objects"] * measured["p90"],
                             load["run_time_budget_seconds"])
        # The object in flight when the budget passes still finishes inside the unit's limit.
        self.assertLess(load["run_time_budget_seconds"] + measured["max"] + 120, limit)


if __name__ == "__main__":
    unittest.main()
