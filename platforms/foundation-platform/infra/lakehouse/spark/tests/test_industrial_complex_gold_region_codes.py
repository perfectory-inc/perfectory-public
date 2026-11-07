"""A complex's 시도·시군구 follow the 법정동 code changes, only where a change reaches (root ADR-0426).

The incident: catalog.industrial_complex kept 시도 29 (19 complexes) and 46 (116) four months after
both were abolished into 12 on 2026-07-01, because the codes were read once from the source and
nothing re-placed them. Synthetic codes: 97 and 98 merge into 96.
"""

from __future__ import annotations

import sys
import unittest
from unittest import mock

# The PySpark stub the schema-evolution test installs, so the job imports without PySpark.
from test_industrial_complex_gold_schema_evolution import job


def pair(old: str, new: str, level: str, kind: str = "pair") -> dict[str, str]:
    return {"kind": kind, "old_code": old, "new_code": new, "level": level, "effective_date": "20990701",
            "source": "derived:code-go-kr:date+name:20990701"}


CHANGES = [pair("9700000000", "9600000000", "sido"), pair("9800000000", "9600000000", "sido"),
           pair("9717000000", "9630000000", "sigungu"), pair("9889000000", "9685000000", "sigungu")]
NAMES = [("9600000000", "Southsynth City", "존재"), ("9500000000", "Midsynth Province", "존재"),
         ("9700000000", "Oldsynth City", "폐지")]
REGION = job.region_codes(CHANGES, NAMES)
SOURCE = {"a": ("97", "97170", None), "b": ("98", "98890", None), "c": ("95", "95110", None)}


class RegionReplacements(unittest.TestCase):
    def test_the_first_follow_reaches_every_complex_and_replaces_only_stale_ones(self) -> None:
        gold = {"a": ("97", "97170", None), "b": ("96", "96850", "Southsynth"), "c": ("95", "95110", "Midsynth")}
        self.assertEqual(job.region_replacements(gold, SOURCE, None, REGION),
                         [("a", "96", "96300", "Southsynth")])

    def test_only_the_complexes_under_a_changed_prefix_are_reached(self) -> None:
        stale = {"a": ("97", "97170", None), "b": ("98", "98890", None), "c": ("95", "95110", None)}
        # Only the 97 pairs changed since the last follow: 98 and 95 are not looked at, stale or not.
        self.assertEqual(job.region_replacements(stale, SOURCE, {"97", "97170"}, REGION),
                         [("a", "96", "96300", "Southsynth")])
        self.assertEqual(job.region_replacements(stale, SOURCE, set(), REGION), [])

    def test_a_held_code_a_supersession_withdrew_is_reached_through_the_gold_code(self) -> None:
        # Gold holds 96300 from a pair official evidence withdrew; the pair in force says 96310.
        changes = [*CHANGES, pair("9717000000", "9631000000", "sigungu"),
                   pair("9717000000", "9630000000", "sigungu", kind="supersession")]
        region = job.region_codes(changes, NAMES)
        gold = {"a": ("96", "96300", "Southsynth")}
        self.assertEqual(job.region_replacements(gold, SOURCE, {"96300"}, region),
                         [("a", "96", "96310", "Southsynth")])

    def test_a_gold_complex_without_a_current_source_row_is_left_alone(self) -> None:
        self.assertEqual(job.region_replacements({"z": ("97", "97170", None)}, SOURCE, None, REGION), [])


class ChangedPrefixes(unittest.TestCase):
    def test_no_earlier_read_reaches_everything_and_an_equal_read_nothing(self) -> None:
        self.assertIsNone(job.changed_prefixes(None, CHANGES))
        self.assertEqual(job.changed_prefixes(job.followed_rows(CHANGES), CHANGES), set())

    def test_a_new_pair_names_the_prefix_of_both_its_codes(self) -> None:
        before = job.followed_rows(CHANGES)
        after = [*CHANGES, pair("9511000000", "9512000000", "sigungu")]
        self.assertEqual(job.changed_prefixes(before, after), {"95110", "95120"})


if __name__ == "__main__":
    unittest.main()


class FollowArguments(unittest.TestCase):
    def test_the_stewardship_follow_arguments_are_accepted(self) -> None:
        # lineage-stewardship-cycle.sh's call, which ValueError'd on the overwrite default 2026-11-07 (W21).
        argv = ["industrial_complex_silver_to_gold.py", "--follow-region-codes", "--input-mode", "iceberg",
                "--write-mode", "iceberg", "--followed-code-changes-output", "/w/followed.json",
                "--summary-output", "/w/summary.json"]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(job, "assert_catalog_env"):
            job.validate_args(job.parse_args())
