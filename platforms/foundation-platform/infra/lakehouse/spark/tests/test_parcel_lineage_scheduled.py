"""The scheduled lineage derivation (root ADR-0113 §1): which pairs and groups it derives, which it
skips as held, and that it starts none after its deadline. The Spark half reads and writes only
through `parcel_lineage_to_silver`, whose rules its own tests pin."""

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import parcel_lineage as pl  # noqa: E402
import parcel_lineage_scheduled as job  # noqa: E402
import parcel_lineage_to_silver as producer  # noqa: E402

CONTRACT = {"snapshot_id_prefix": "vworldkr__parcel-", "served_edition": "209906",
            "editions": {"209909": {}, "209906": {}, "209912": {}}}


class Plan(unittest.TestCase):
    def test_each_edition_pairs_with_the_next_one_oldest_first(self):
        self.assertEqual(job.edition_pairs(CONTRACT), [("209906", "209909"), ("209909", "209912")])
        self.assertEqual(job.edition_pairs({**CONTRACT, "editions": {"209906": {}}}), [])

    def test_a_shared_sido_is_its_own_group_and_the_renumbered_ones_go_together(self):
        self.assertEqual(job.sido_groups({"98", "97", "96"}, {"98", "95"}), [("98", "98"), ("96,97", "95")])
        self.assertEqual(job.sido_groups({"98"}, {"98"}), [("98", "98")])

    def test_a_sido_held_by_one_edition_without_a_counterpart_is_refused(self):
        for before, after in (({"98", "97"}, {"98"}), ({"98"}, {"98", "97"}), (set(), {"98"}), ({"98"}, set())):
            with self.subTest(before=before, after=after), self.assertRaises(ValueError):
                job.sido_groups(before, after)


class RunGroups(unittest.TestCase):
    GROUPS = [("98", "98"), ("97", "97"), ("96", "96")]

    def test_held_groups_are_skipped_and_the_rest_derived(self):
        derived = []
        outcome = job.run_groups(self.GROUPS, lambda f, t: f == "97",
                                 lambda f, t: derived.append(f) or {"rows": 1}, deadline_seconds=60)
        self.assertEqual(derived, ["98", "96"])
        self.assertEqual([g["from_sido"] for g in outcome["held"]], ["97"])
        self.assertEqual(outcome["derived"][0], {"from_sido": "98", "to_sido": "98", "rows": 1})
        self.assertEqual(outcome["deferred"], [])

    def test_no_group_starts_after_the_deadline(self):
        ticks = iter([0, 0, 100, 100])  # start, before group 1, before group 2, before group 3
        derived = []
        outcome = job.run_groups(self.GROUPS, lambda f, t: False, lambda f, t: derived.append(f) or {},
                                 deadline_seconds=50, clock=lambda: next(ticks))
        self.assertEqual(derived, ["98"])
        self.assertEqual([g["from_sido"] for g in outcome["deferred"]], ["97", "96"])

    def test_a_held_group_is_reported_held_even_past_the_deadline(self):
        outcome = job.run_groups([("98", "98")], lambda f, t: True, lambda f, t: {}, deadline_seconds=0)
        self.assertEqual((len(outcome["held"]), len(outcome["deferred"])), (1, 0))


class ProducerArguments(unittest.TestCase):
    def test_a_group_becomes_a_hand_run_the_producer_itself_validates(self):
        args = job.parse_args(["--deadline-minutes", "60", "--work-dir", "w", "--allow-non-smoke-write"])
        with mock.patch.object(producer.editions, "load", return_value=CONTRACT):
            parsed = producer.parse_args(job.producer_argv(args, CONTRACT, ("209906", "209909"), ("96,97", "95"),
                                                           Path("pins.json")))
        self.assertEqual((parsed.from_snapshot_id, parsed.to_snapshot_id),
                         ("vworldkr__parcel-209906", "vworldkr__parcel-209909"))
        self.assertEqual((parsed.from_date, parsed.to_date), ("2099-06-01", "2099-09-01"))
        self.assertEqual((parsed.from_sido, parsed.to_sido), ("96,97", "95"))
        self.assertEqual((parsed.iceberg_table, parsed.allow_non_smoke_write), ("parcel_lineage", True))

    def test_without_the_write_flag_the_producer_refuses_the_real_table(self):
        args = job.parse_args(["--deadline-minutes", "60", "--work-dir", "w"])
        self.assertNotIn("--allow-non-smoke-write",
                         job.producer_argv(args, CONTRACT, ("209906", "209909"), ("98", "98"), Path("p")))

    def test_a_deadline_must_be_positive(self):
        with self.assertRaises(ValueError):
            job.parse_args(["--deadline-minutes", "0", "--work-dir", "w"])

    def test_the_held_query_names_the_pair_the_rules_and_the_group(self):
        sql = job.held_sql("t", "a", "b", "96,97")
        for part in ("from_snapshot_id = 'a'", "to_snapshot_id = 'b'", f"rules_version = '{pl.RULES_VERSION}'",
                     "IN ('96', '97')"):
            self.assertIn(part, sql)


if __name__ == "__main__":
    unittest.main()
