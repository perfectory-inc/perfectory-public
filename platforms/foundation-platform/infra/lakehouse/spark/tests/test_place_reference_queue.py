"""The one steward queue for every parcel source's unresolved rows (root ADR-0483 §5).

Synthetic codes (prefix 99999). Pins: the sources are every Silver carrying a `pnu` and its reason; an
item is one (source, reason, 법정동) with its count and at most a few sorted sample keys; a reason the
source withheld waits for data; an item's identity and etag follow its content; a run reads only the
sources whose snapshot moved since the queue recorded it, and the MERGE touches that source's items
only.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import place_reference_queue as queue  # noqa: E402
import place_reference_queue_to_gold as job  # noqa: E402

DONG = "9999910100"


class QueueTest(unittest.TestCase):
    def test_the_sources_are_the_silvers_the_resolver_placed(self):
        from platform_contracts import load_lakehouse_artifact

        found = queue.sources(load_lakehouse_artifact()["contracts"])
        for table in ("silver.court_auction_item", "silver.court_auction_sale_result", "silver.real_transaction",
                      "silver.registered_factory"):
            self.assertIn(table, found)
        self.assertNotIn("silver.parcel_boundaries", found)
        self.assertTrue(all(t.startswith("silver.") for t in found))

    def test_an_item_is_a_source_reason_and_dong_with_its_count(self):
        rows = [{"reason": "code_superseded_parcel_absent", "legal_dong_code": DONG, "rows": 57,
                 "sample_row_keys": [f"k{n}" for n in range(9, 0, -1)]},
                {"reason": "jibun_masked", "legal_dong_code": None, "rows": 3, "sample_row_keys": ["a"]}]
        steward, waiting = queue.items("silver.x", rows)
        self.assertEqual(steward.subject_code, f"silver.x|code_superseded_parcel_absent|{DONG}")
        self.assertEqual(steward.status, queue.STEWARD)
        body = json.loads(steward.candidates_json)
        self.assertEqual((body["rows"], body["sample_row_keys"]), (57, ["k1", "k2", "k3", "k4", "k5"]))
        self.assertEqual((waiting.subject_code, waiting.status), ("silver.x|jibun_masked|-", queue.AWAITING))
        again = queue.items("silver.x", rows[:1])[0]
        self.assertEqual((again.item_id, again.evidence_etag), (steward.item_id, steward.evidence_etag))
        moved = queue.items("silver.x", [{**rows[0], "rows": 58}])[0]
        self.assertEqual(moved.item_id, steward.item_id)
        self.assertNotEqual(moved.evidence_etag, steward.evidence_etag)
        # The fingerprint the steward database recomputes on load (stewardship_domain::evidence_etag).
        self.assertEqual(steward.evidence_etag, queue.q.evidence_etag(steward.status, steward.candidates_json))
        self.assertEqual(queue.summary({"silver.x": [steward, waiting]})["silver.x"],
                         {"items": {"steward": 1, "awaiting_data": 1}, "rows": {"steward": 57, "awaiting_data": 3}})

    def test_the_newest_recorded_snapshot_of_each_source_is_the_comparison_point(self):
        newest_first = [{job.SNAPSHOT_PREFIX + "silver.a": "3"}, {"other": "x"},
                        {job.SNAPSHOT_PREFIX + "silver.a": "1", job.SNAPSHOT_PREFIX + "silver.b": "2"}]
        self.assertEqual(job.recorded_snapshots(newest_first), {"silver.a": "3", "silver.b": "2"})

    def test_the_merge_deletes_only_the_source_s_own_items(self):
        sql = job.merge_sql("t", ["unit", "subject_code", "evidence_etag"], "silver.a")
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND t.unit = 'place_reference' AND t.subject_code LIKE 'silver.a|%' "
                      "THEN DELETE", sql)
        self.assertIn("WHEN MATCHED AND t.evidence_etag <> v.evidence_etag THEN UPDATE", sql)


if __name__ == "__main__":
    unittest.main()
