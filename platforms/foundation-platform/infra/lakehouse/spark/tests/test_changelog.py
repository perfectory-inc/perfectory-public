"""The changelog's decisions without Spark (root ADR-0191): the chain a consumer reads, the closure
check, the content columns. The Spark proof is test_changelog_spark.py. Snapshot ids are synthetic.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import changelog  # noqa: E402
from changelog import Totals  # noqa: E402


def snap(snapshot_id, parent, operation="append"):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "operation": operation}


# main: 1 -> (branch 2, 3 published) -> 4 compaction -> (branch 5 published)
MAIN = [snap(1, None), snap(2, 1, "overwrite"), snap(3, 2), snap(4, 3, "replace"), snap(5, 4, "overwrite")]


class ChainTest(unittest.TestCase):
    def test_recorded_steps_and_compactions_take_main_from_the_pin_to_the_head(self):
        self.assertEqual(changelog.chain(MAIN, [(1, 3), (4, 5)], 1, 5), [("1", "3"), ("4", "5")])
        self.assertEqual(changelog.chain(MAIN, [(4, 5)], 3, 5), [("4", "5")])

    def test_the_pin_is_the_head_reads_nothing(self):
        self.assertEqual(changelog.chain(MAIN, [], 5, 5), [])

    def test_a_load_without_a_changelog_falls_back(self):
        self.assertIsNone(changelog.chain(MAIN, [(1, 3)], 1, 5))

    def test_a_pin_off_the_lineage_falls_back(self):
        self.assertIsNone(changelog.chain(MAIN, [(1, 3), (4, 5)], 99, 5))

    def test_a_step_whose_publish_failed_is_never_read(self):
        # Step (3, 7) recorded, then its branch was never published: 7 is not on main.
        self.assertEqual(changelog.chain(MAIN, [(1, 3), (3, 7), (4, 5)], 1, 5), [("1", "3"), ("4", "5")])

    def test_two_steps_into_one_snapshot_are_refused(self):
        with self.assertRaisesRegex(ValueError, "two changelog steps"):
            changelog.chain(MAIN, [(1, 3), (2, 3)], 1, 3)

    def test_a_cycle_falls_back_instead_of_looping(self):
        looped = [snap(1, 2, "replace"), snap(2, 1, "replace")]
        self.assertIsNone(changelog.chain(looped, [], 9, 1))


class ChangeRecordTest(unittest.TestCase):
    """The release compare's commit is the change record (root ADR-0311): steps come from summaries."""

    def marked(self, snapshot_id, parent, operation="append"):
        return {**snap(snapshot_id, parent, operation),
                "summary": {changelog.CHANGE_RECORD_KEY: changelog.CHANGE_RECORD_RELEASE_COMPARE}}

    def test_a_compare_commit_is_a_step_from_its_parent(self):
        main = [snap(1, None), self.marked(2, 1), snap(3, 2, "replace"), self.marked(4, 3)]
        steps = changelog.recorded_steps(main)
        self.assertEqual(steps, [("1", "2"), ("3", "4")])
        self.assertEqual(changelog.chain(main, steps, 1, 4), [("1", "2"), ("3", "4")])

    def test_an_unmarked_load_between_falls_back(self):
        # The first load, or a lane that still overwrites: no record, so the consumer diffs snapshots.
        main = [snap(1, None), self.marked(2, 1), snap(3, 2, "overwrite"), self.marked(4, 3)]
        self.assertIsNone(changelog.chain(main, changelog.recorded_steps(main), 1, 4))

    def test_only_an_append_can_be_a_record(self):
        main = [snap(1, None), self.marked(2, 1, "overwrite")]
        self.assertEqual(changelog.recorded_steps(main), [])

    def test_the_options_land_in_the_summary_under_the_keys_the_reader_asks(self):
        options = changelog.change_record_options(4, 1)
        summary = {k.removeprefix("snapshot-property."): v for k, v in options.items()}
        self.assertEqual(summary[changelog.CHANGE_RECORD_KEY], changelog.CHANGE_RECORD_RELEASE_COMPARE)
        self.assertEqual((summary[changelog.CHANGED_KEY_ROWS_KEY], summary[changelog.REMOVED_KEYS_KEY]), ("4", "1"))
        self.assertEqual(changelog.recorded_steps([{**snap(2, 1), "summary": summary}]), [("1", "2")])


class ClosureTest(unittest.TestCase):
    def test_changes_that_take_old_to_new_close(self):
        self.assertIsNone(changelog.closure_verdict(Totals(3, 30), Totals(4, 47), Totals(1, 17)))

    def test_a_lost_change_row_is_refused(self):
        # Planted violation: the +1 row with fingerprint 17 was not written.
        self.assertRegex(changelog.closure_verdict(Totals(3, 30), Totals(4, 47), Totals(0, 0)),
                         "row count")

    def test_a_wrong_image_with_the_right_count_is_refused(self):
        self.assertRegex(changelog.closure_verdict(Totals(3, 30), Totals(3, 31), Totals(0, 0)),
                         "fingerprint sum")


class ColumnsTest(unittest.TestCase):
    def test_content_is_the_schema_minus_lineage_and_changelog_columns(self):
        columns = ["pnu", "area", "source_snapshot_id", changelog.FINGERPRINT, *changelog.CHANGE_COLUMNS]
        self.assertEqual(changelog.content_columns(columns, ["source_snapshot_id"]), ["pnu", "area"])

    def test_a_table_of_lineage_only_has_nothing_to_fingerprint(self):
        with self.assertRaisesRegex(ValueError, "at least one content column"):
            changelog.content_columns(["source_snapshot_id"], ["source_snapshot_id"])

    def test_the_changes_table_sits_beside_its_table(self):
        self.assertEqual(changelog.changes_table("silver.example"), "silver.example__changes")


class ReleasedVersionsTest(unittest.TestCase):
    """ADR-0318: a sealed compare records its release's version, read along one lineage."""

    def test_only_the_seals_on_the_lineage_name_versions(self):
        import changelog

        snapshots = [
            {"snapshot_id": 1, "parent_id": None, "summary": {}},
            {"snapshot_id": 2, "parent_id": 1, "summary": {changelog.RELEASE_VERSION_KEY: "209909"}},
            {"snapshot_id": 3, "parent_id": 2, "summary": None},
            {"snapshot_id": 4, "parent_id": 2, "summary": {changelog.RELEASE_VERSION_KEY: "209910"}},
        ]
        lineage = changelog.snapshot_lineage(snapshots, 3)
        self.assertEqual(lineage, {1, 2, 3})
        self.assertEqual(changelog.released_versions(snapshots, lineage), {"209909"})
        self.assertEqual(changelog.snapshot_lineage(snapshots, None), set())


if __name__ == "__main__":
    unittest.main()
