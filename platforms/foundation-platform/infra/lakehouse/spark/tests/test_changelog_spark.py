"""The changelog on a Spark runtime (root ADR-0191): the Z-set a load writes takes the old release to
the new one, composes over skipped releases, and the closure check refuses a lost row.

Skipped where pyspark is missing; the CI spark-runtime lane runs it. Rows are synthetic.
"""
import importlib.util
import sys
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import changelog  # noqa: E402

LINEAGE = ("source_snapshot_id",)
KEYS = ("pnu",)
SCHEMA = "pnu string, area double, source_snapshot_id string"


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def content(frame):
    """A frame's rows as a multiset of content, lineage and fingerprint dropped."""
    return Counter((r["pnu"], r["area"]) for r in frame.collect())


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class ChangeSetSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("changelog")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def release(self, rows, label):
        frame = self.spark.createDataFrame([(*row, label) for row in rows], SCHEMA)
        return changelog.with_fingerprint(frame, LINEAGE)

    def apply(self, old, changes):
        """`old` plus a Z-set, expanded back to rows."""
        state = Counter(content(old))
        for row in changes.collect():
            state[(row["pnu"], row["area"])] += row[changelog.WEIGHT]
        self.assertTrue(all(n >= 0 for n in state.values()), state)
        return +state

    def test_the_changes_take_the_old_release_to_the_new_one(self):
        old = self.release([("A", 1.0), ("B", 2.0), ("B", 2.0), ("C", 3.0), (None, 4.0)], "r1")
        # A restamped only, B lost a duplicate, C updated, D new, the NULL-key row unchanged.
        new = self.release([("A", 1.0), ("B", 2.0), ("C", 3.5), ("D", 5.0), (None, 4.0)], "r2")
        changes = changelog.change_set(old, new, KEYS)
        weights = Counter({(r["pnu"], r["area"]): r[changelog.WEIGHT] for r in changes.collect()})
        self.assertEqual(weights, Counter({("B", 2.0): -1, ("C", 3.0): -1, ("C", 3.5): 1, ("D", 5.0): 1}))
        self.assertEqual(self.apply(old, changes), content(new))
        self.assertIsNone(changelog.closure_verdict(
            changelog.totals(old), changelog.totals(new), changelog.totals(changes, weighted=True)))

    def test_skipped_releases_compose_into_the_direct_difference(self):
        r1 = self.release([("A", 1.0), ("B", 2.0)], "r1")
        r2 = self.release([("A", 1.5), ("B", 2.0), ("C", 3.0)], "r2")
        r3 = self.release([("A", 1.0), ("C", 3.0)], "r3")
        steps = changelog.stamped(changelog.change_set(r1, r2, KEYS), 1, 2).unionByName(
            changelog.stamped(changelog.change_set(r2, r3, KEYS), 2, 3))
        composed = changelog.net(steps, KEYS)
        direct = changelog.change_set(r1, r3, KEYS)
        as_weights = lambda f: Counter({(r["pnu"], r["area"]): r[changelog.WEIGHT] for r in f.collect()})
        self.assertEqual(as_weights(composed), as_weights(direct))
        # A Counter forgets zero weights: the row count sees a pair that summed to 0 and was kept.
        self.assertEqual(composed.count(), 2)
        self.assertEqual(as_weights(direct), Counter({("B", 2.0): -1, ("C", 3.0): 1}))

    def test_a_composed_pair_carries_its_latest_image_whole(self):
        # A enters in r2, leaves in r3, enters again in r4: the one surviving row is r4's image.
        r1, r2, r3, r4 = (self.release(rows, label) for rows, label in
                          (([], "r1"), ([("A", 1.0)], "r2"), ([], "r3"), ([("A", 1.0)], "r4")))
        steps = changelog.stamped(changelog.change_set(r1, r2, KEYS), 1, 2).unionByName(
            changelog.stamped(changelog.change_set(r2, r3, KEYS), 2, 3)).unionByName(
            changelog.stamped(changelog.change_set(r3, r4, KEYS), 3, 4))
        [row] = changelog.net(steps, KEYS).collect()
        self.assertEqual((row["pnu"], row[changelog.WEIGHT], row["source_snapshot_id"]), ("A", 1, "r4"))

    def test_the_fingerprint_does_not_depend_on_column_order(self):
        frame = self.spark.createDataFrame([("A", 1.0, "r1")], SCHEMA)
        reordered = frame.select(*reversed(frame.columns))
        fingerprint = lambda f: changelog.with_fingerprint(f, LINEAGE).first()[changelog.FINGERPRINT]
        self.assertEqual(fingerprint(frame), fingerprint(reordered))

    def test_a_lost_change_row_fails_the_closure(self):
        from pyspark.sql import functions as F

        old = self.release([("A", 1.0)], "r1")
        new = self.release([("A", 1.0), ("B", 2.0)], "r2")
        # Planted violation: the change set without its only row.
        lost = changelog.change_set(old, new, KEYS).where(F.col("pnu") != "B")
        self.assertIsNotNone(changelog.closure_verdict(
            changelog.totals(old), changelog.totals(new), changelog.totals(lost, weighted=True)))

    def test_a_restamp_alone_changes_nothing(self):
        rows = [("A", 1.0), ("B", None)]
        self.assertEqual(changelog.change_set(self.release(rows, "r1"), self.release(rows, "r2"), KEYS).count(), 0)


if __name__ == "__main__":
    unittest.main()
