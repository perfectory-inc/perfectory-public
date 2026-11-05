"""A vintage joined twice moves into the versions table as the batch the panel served (root ADR-0317 §5.2).

On 2026-11-04 `silver.unit_official_price` held vintage 202610 as two batches (two joins from reloaded hub
snapshots, 188,736,512 rows each) and 202609 as four. By `vintage` alone they tie (root ADR-0376 §1). The
building panel served the batch committed last (`newest_batch` with `batch_commit_order`, root ADR-0185),
so that is the one the migration writes; in the versions table a key's versions are then ordered by
`[vintage, ingested_at_utc]`, and a later join of the same vintage outranks the earlier by its commit
instant (root ADR-0328). Skipped where pyspark is missing; the CI spark-runtime lane runs it.
"""
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


EARLY = "price:1|exclusive:2|vintage:202610"
LATE = "price:3|exclusive:4|vintage:202610"
SEPTEMBER = "price:5|exclusive:6|vintage:202609"


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class TiedBatchesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[1]").appName("unit-price-tied-batches")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def source(self):
        # SQL VALUES, not createDataFrame: no Python worker.
        return self.spark.sql(
            f"SELECT * FROM VALUES ('{EARLY}', 'U1', 10), ('{LATE}', 'U1', 11), ('{SEPTEMBER}', 'U1', 9) "
            "AS t(source_snapshot_id, mgm_bldrgst_pk, price_won)")

    def chosen(self, commit_order):
        import vintage_versions

        with mock.patch("unit_official_price.batch_commit_order", return_value=commit_order):
            moved = vintage_versions.unit_price_vintages(self.spark, self.source(), "r2.silver.unit_official_price")
        return {vintage: (batch, [row.price_won for row in rows.collect()]) for vintage, (rows, batch) in moved.items()}

    def test_the_batch_committed_last_is_the_vintages_rows(self):
        self.assertEqual(self.chosen([SEPTEMBER, EARLY, LATE]),
                         {"202609": (SEPTEMBER, [9]), "202610": (LATE, [11])})
        # The commit order decides, not the id: committed the other way round, the other batch.
        self.assertEqual(self.chosen([SEPTEMBER, LATE, EARLY])["202610"], (EARLY, [10]))

    def test_without_the_commit_order_two_batches_of_a_vintage_are_refused(self):
        with self.assertRaisesRegex(ValueError, "batches of vintage 202610"):
            self.chosen([])


if __name__ == "__main__":
    unittest.main()
