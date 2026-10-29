"""A vintage table stored as versions reads back every vintage exactly (root ADR-0317).

Synthetic full copies of `silver.building_register_apartment_price` and `silver.unit_official_price`
go through the migration's own step (`vintage_versions.vintage_changes`) one vintage at a time, as
the job appends them. After each, `rows_as_of` of every vintage so far equals that vintage's full
copy on the key and content columns, repeats counted: a changed price, a correction notice added to
a key, a key that vanishes and comes back, a key without a management key, repeated identical lines,
and a vintage loaded again after a partial load. Only the changed keys are appended. PNUs are the
synthetic 99999 band.
"""
import importlib.util
import sys
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def rows(frame):
    """A frame's rows as a multiset; `raw_columns` lists as tuples."""
    return Counter(tuple(tuple(v) if isinstance(v, list) else v for v in row) for row in frame.collect())


PNU = "9999910100100010000"


def price_line(key, base_date, price, notice):
    """One apartment-price line: (mgmt_key, base_date, price_won, notice_date)."""
    return (key, base_date, price, notice)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class VintageVersionsSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        cls.spark = (SparkSession.builder.master("local[2]").appName("vintage-versions")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.load = staticmethod(load_lakehouse_contract)
        cls.names = staticmethod(column_names)
        cls.schema = staticmethod(lambda contract: " ".join(create_table_columns_sql(contract, indent=0).split()))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def apartment_copy(self, vintage, lines, load=1):
        """A vintage's full copy as the lane appends it."""
        contract = self.load("silver.building_register_apartment_price_versions")
        at = datetime(2099, 1, load, tzinfo=timezone.utc)
        rows = []
        for number, (key, base_date, price, notice) in enumerate(lines):
            raw = [key or "", PNU[:5], base_date, price, notice]
            row = {"mgmt_key": key, "sigungu_cd": PNU[:5], "bjdong_cd": PNU[5:10], "san_gubun": "0",
                   "bonbeon": "0001", "bubeon": "0000", "base_date": base_date, "price_won": price,
                   "notice_date": notice, "pnu": PNU, "raw_columns": raw, "vintage": vintage,
                   "source_record_id": f"bronze/OPN{vintage}01.zip", "source_part_id": f"part-{number % 2}",
                   "source_line_number": number, "source_snapshot_id": f"silver-refresh-price-{vintage}-{load}",
                   "ingested_at_utc": at, "row_state": None}
            rows.append(tuple(row[c] for c in self.names(contract)))
        return self.spark.createDataFrame(rows, self.schema(contract))

    def unit_copy(self, vintage, prices):
        """A vintage's join as `unit_official_price.py` appends it: (unit, base_date, price, ho)."""
        contract = self.load("silver.unit_official_price_versions")
        batch = f"price:1|exclusive:1|vintage:{vintage}"
        rows = [tuple({"mgm_bldrgst_pk": unit, "pnu": PNU, "dong_name": "101동", "ho_name": ho, "base_date": base_date,
                       "price_won": price, "sido": "99", "vintage": vintage, "source_snapshot_id": batch,
                       "source_record_id": f"unit-official-price/base-date-v2/99/{batch}",
                       "ingested_at_utc": datetime(2099, 1, 1, tzinfo=timezone.utc), "row_state": None}[c]
                      for c in self.names(contract)) for unit, base_date, price, ho in prices]
        return self.spark.createDataFrame(rows, self.schema(contract))

    def append_vintages(self, table, content, copies):
        """Appends each (vintage, full copy) as the migration does; returns the versions and the rows each added."""
        from pyspark.sql import functions as F

        import vintage_versions as job

        contract = self.load(table)
        versions = self.spark.createDataFrame([], self.schema(contract))
        added = []
        for index, (vintage, copy) in enumerate(copies):
            instant = F.lit(datetime(2099, 2, 1 + index, tzinfo=timezone.utc)).cast("timestamp")
            changes = job.vintage_changes(copy, versions, contract, content, vintage, f"batch-{index}", instant)
            changes = self.spark.createDataFrame(changes.collect(), changes.schema)
            added.append(changes.count())
            versions = versions.unionByName(changes)
        return contract, versions, added

    def assert_reconstructs(self, contract, versions, content, vintage, copy):
        from platform_contracts import current_row_latest, rows_as_of

        compared = [*current_row_latest(contract).key, *content]
        rebuilt = rows_as_of(versions, contract, vintage).select(*compared)
        self.assertEqual(rows(rebuilt), rows(copy.select(*compared)), f"vintage {vintage} does not read back as its full copy")

    def test_every_apartment_price_vintage_reads_back_as_its_full_copy(self):
        import vintage_versions as job

        august = [price_line("K-same", "20990101", "100", "20990430"),
                  price_line("K-price", "20990101", "200", "20990430"),
                  price_line("K-notice", "20990101", "300", "20990430"),
                  price_line("K-gone", "20990101", "400", "20990430"),
                  price_line(None, "20990101", "500", "20990430"),
                  price_line("K-repeat", "20990101", "600", "20990430"),
                  price_line("K-repeat", "20990101", "600", "20990430")]
        september = [price_line("K-same", "20990101", "100", "20990430"),
                     price_line("K-price", "20990101", "210", "20990430"),
                     price_line("K-notice", "20990101", "300", "20990430"),
                     price_line("K-notice", "20990101", "290", "20990531"),
                     price_line(None, "20990101", "500", "20990430"),
                     price_line("K-repeat", "20990101", "600", "20990430"),
                     price_line("K-new", "20990601", "700", "20990930")]
        october_partial = [line for line in september if line[0] != "K-new"]
        october = [*september, price_line("K-gone", "20990101", "400", "20990430")]
        copies = [("209908", self.apartment_copy("209908", august)),
                  ("209909", self.apartment_copy("209909", september)),
                  ("209910", self.apartment_copy("209910", october_partial)),
                  # The same vintage loaded again in full after the partial load (2026-10-11).
                  ("209910", self.apartment_copy("209910", october, load=2))]
        contract, versions, added = self.append_vintages(
            "silver.building_register_apartment_price_versions", job.APARTMENT_PRICE_CONTENT, copies)

        # August whole; September: K-price, K-notice (2 rows), K-repeat (1 row), K-new, one marker for
        # K-gone; the partial October: a marker for K-new; the full October: K-new and K-gone back.
        self.assertEqual(added, [7, 6, 1, 2])
        for vintage, copy in [copies[0], copies[1], copies[3]]:
            self.assert_reconstructs(contract, versions, job.APARTMENT_PRICE_CONTENT, vintage, copy)

    def test_every_unit_price_vintage_reads_back_as_its_join(self):
        import vintage_versions as job

        august = [("U-1", "20990101", 100, "101호"), ("U-2", "20990101", 200, "102호"), ("U-3", "20990101", 300, "103호")]
        september = [("U-1", "20990101", 100, "101호"), ("U-2", "20990101", 210, "102호"),
                     ("U-1", "20990601", 110, "101호")]
        october = [("U-1", "20990101", 100, "101호"), ("U-2", "20990101", 210, "102-1호"),
                   ("U-1", "20990601", 110, "101호"), ("U-3", "20990101", 300, "103호")]
        copies = [(v, self.unit_copy(v, rows)) for v, rows in (("209908", august), ("209909", september),
                                                               ("209910", october), ("209911", october))]
        contract, versions, added = self.append_vintages("silver.unit_official_price_versions",
                                                         job.UNIT_PRICE_CONTENT, copies)

        # A vintage equal to the last appends nothing (2026-10-28: Sejong's 202608-202610 were one copy).
        self.assertEqual(added, [3, 3, 2, 0])
        for vintage, copy in copies:
            self.assert_reconstructs(contract, versions, job.UNIT_PRICE_CONTENT, vintage, copy)

    def test_an_older_vintage_is_refused_and_a_table_without_markers_cannot_be_read_as_of(self):
        from platform_contracts import rows_as_of
        from silver_release_compare import refuse_older_edition

        refuse_older_edition("209910", {"209908", "209910"})
        with self.assertRaisesRegex(ValueError, "older than the held editions"):
            refuse_older_edition("209909", {"209908", "209910"})
        legacy = self.load("silver.unit_official_price")
        with self.assertRaisesRegex(ValueError, "removal markers"):
            rows_as_of(self.spark.createDataFrame([], self.schema(legacy)), legacy, "209908")


if __name__ == "__main__":
    unittest.main()
