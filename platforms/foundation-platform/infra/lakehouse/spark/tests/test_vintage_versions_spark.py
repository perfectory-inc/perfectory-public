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

    def test_only_the_keys_whose_inputs_changed_are_joined_again(self):
        """Root ADR-0317 §6: a changed price, a moved unit and a newly named priced unit, not the rest."""
        import unit_official_price as job

        prices = self.spark.createDataFrame(
            [("K1", "209908"), ("K2", "209908"), ("K2", "209909"), ("K3", "209908"), ("K4", "209908"), ("K5", "209911")],
            "mgmt_key string, vintage string")
        dictionary = self.spark.createDataFrame(
            [("K1", PNU, "101동", "102호"), ("K2", PNU, "101동", "201호"), ("K3", PNU, "101동", "301호"),
             ("K4", PNU, "101동", "401호"), ("K6", PNU, "101동", "601호")],
            "mgmt_key string, pnu string, dong_name string, ho_name string")
        joined = self.spark.createDataFrame(
            [("K1", PNU, "101동", "101호"), ("K2", PNU, "101동", "201호"), ("K3", PNU, "101동", "301호"),
             ("K7", PNU, "101동", "701호")],
            "mgm_bldrgst_pk string, pnu string, dong_name string, ho_name string")
        keys = {row.key for row in job.rejoined_keys(prices, dictionary, joined, "209909", "209910").collect()}
        # K2: a price version at the held vintage; K1: moved to another ho; K7: dropped from the
        # dictionary; K4: newly named and priced. K3 is unchanged, K6 has no price, K5's change is later.
        self.assertEqual(keys, {"K1", "K2", "K4", "K7"})

    def test_an_older_vintage_is_refused_and_a_table_without_markers_cannot_be_read_as_of(self):
        from platform_contracts import rows_as_of
        from silver_release_compare import refuse_older_edition

        refuse_older_edition("209910", {"209908", "209910"})
        with self.assertRaisesRegex(ValueError, "older than the held editions"):
            refuse_older_edition("209909", {"209908", "209910"})
        legacy = self.load("silver.unit_official_price")
        with self.assertRaisesRegex(ValueError, "removal markers"):
            rows_as_of(self.spark.createDataFrame([], self.schema(legacy)), legacy, "209908")

    def test_a_vintage_written_in_key_shards_is_the_versions_its_one_commit_would_be(self):
        # 2026-11-04: one apartment vintage took 13,226 s, more than a run may hold the pool; a
        # vintage is written in key-hash shards, each its own commit (vintage_versions.shard_of).
        from pyspark.sql import functions as F

        import vintage_versions as job
        from platform_contracts import current_row_latest

        contract = self.load("silver.building_register_apartment_price_versions")
        key = list(current_row_latest(contract).key)
        august = [price_line(f"K-{n}", "20990101", str(100 + n), "20990430") for n in range(12)]
        september = [price_line(f"K-{n}", "20990101", str(100 + n + (n % 3 == 0)), "20990430") for n in range(1, 14)]
        copies = [("209908", self.apartment_copy("209908", august)), ("209909", self.apartment_copy("209909", september))]
        versions = self.spark.createDataFrame([], self.schema(contract))
        for index, (vintage, copy) in enumerate(copies):
            instant = F.lit(datetime(2099, 2, 1 + index, tzinfo=timezone.utc)).cast("timestamp")
            for shard in range(3):
                mine = job.shard_of(key, 3) == shard
                changes = job.vintage_changes(copy.where(mine), versions.where(mine), contract,
                                              job.APARTMENT_PRICE_CONTENT, vintage, f"batch-{index}", instant)
                versions = versions.unionByName(self.spark.createDataFrame(changes.collect(), changes.schema))
        _, whole, _ = self.append_vintages("silver.building_register_apartment_price_versions",
                                           job.APARTMENT_PRICE_CONTENT, copies)
        compared = [*key, *job.APARTMENT_PRICE_CONTENT, "vintage", "row_state"]
        self.assertEqual(rows(versions.select(*compared)), rows(whole.select(*compared)))
        for vintage, copy in copies:
            self.assert_reconstructs(contract, versions, job.APARTMENT_PRICE_CONTENT, vintage, copy)
        self.assertEqual(job.shard_record("r", 0, 4), "r/shard-1-of-4")

    def test_the_first_vintage_is_its_rows_as_the_comparison_against_no_versions_finds(self):
        # 2026-11-04: the first apartment vintage spent 13,226 s comparing 202M rows with an empty
        # table; with no older vintage held, every row is a change and none is compared.
        from pyspark.sql import functions as F

        import vintage_versions as job

        contract = self.load("silver.building_register_apartment_price_versions")
        lines = [price_line(f"K-{n}", "20990101", str(100 + n), "20990430") for n in range(6)]
        lines.append(lines[0])  # a repeated line stays two rows
        copy = self.apartment_copy("209908", lines)
        empty = self.spark.createDataFrame([], self.schema(contract))
        instant = F.lit(datetime(2099, 2, 1, tzinfo=timezone.utc)).cast("timestamp")
        compared = job.vintage_changes(copy, empty, contract, job.APARTMENT_PRICE_CONTENT, "209908", "b", instant)
        first = job.vintage_changes(copy, empty, contract, job.APARTMENT_PRICE_CONTENT, "209908", "b", instant,
                                    first=True)
        self.assertEqual(first.dtypes, compared.dtypes)
        self.assertEqual(rows(first), rows(compared))
        self.assertEqual(first.count(), 7)

    def test_parity_counts_what_the_checksums_find_and_tells_a_null_from_an_empty_value(self):
        import vintage_versions as job

        august = [price_line(f"K-{n}", "20990101", str(100 + n), "20990430") for n in range(8)]
        august.append(price_line("K-null", "20990101", "900", None))
        contract, versions, _ = self.append_vintages("silver.building_register_apartment_price_versions",
                                                     job.APARTMENT_PRICE_CONTENT,
                                                     [("209908", self.apartment_copy("209908", august))])
        same = job.parity(versions, self.apartment_copy("209908", august), contract, job.APARTMENT_PRICE_CONTENT,
                          "209908", buckets=4)
        self.assertEqual((same["missing"], same["extra"], same["buckets_differing"]), (0, 0, 0))
        self.assertEqual((same["full_copy_rows"], same["rebuilt_rows"]), (9, 9))
        for changed in ([*august[:3], price_line("K-3", "20990101", "999", "20990430"), *august[4:]],
                        [*august[:-1], price_line("K-null", "20990101", "900", "")]):
            with self.subTest(changed=changed[-1]):
                found = job.parity(versions, self.apartment_copy("209908", changed), contract,
                                   job.APARTMENT_PRICE_CONTENT, "209908", buckets=4)
                self.assertEqual((found["missing"], found["extra"]), (1, 1))
                self.assertEqual(found["buckets_differing"], 1)
        fewer = job.parity(versions, self.apartment_copy("209908", august[:-2]), contract,
                           job.APARTMENT_PRICE_CONTENT, "209908", buckets=4)
        self.assertEqual((fewer["missing"], fewer["extra"]), (0, 2))


class RunBudgetTest(unittest.TestCase):
    def test_a_step_starts_only_while_its_cost_fits_and_a_run_plans_with_what_it_measured(self):
        import time

        import vintage_versions as job

        budget = job.Budget(100.0, time.monotonic(), {"write": 60.0, "parity": 30.0})
        self.assertTrue(budget.allows("write"))
        measured = set()
        budget.took("write", 10.0, measured)
        self.assertEqual(budget.costs["write"], 10.0, "the first measured step replaces the estimate")
        budget.took("write", 5.0, measured)
        self.assertEqual(budget.costs["write"], 10.0, "then the longest step of the run counts")
        budget.started -= 95
        self.assertFalse(budget.allows("write"))

    def test_the_contract_is_the_runs_size_and_refuses_a_step_no_run_can_take(self):
        import json
        import tempfile

        import vintage_versions as job

        contract = job.load_contract()
        self.assertEqual([t["table"] for t in contract["targets"]],
                         ["silver.building_register_apartment_price_versions", "silver.unit_official_price_versions"])
        jobs = json.loads((Path(job.__file__).resolve().parents[4] / "orchestration" / "jobs.v1.json")
                          .read_text(encoding="utf-8"))
        (limit,) = [j["time_limit_minutes"] for j in jobs["jobs"] if j["id"] == "unit_price_versions_migration"]
        self.assertEqual(contract["run_time_budget_seconds"], limit * 60 * 9 // 10)
        contract["targets"][0]["write_shards"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "contract.json"
            path.write_text(json.dumps(contract), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not fit"):
                job.load_contract(path)

    def test_a_done_migration_is_a_no_op_and_one_that_cannot_move_fails(self):
        # The job runs daily until done (unit-price-versions-migration.definition.json): a complete run
        # took no step and says `unchanged`; one not complete that took no step is stuck, not a no-op.
        import vintage_versions as job

        def summary(complete, *entries):
            return {"complete": complete, "parity": True,
                    "targets": {"t": {"vintages": {str(n): e for n, e in enumerate(entries)}}}}

        held = {"source_batch": "b", "parity": "held"}
        self.assertEqual(job.run_state(summary(True, held, held)), ("complete", False))
        self.assertEqual(job.run_state(summary(True, held, {"parity_seconds": 1.0, "missing": 0, "extra": 0})),
                         ("complete", True))
        self.assertEqual(job.run_state(summary(False, held, {"appended_seconds": {"1/4": 2.0}})), ("progress", True))
        self.assertEqual(job.run_state(summary(False, held, {"source_batch": "b"})), ("stalled", False))


if __name__ == "__main__":
    unittest.main()
