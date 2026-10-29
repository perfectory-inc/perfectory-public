"""A release loaded again outranks its earlier load, on a real local Iceberg catalog (ADR-0328).

`silver_release_compare` stamps every row it appends with the release's edition and the run's
instant, and a compared table orders a key's versions by both. Each test names the load that broke
before: a vintage re-loaded on the exclusive-unit bridge (its versions were ordered by the vintage
alone, so changed units tied with their old rows and removal markers tied with the rows they
removed), a hub month re-loaded under a run identity whose digest sorts lower (its versions were
ordered by that identity), and a compare resumed by a later run, whose rows get a later instant.
Rows are synthetic (PNUs start 99999).
"""
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
# Before any run's instant: the compare refuses a table holding a row loaded at or after it.
LOADED = datetime(2020, 1, 1, tzinfo=timezone.utc)
EXCLUSIVE, TITLES = "silver.building_register_exclusive_unit", "silver.building_register_titles"


def lane_compare(name):
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))["silver_refresh"]["compare"]


def default(kind):
    return {"string": "same", "int": 1, "long": 1, "double": 1.0, "array<string>": ["same"],
            "timestamp": LOADED, "date": date(2020, 1, 1)}[kind]


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ReloadedReleaseIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-release-compare-versions")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def setUp(self):
        from platform_contracts import create_table_columns_sql, load_lakehouse_contract

        self.table = None

        def create(name):
            self.table = name
            self.contract = load_lakehouse_contract(name)
            self.spark.sql(f"DROP TABLE IF EXISTS {self.qualified()}")
            self.spark.sql(f"CREATE TABLE {self.qualified()} ({create_table_columns_sql(self.contract)}) USING iceberg")

        self.create = create

    def qualified(self):
        return "`proof`.`silver`.`{}`".format(self.table.split(".", 1)[1])

    def unquoted(self):
        return f"proof.{self.table}"

    def frame(self, rows):
        from platform_contracts import column_names, create_table_columns_sql

        kinds = {c["name"]: c["logical_type"] for c in self.contract["columns"]}
        full = [tuple(row.get(c, None if c == "row_state" else default(kinds[c])) for c in column_names(self.contract))
                for row in rows]
        return self.spark.createDataFrame(full, " ".join(create_table_columns_sql(self.contract, indent=0).split()))

    def load(self, rows, branch=None):
        import lakehouse_ingest as ingest
        from platform_contracts import column_names

        mode = "overwrite" if branch else "append"
        return ingest.append_batch_once(self.spark, self.frame(rows), column_names(self.contract), self.qualified(),
                                        self.table, write_mode=mode, branch=branch)["record_ids"]

    def compare(self, release, objects, branch, compare, deadline_utc=None, now=None):
        import silver_release_compare as job

        args = Namespace(contract=self.table, branch=branch, source_snapshot_id=release, objects=",".join(objects),
                         content_columns=",".join(compare["content_columns"]), changes_branch=f"changes_{branch}",
                         bucket_prefix_length=compare["bucket_prefix_length"], deadline_utc=deadline_utc,
                         iceberg_catalog_name="proof")
        return job.compare_and_append(self.spark, args, now=now)

    def publish(self, branch):
        """What the lane does with a staged compare (`publish_branch`): main fast-forwards to it."""
        self.spark.sql(f"CALL proof.system.fast_forward('{self.table}', 'main', 'changes_{branch}')")
        self.spark.sql(f"ALTER TABLE {self.qualified()} DROP BRANCH changes_{branch}")

    def current(self, key, content):
        from platform_contracts import current_rows

        rows = current_rows(self.spark.table(self.unquoted()), self.contract).select(key, *content).collect()
        return Counter(tuple(tuple(v) if isinstance(v, list) else v for v in row) for row in rows)

    def expected(self, rows, key, content):
        frame = self.frame(rows).select(key, *content)
        return Counter(tuple(tuple(v) if isinstance(v, list) else v for v in row) for row in frame.collect())

    def appended(self, release):
        """The rows the compare of `release` appended: its changed keys' rows and its markers."""
        return self.spark.table(self.unquoted()).where(f"source_snapshot_id = '{release}'")

    def test_an_exclusive_unit_vintage_loaded_again_outranks_its_first_load_and_its_markers_remove(self):
        # Before ADR-0328 the bridge ordered a unit's versions by `vintage` alone: unit 2's old and
        # new rows both stayed current, and unit 3's marker tied with its row and was dropped.
        import changelog
        import lakehouse_ingest as ingest

        self.create(EXCLUSIVE)
        compare = lane_compare("hub-building-register-exclusive-unit-source-objects.json")
        key, content = "mgmt_key", compare["content_columns"]

        def unit(n, ho, part, vintage="209908"):
            return {"mgmt_key": f"1{n}-unit-{n}", "pnu": f"99999101001{n:04d}0000", "ho_name": ho,
                    "raw_columns": [f"unit-{n}", ho], "vintage": vintage, "source_record_id": part,
                    "source_part_id": part, "source_snapshot_id": f"hub-exclusive-unit:{vintage}:{part}"}

        first = [unit(1, "101", "a"), unit(2, "102", "a"), unit(3, "103", "a")]
        self.load(first)
        again = [unit(1, "101", "b"), unit(2, "202", "b"), unit(5, "105", "b")]
        release = again[0]["source_snapshot_id"]
        objects = self.load(again, branch="load_again")
        before = self.spark.sql(f"SELECT snapshot_id FROM {self.qualified()}.refs WHERE name = 'main'").first()[0]
        summary = self.compare(release, objects, "load_again", compare)
        self.assertEqual((summary["outcome"], summary["edition"]), ("staged", "209908"))
        self.assertEqual((summary["changed_key_rows"], summary["removed_keys"]), (2, 1))
        self.publish("load_again")

        self.assertEqual(self.current(key, content), self.expected(again, key, content))
        stamped = self.appended(release).select("vintage", "ingested_at_utc").distinct().collect()
        self.assertEqual(len(stamped), 1, "one edition and one instant on every appended row")
        self.assertEqual(stamped[0].vintage, "209908")
        self.assertGreater(stamped[0].ingested_at_utc.year, LOADED.year)
        # The unit-price join reads the vintage as of itself (`rows_as_of`): the second load, once.
        from platform_contracts import rows_as_of

        as_of = rows_as_of(self.spark.table(self.unquoted()), self.contract, "209908").select(key, *content)
        self.assertEqual(Counter(tuple(tuple(v) if isinstance(v, list) else v for v in r) for r in as_of.collect()),
                         self.expected(again, key, content))
        # The seal names the vintage, so a vintage that changed nothing is still held.
        snapshots = [{"snapshot_id": r.snapshot_id, "parent_id": r.parent_id, "summary": r.summary or {}}
                     for r in self.spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {self.qualified()}.snapshots")
                     .collect()]
        head = self.spark.sql(f"SELECT snapshot_id FROM {self.qualified()}.refs WHERE name = 'main'").first()[0]
        self.assertEqual(changelog.released_versions(snapshots, changelog.snapshot_lineage(snapshots, head)), {"209908"})
        self.assertNotEqual(before, head)

        # The durable edition record (a table property, which `publish_branch` sets) refuses an older
        # vintage after the summaries that named the newer one have expired.
        self.spark.sql(f"ALTER TABLE {self.qualified()} SET TBLPROPERTIES ('foundation.edition.209912' = '1')")
        self.assertIn("209912", ingest.read_editions(self.spark, self.qualified(), self.unquoted()))
        older = [unit(1, "101", "c", "209910")]
        objects = self.load(older, branch="load_older")
        with self.assertRaisesRegex(ValueError, "older than the held editions"):
            self.compare(older[0]["source_snapshot_id"], objects, "load_older", compare)

    def test_a_hub_month_loaded_again_under_a_lower_digest_outranks_its_first_load(self):
        # Before ADR-0328 the hub registers ordered versions by the run identity, whose digest sorts
        # a second load of one month below the first: its changes stayed hidden, its markers lost.
        self.create(TITLES)
        compare = lane_compare("hub-building-register-title-source-objects.json")
        key, content = "mgm_bldrgst_pk", compare["content_columns"]
        month = datetime(2099, 8, 1, tzinfo=timezone.utc)

        def title(n, purpose, release):
            return {"mgm_bldrgst_pk": f"1{n}-title-{n}", "title_row_id": f"title-{n}-{release[-4:]}",
                    "pnu": f"99999101001{n:04d}0000", "purpose_name_raw": purpose, "source_snapshot_id": release,
                    "valid_from_utc": month, "row_checksum_sha256": "0" * 64}

        high = "silver-refresh-building-register-titles-209908-ffffffffffffffff"
        low = "silver-refresh-building-register-titles-209908-0000000000000000"
        self.assertLess(low, high, "the second load's identity sorts first")
        self.load([title(1, "factory", high), title(2, "office", high), title(3, "shop", high)])
        again = [title(1, "factory", low), title(2, "warehouse", low), title(4, "school", low)]
        objects = self.load(again, branch="load_low")
        summary = self.compare(low, objects, "load_low", compare)
        self.assertEqual((summary["outcome"], summary["changed_key_rows"], summary["removed_keys"]), ("staged", 2, 1))
        self.publish("load_low")
        self.assertEqual(self.current(key, content), self.expected(again, key, content))
        # Every appended row, the marker included, carries the month: the edition is the column's type.
        self.assertEqual({r[0] for r in self.appended(low).selectExpr("CAST(valid_from_utc AS STRING)").distinct()
                          .collect()}, {"2099-08-01 00:00:00"})

    def test_a_resumed_compare_stamps_each_run_its_own_instant_and_each_key_stays_one_version(self):
        self.create(TITLES)
        compare = lane_compare("hub-building-register-title-source-objects.json")
        key, content = "mgm_bldrgst_pk", compare["content_columns"]

        def title(prefix, n, purpose, release, month):
            return {"mgm_bldrgst_pk": f"{prefix}-title-{n}", "title_row_id": f"title-{prefix}-{n}-{month.month}",
                    "pnu": f"99999101001{n:04d}0000", "purpose_name_raw": purpose, "source_snapshot_id": release,
                    "valid_from_utc": month, "row_checksum_sha256": "0" * 64}

        august, september = datetime(2099, 8, 1, tzinfo=timezone.utc), datetime(2099, 9, 1, tzinfo=timezone.utc)
        old = "silver-refresh-building-register-titles-209908-1111111111111111"
        new = "silver-refresh-building-register-titles-209909-2222222222222222"
        self.load([title(p, n, "factory", old, august) for p in ("10", "11") for n in (1, 2)])
        # Each bucket (the PK's first two characters) changes one register and drops one.
        release = [title(p, 1, "office", new, september) for p in ("10", "11")]
        objects = self.load(release, branch="load_resumed")
        deadline = datetime(2099, 1, 1, tzinfo=timezone.utc)
        clock = iter([deadline.replace(year=2098), deadline])
        stopped = self.compare(new, objects, "load_resumed", compare, deadline, now=lambda: next(clock))
        self.assertEqual((stopped["outcome"], stopped["buckets_done"]), ("deadline", 1))
        resumed = self.compare(new, objects, "load_resumed", compare)
        self.assertEqual((resumed["outcome"], resumed["buckets_done"]), ("staged", 2))
        self.publish("load_resumed")

        instants = self.appended(new).groupBy("ingested_at_utc").agg({"mgm_bldrgst_pk": "collect_set"}).collect()
        self.assertEqual(len(instants), 2, "each run stamps its own instant")
        for row in instants:
            prefixes = {pk[:2] for pk in row[1]}
            self.assertEqual(len(prefixes), 1, "one run's instant covers only the buckets that run compared")
        self.assertEqual(self.current(key, content), self.expected(release, key, content))


if __name__ == "__main__":
    unittest.main()
