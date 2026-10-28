"""The one-time migration on a real local Iceberg catalog (root ADR-0317).

Both legacy tables are filled the way their writers fill them (`append_batch_once`: the price lane
appends a full copy per vintage; the join appends a batch per vintage, a vintage joined twice holds
two batches). `vintage_versions.migrate` writes each `_versions` table, records every vintage in its
commits, and its parity check finds no difference. A second run appends nothing and checks again.
Rows are synthetic (PNU band 99999).
"""
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

PNU = "9999910100100010000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class VintageVersionsIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("vintage-versions-iceberg")
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

    def legacy(self, contract_name, rows):
        """Appends `rows` (dicts) to the legacy table as its writer does."""
        from lakehouse_ingest import append_batch_once
        from platform_contracts import column_names, create_table_columns_sql, ensure_contract_table, load_lakehouse_contract

        contract = load_lakehouse_contract(contract_name)
        table = "`proof`.`silver`.`{}`".format(contract_name.split(".", 1)[1])
        ensure_contract_table(self.spark, table, contract)
        schema = " ".join(create_table_columns_sql(contract, indent=0).split())
        frame = self.spark.createDataFrame([tuple(row.get(c) for c in column_names(contract)) for row in rows], schema)
        self.assertTrue(append_batch_once(self.spark, frame, column_names(contract), table, contract_name)["appended"])

    def migrate(self, target):
        import vintage_versions as job

        return job.migrate(self.spark, Namespace(target=target, parity_only=False, iceberg_catalog_name="proof"))

    def test_both_tables_migrate_with_parity_and_a_second_run_appends_nothing(self):
        from lakehouse_ingest import read_editions

        at = datetime(2099, 1, 1, tzinfo=timezone.utc)

        def line(vintage, key, price, number):
            return {"mgmt_key": key, "base_date": "20990101", "price_won": price, "notice_date": "20990430",
                    "pnu": PNU, "raw_columns": [key, "20990101", price], "vintage": vintage,
                    "source_record_id": f"bronze/OPN{vintage}01.zip", "source_part_id": f"part/{vintage}/0",
                    "source_line_number": number, "source_snapshot_id": f"silver-refresh-price-{vintage}",
                    "ingested_at_utc": at}

        self.legacy("silver.building_register_apartment_price",
                    [line("209908", "K-1", "100", 0), line("209908", "K-2", "200", 1)])
        self.legacy("silver.building_register_apartment_price",
                    [line("209909", "K-1", "100", 0), line("209909", "K-2", "210", 1), line("209909", "K-3", "300", 2)])

        def unit(batch, key, price, ho):
            return {"mgm_bldrgst_pk": key, "pnu": PNU, "dong_name": "101동", "ho_name": ho, "base_date": "20990101",
                    "price_won": price, "sido": "99", "source_snapshot_id": batch,
                    "source_record_id": f"unit-official-price/base-date-v2/99/{batch}"}

        first, again = "price:1|exclusive:1|vintage:209908", "price:2|exclusive:1|vintage:209908"
        self.legacy("silver.unit_official_price", [unit(first, "U-1", 100, "101호")])
        # The same vintage joined again (2026-10-11): the panel serves the later batch.
        self.legacy("silver.unit_official_price", [unit(again, "U-1", 100, "101호"), unit(again, "U-2", 200, "102호")])

        prices = self.migrate("silver.building_register_apartment_price_versions")
        self.assertTrue(prices["parity"], prices)
        self.assertEqual({v: e["version_rows"] for v, e in prices["vintages"].items()}, {"209908": 2, "209909": 2})
        units = self.migrate("silver.unit_official_price_versions")
        self.assertTrue(units["parity"], units)
        self.assertEqual(units["vintages"]["209908"]["source_batch"], again)
        self.assertEqual(units["vintages"]["209908"]["version_rows"], 2)

        table = "`proof`.`silver`.`building_register_apartment_price_versions`"
        self.assertEqual(read_editions(self.spark, table, "proof.silver.building_register_apartment_price_versions"),
                         {"209908", "209909"})
        rows = self.spark.table(table).count()
        again_report = self.migrate("silver.building_register_apartment_price_versions")
        self.assertTrue(again_report["parity"])
        self.assertTrue(all("appended_seconds" not in entry for entry in again_report["vintages"].values()))
        self.assertEqual(self.spark.table(table).count(), rows, "a second run appends nothing")


if __name__ == "__main__":
    unittest.main()
