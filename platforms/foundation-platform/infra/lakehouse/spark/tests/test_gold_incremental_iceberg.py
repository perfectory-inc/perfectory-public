"""Incremental panel Gold == full rebuild, on a real local Iceberg catalog (root ADR-0180).

Both producers run their own `run` against Silver tables made from the lakehouse contracts. Each
case builds Gold v1 in full, changes several Silver inputs (inserts, updates, deletes, and a
whole-table overwrite with a new batch id whose content is mostly the same), merges the change
incrementally, rebuilds v2 in full into a second table, and compares every PNU's content and
`row_digest`. It also shows that rows of PNUs no change reached keep their bytes and lineage, that
a data file holding none of them is not rewritten, and that the fallbacks rebuild whole. PNUs are
synthetic (`99999…`); no R2 credentials are needed.
"""
import json
import os
from datetime import date, datetime
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

SILVER = "silver"


def pnu(main: int) -> str:
    return f"99999000001{main:04d}0000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class IncrementalGoldIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-incremental")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "4")
                     # Without coalescing, the range-distributed Gold is four files, as a large
                     # Gold is many: the merge then has files it must leave alone.
                     .config("spark.sql.adaptive.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        for namespace in (SILVER, "gold", "reference"):
            cls.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS proof.{namespace}")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_the_difference_compares_fingerprints_and_still_sees_every_changed_row(self):
        import gold_incremental as incremental

        schema = "k string, a string, b string"
        old = self.spark.createDataFrame(
            [("1", "x", None), ("2", None, "x"), ("3", "same", "same"), ("3", "same", "same"), ("4", "gone", None)],
            schema)
        new = self.spark.createDataFrame(
            [("1", None, "x"), ("2", None, "x"), ("3", "same", "same"), ("5", "new", "x")], schema)
        changed = sorted(row.k for row in incremental.changed_rows(old, new, ("k",)).collect())
        # 1: its NULL moved to the other column (old and new side); 3: one duplicate fewer;
        # 4: deleted; 5: inserted. 2 is unchanged.
        self.assertEqual(changed, ["1", "1", "3", "4", "5"])
        self.assertEqual(incremental.changed_rows(old, new, ("k",)).columns, ["k"])
        with self.assertRaises(ValueError):
            incremental.changed_rows(old, new, ("pnu",))

    # ---- fixtures --------------------------------------------------------------------------

    def create(self, name):
        from platform_contracts import create_table_columns_sql, load_lakehouse_contract

        table = f"proof.{name}"
        self.spark.sql(f"DROP TABLE IF EXISTS {table}")
        self.spark.sql(f"CREATE TABLE {table} ({create_table_columns_sql(load_lakehouse_contract(name))}) USING iceberg")

    def put(self, name, rows):
        """Replace every row of `name` (one overwrite snapshot); the new head snapshot id."""
        from pyspark.sql import functions as F

        table = f"proof.{name}"
        schema = self.spark.table(table).schema
        frame = self.spark.createDataFrame([tuple(row.get(f.name) for f in schema.fields) for row in rows], schema)
        frame.writeTo(table).overwrite(F.lit(True))
        return self.head(table)

    def head(self, table):
        return str(self.spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()[0])

    def args(self, job, table, published, previous=None, fraction=0.9, sample=100000, extra=()):
        work = Path(self.directory.name) / f"run-{table}-{published[:10]}"
        work.mkdir(parents=True, exist_ok=True)
        argv = ["--input-mode", "iceberg", "--write-mode", "iceberg", "--iceberg-catalog-name", "proof",
                "--target-iceberg-table", table, "--allow-non-smoke-overwrite", "--iceberg-snapshot-id", "1",
                "--published-at-utc", published, "--summary-output", str(work / "summary.json"),
                "--lineage-output", str(work / "lineage.json"), *extra]
        if previous is not None:
            (work / "previous.json").write_text(json.dumps(previous), encoding="utf-8")
            argv += ["--incremental-from-snapshots", str(work / "previous.json"),
                     "--max-changed-key-fraction", str(fraction), "--parity-sample-keys", str(sample)]
        return job.parse_args(argv), work / "summary.json"

    def run_job(self, job, pins, table, published, **options):
        args, summary = self.args(job, table, published, **options)
        lineage = summary.with_name("lineage.json")
        lineage.unlink(missing_ok=True)
        log = StringIO()
        with redirect_stdout(log):
            self.assertEqual(job.run(self.spark, args, pins), 0)
        self.log = log.getvalue()
        # The lineage names the Gold snapshot the run left, never an input pin (the building run of
        # 2026-10-12 recorded the titles snapshot).
        recorded = json.loads(lineage.read_text(encoding="utf-8"))["iceberg_snapshot"]["snapshot_id"]
        self.assertEqual(recorded, self.head(f"proof.gold.{table}"))
        self.assertNotIn(recorded, pins.values())
        return json.loads(summary.read_text(encoding="utf-8"))

    def append(self, name, rows):
        """Add `rows` to `name` (one append snapshot); the new head snapshot id."""
        table = f"proof.{name}"
        schema = self.spark.table(table).schema
        self.spark.createDataFrame([tuple(row.get(f.name) for f in schema.fields) for row in rows], schema) \
            .writeTo(table).append()
        return self.head(table)

    def rows(self, table, job):
        columns = list(dict.fromkeys(["pnu", *job.CONTENT_DIGEST_COLUMNS, "row_digest", "source_snapshot_id", "published_at_utc"]))
        return {r.pnu: r for r in self.spark.table(f"proof.gold.{table}").select(*columns).collect()}

    def content(self, table, job):
        columns = list(dict.fromkeys(["pnu", *job.CONTENT_DIGEST_COLUMNS, "row_digest"]))
        return sorted(tuple(r) for r in self.spark.table(f"proof.gold.{table}").select(*columns).collect())

    def files(self, table):
        return {r.file_path: (r.lo, r.hi) for r in self.spark.sql(
            f"SELECT file_path, readable_metrics.pnu.lower_bound AS lo, readable_metrics.pnu.upper_bound AS hi "
            f"FROM proof.gold.{table}.files").collect()}

    def gold_head(self, table):
        return self.spark.sql(
            f"SELECT operation, summary FROM proof.gold.{table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()

    def assert_untouched(self, job, table, before, after, reached):
        """Rows no change reached keep every byte; files holding none of them stay in the table."""
        for key in sorted(set(before) - reached):
            self.assertEqual(after[key], before[key], f"{key} was not reached but its row changed")
        top = max(reached)
        kept = [path for path, (lo, _) in self.files_before.items() if lo > top]
        self.assertTrue(kept, "the fixture must leave at least one data file above the changed PNUs")
        self.assertTrue(set(kept) <= set(self.files(table)), "a file without a changed PNU was rewritten")

    # ---- building panel ----------------------------------------------------------------------

    def building_v1(self):
        titles = [{"mgm_bldrgst_pk": f"T{i}", "pnu": pnu(i), "purpose_code_raw": "01000", "structure_code_raw": "21",
                   "floor_area_m2": 100.0 + i, "ground_floor_count": 3, "basement_floor_count": 1,
                   "approval_year": 2000 + i, "title_row_id": f"title-row-{i}", "source_record_id": f"v1#{i}",
                   "source_snapshot_id": "titles-v1"} for i in range(1, 41)]
        evidence = {"building_link_method": "parent_key", "building_link_source_record_id": "synthetic#line=1",
                    "building_link_input_sha256": "a" * 64, "source_snapshot_id": "units-v1"}
        units = [{"mgm_bldrgst_pk": f"U{i}", "pnu": pnu(i), "building_mgm_bldrgst_pk": f"T{i}", "unit_row_id": f"u{i}",
                  "dong_join_name": "A", "dong_name_raw": "A", "unit_label_ko": f"{i}01", "unit_name_raw": f"{i}01",
                  "floor_number": 1, "floor_kind": "above_ground", **evidence} for i in range(1, 41, 2)]
        units.append({"mgm_bldrgst_pk": "U-orphan", "pnu": pnu(45), "building_mgm_bldrgst_pk": None,
                      "unit_row_id": "u-orphan", "unit_label_ko": "1", "floor_number": 1, "floor_kind": "above_ground",
                      "building_link_method": "parent_key", "source_snapshot_id": "units-v1"})
        floors = [{"floor_row_id": f"F{i}", "mgm_bldrgst_pk": f"T{i}", "floor_kind": "above_ground", "floor_number": 1,
                   "floor_index": 1, "floor_display_ko": "1F", "source_snapshot_id": "floors-v1"} for i in range(1, 41)]
        floors.append({"floor_row_id": "F-roof-7", "mgm_bldrgst_pk": "T7", "floor_kind": "rooftop",
                       "floor_display_ko": "R", "source_snapshot_id": "floors-v1"})
        areas = [{"area_row_id": f"A{i}", "mgm_bldrgst_pk": f"U{i}", "area_kind": "exclusive", "area_m2": 50.0 + i,
                  "usage_name_raw": "home", "structure_name_raw": "rc", "floor_kind": "above_ground",
                  "source_snapshot_id": "areas-v1"} for i in range(1, 41, 2)]
        areas.append({"area_row_id": "A-roof-7", "mgm_bldrgst_pk": "T7", "area_kind": "common", "area_m2": 9.0,
                      "usage_name_raw": "tank", "floor_kind": "rooftop", "source_snapshot_id": "areas-v1"})
        prices = [{"mgm_bldrgst_pk": f"U{i}", "pnu": pnu(i), "base_date": "20250101", "price_won": 1000 * i,
                   "sido": "99", "source_snapshot_id": "prices-v1"} for i in range(1, 41, 2)]
        return {"titles": titles, "units": units, "floors": floors, "areas": areas, "prices": prices}

    def building_tables(self):
        import building_panel_silver_to_gold as job

        return {"titles": job.TITLE_SOURCE, "units": job.UNIT_SOURCE, "floors": job.FLOOR_SOURCE,
                "areas": job.AREA_SOURCE, "prices": job.PRICE_SOURCE}

    def test_building_incremental_merge_equals_the_full_rebuild(self):
        import building_panel_silver_to_gold as job

        names = self.building_tables()
        for name in names.values():
            self.create(name)
        data = self.building_v1()
        pins1 = {names[k]: self.put(names[k], rows) for k, rows in data.items()}
        first = self.run_job(job, pins1, "building_panel", "2099-01-01T00:00:00Z")
        self.assertEqual(first["rebuild"]["mode"], "full")
        before = self.rows("building_panel", job)
        self.files_before = self.files("building_panel")
        self.assertGreater(len(self.files_before), 1, "range-ordered Gold is several files")

        # The monthly titles release overwrites every row under a new batch id and row ids; only
        # T1 (area), T2 (gone: its unit becomes unlinked) and a new building at PNU 3 changed.
        titles = [{**row, "source_snapshot_id": "titles-v2", "source_record_id": f"v2#{row['mgm_bldrgst_pk']}",
                   "title_row_id": f"title-row-v2-{row['mgm_bldrgst_pk']}"} for row in data["titles"]
                  if row["mgm_bldrgst_pk"] != "T2"]
        titles[0]["floor_area_m2"] = 999.0
        titles.append({**titles[1], "mgm_bldrgst_pk": "T3b", "pnu": pnu(3), "approval_year": 2024})
        units = [row for row in data["units"]] + [{**data["units"][0], "mgm_bldrgst_pk": "U2", "pnu": pnu(2),
                                                   "building_mgm_bldrgst_pk": "T2", "unit_row_id": "u2"}]
        floors = [{**row, "floor_display_ko": "1F*"} if row["mgm_bldrgst_pk"] == "T4" else row for row in data["floors"]]
        areas = [{**row, "area_m2": 77.0} if row["mgm_bldrgst_pk"] == "U5" else row for row in data["areas"]]
        prices = [row for row in data["prices"] if row["mgm_bldrgst_pk"] != "U1"] + [
            {"mgm_bldrgst_pk": "U5", "pnu": pnu(5), "base_date": "20260101", "price_won": 7, "sido": "99",
             "source_snapshot_id": "prices-v1"}]
        pins2 = {names["titles"]: self.put(names["titles"], titles), names["units"]: self.put(names["units"], units),
                 names["floors"]: self.put(names["floors"], floors), names["areas"]: self.put(names["areas"], areas),
                 names["prices"]: self.put(names["prices"], prices)}

        merged = self.run_job(job, pins2, "building_panel", "2099-02-01T00:00:00Z", previous=pins1)
        self.assertEqual(merged["rebuild"]["mode"], "incremental", merged["rebuild"])
        self.assertTrue(merged["rebuild"]["merged"])
        counts = merged["rebuild"]["counts"]
        self.assertEqual(counts["affected_pnus"], 5, counts)  # PNU 1..5; the overwrite moved no other row
        self.assertGreater(counts["parity_sample_pnus"], 0)
        self.assertEqual(merged["write_disposition"], "iceberg_merge")
        # The merge's stages live on local disk, never in the executor's memory store, and are gone
        # once the run ends: a whole unit price vintage filled the store in production (2026-10-12).
        held = [info.name() for info in self.spark.sparkContext._jsc.sc().getRDDStorageInfo() if info.memSize() > 0]
        self.assertEqual(held, [], "a merge stage was kept in executor memory")
        self.assertEqual(self.spark.conf.get("spark.sql.parquet.enableVectorizedReader"), "false",
                         "a stage of wide JSON rows is read back row-wise")
        local = (self.spark.conf.get("spark.local.dir", None) or "/tmp").split(",")[0]
        self.assertEqual(sorted(str(path) for path in Path(local).glob("gold-incremental-*")), [])
        self.run_job(job, pins2, "building_panel_full", "2099-02-01T00:00:00Z")
        self.assertEqual(self.content("building_panel", job), self.content("building_panel_full", job))
        self.assert_untouched(job, "building_panel", before, self.rows("building_panel", job),
                              {pnu(i) for i in range(1, 6)})

        head = self.gold_head("building_panel")
        self.assertEqual(head.operation, "overwrite")
        self.assertEqual(json.loads(head.summary["foundation.source-iceberg-snapshots"]), pins2)
        tag = "foundation-gold-input-gold-building_panel"
        for name, snapshot in pins2.items():
            ref = self.spark.sql(f"SELECT snapshot_id FROM proof.{name}.refs WHERE name = '{tag}'").first()
            self.assertEqual(str(ref.snapshot_id), snapshot, f"{name} keeps the next comparison point")
        # Its JSON rows overflow the vectorized reader's 2 GiB batch buffer in production: every
        # reader reads the table row-wise (2026-10-11).
        properties = {row.key: row.value for row in self.spark.sql("SHOW TBLPROPERTIES proof.gold.building_panel").collect()}
        self.assertEqual(properties.get("read.parquet.vectorization.enabled"), "false")

        # No input moved: nothing to merge, and the commit records the pins only.
        quiet = self.run_job(job, pins2, "building_panel", "2099-03-01T00:00:00Z", previous=pins2)
        self.assertEqual((quiet["rebuild"]["mode"], quiet["rebuild"]["merged"]), ("incremental", False))
        self.assertEqual(self.content("building_panel", job), self.content("building_panel_full", job))

        # Too large a change set: the full rebuild, with its reason.
        whole = self.run_job(job, pins2, "building_panel", "2099-04-01T00:00:00Z", previous=pins1, fraction=0.01)
        self.assertEqual(whole["rebuild"]["mode"], "full")
        self.assertIn("max_changed_key_fraction", whole["rebuild"]["full_reason"])
        self.assertEqual(self.content("building_panel", job), self.content("building_panel_full", job))

        # A Gold that is not what the build makes of its pins: the parity sample sees it.
        self.spark.sql(f"UPDATE proof.gold.building_panel SET buildings_json = '[]' WHERE pnu = '{pnu(30)}'")
        stale = self.run_job(job, pins2, "building_panel", "2099-05-01T00:00:00Z", previous=pins2)
        self.assertEqual(stale["rebuild"]["mode"], "full")
        self.assertIn("parity sample", stale["rebuild"]["full_reason"])
        self.assertEqual(self.content("building_panel", job), self.content("building_panel_full", job))

    def test_an_appended_price_batch_is_its_own_change_set(self):
        """Root ADR-0189: appends are read as the change set, and a switched batch maps both batches."""
        import building_panel_silver_to_gold as job

        names = self.building_tables()
        for name in names.values():
            self.create(name)
        data = self.building_v1()
        batch_a, batch_b = "price:1|exclusive:2|vintage:202501", "price:3|exclusive:4|vintage:202601"
        prices_a = [{**row, "source_snapshot_id": batch_a} for row in data["prices"]]
        pins1 = {names[k]: self.put(names[k], prices_a if k == "prices" else rows) for k, rows in data.items()}
        self.run_job(job, pins1, "building_append", "2099-01-01T00:00:00Z")
        price = names["prices"]
        phases = ("affected", "restrict", "build", "parity", "merge")

        # A new vintage, appended in two snapshots: its history drops U1 (PNU 1 loses its price,
        # a row only the old batch held), adds a date to U5 and corrects U7; the rest repeat.
        prices_b = [{**row, "source_snapshot_id": batch_b, "price_won": 1 if row["mgm_bldrgst_pk"] == "U7"
                     else row["price_won"]} for row in prices_a if row["mgm_bldrgst_pk"] != "U1"]
        prices_b.append({**prices_b[1], "base_date": "20260101", "price_won": 5})
        self.append(price, prices_b[:5])
        pins2 = {**pins1, price: self.append(price, prices_b[5:])}
        switched = self.run_job(job, pins2, "building_append", "2099-02-01T00:00:00Z", previous=pins1)
        self.assertEqual(switched["rebuild"]["mode"], "incremental", switched["rebuild"])
        self.assertEqual(switched["rebuild"]["appended_inputs"], [price])
        # Both batches' units: every priced PNU, PNU 1 among them.
        self.assertEqual(switched["rebuild"]["counts"]["affected_pnus"], len(prices_a))
        for phase in phases:
            self.assertIn(f"gold-incremental-phase {phase} seconds=", self.log)
        self.run_job(job, pins2, "building_append_full", "2099-02-01T00:00:00Z")
        self.assertEqual(self.content("building_append", job), self.content("building_append_full", job))

        # More rows of the batch already read: the change set is those rows, one unit's PNU.
        pins3 = {**pins2, price: self.append(price, [{**prices_b[4], "base_date": "20270101", "price_won": 9}])}
        same = self.run_job(job, pins3, "building_append", "2099-03-01T00:00:00Z", previous=pins2)
        self.assertEqual((same["rebuild"]["mode"], same["rebuild"]["appended_inputs"]), ("incremental", [price]))
        self.assertEqual(same["rebuild"]["counts"]["affected_pnus"], 1, same["rebuild"])
        self.run_job(job, pins3, "building_append_full", "2099-03-01T00:00:00Z")
        self.assertEqual(self.content("building_append", job), self.content("building_append_full", job))

        # A delete in between: not an append span, so the two snapshots are compared as before.
        self.spark.sql(f"DELETE FROM proof.{price} WHERE mgm_bldrgst_pk = 'U11'")
        pins4 = {**pins3, price: self.head(f"proof.{price}")}
        diffed = self.run_job(job, pins4, "building_append", "2099-04-01T00:00:00Z", previous=pins3)
        self.assertEqual((diffed["rebuild"]["mode"], diffed["rebuild"]["appended_inputs"]), ("incremental", []))
        self.assertEqual(diffed["rebuild"]["counts"]["affected_pnus"], 1, diffed["rebuild"])
        self.run_job(job, pins4, "building_append_full", "2099-04-01T00:00:00Z")
        self.assertEqual(self.content("building_append", job), self.content("building_append_full", job))

    # ---- parcel panel ------------------------------------------------------------------------

    def test_parcel_incremental_merge_equals_the_full_rebuild(self):
        import parcel_panel_silver_to_gold as job
        import vworld_parcel_editions as editions

        names = (*job.ALL_SOURCES, job.LINEAGE_SOURCE)
        for name in names:
            self.create(name)
        contract = editions.load()
        served = editions.snapshot_id(contract, editions.served(contract))
        parcels = [{"pnu": pnu(i), "boundary_id": f"b{i}", "source_snapshot_id": served} for i in range(1, 41)]
        prices = [{"pnu": pnu(i), "base_year": "2025", "base_month": "1", "price_per_m2": str(100 * i),
                   "announced_date": "2025-05-31", "source_snapshot_id": "price-v1", "source_record_id": f"p{i}"}
                  for i in [*range(1, 41), 50] if i != 3]
        zonings = [{"pnu": pnu(i), "inclusion_code": "1", "zone_code": "UQA100", "zone_name": "zone",
                    "source_snapshot_id": "plan-v1"} for i in range(1, 41, 3)]
        zone_codes = [{"ucode": "UQA100", "parent_ucode": "000000", "uname": "zone", "source_snapshot_id": "zc-v1"}]
        characteristics = [{"pnu": pnu(i), "area_m2": 10.0 + i, "land_category": "dae", "source_snapshot_id": "ch-v1"}
                           for i in range(1, 41, 2)]
        forest = [{"pnu": pnu(i), "area_m2": 5.0, "land_category": "im", "ownership_kind_code": "1",
                   "ownership_kind_name": "synthetic-owner-kind", "co_owner_count": 1, "source_snapshot_id": "fo-v1"}
                  for i in range(2, 41, 5)]
        # The code list: an older snapshot's name is not read, nor an abolished code of the newest.
        names = [
            {"region_cd": "9999900000", "full_name": "synthetic-si old-dong", "status": "존재",
             "snapshot_date": date(2098, 1, 1), "source_record_id": "synthetic-a", "ingested_at_utc": datetime(2098, 1, 1)},
            {"region_cd": "9999900000", "full_name": "synthetic-si synthetic-dong", "status": "존재",
             "snapshot_date": date(2099, 1, 1), "source_record_id": "synthetic-b", "ingested_at_utc": datetime(2099, 1, 1),
             "lowest_name": "synthetic-dong"},
            {"region_cd": "9999800000", "full_name": "synthetic-si gone-dong", "status": "폐지",
             "snapshot_date": date(2099, 1, 1), "source_record_id": "synthetic-b", "ingested_at_utc": datetime(2099, 1, 1)},
        ]
        transfers = [{"pnu": pnu(i), "transfer_history_seq": 1, "parcel_history_seq": "1", "reason": "split",
                      "moved_at": "2020-01-01", "source_snapshot_id": "tr-v1"} for i in range(1, 41)]
        rights = [{"pnu": pnu(i), "right_serial_no": "1", "right_ratio": "1/2", "closure_kind_name": "open",
                   "source_snapshot_id": "lr-v1", "source_record_id": f"r{i}"} for i in range(1, 41, 4)]
        # The land-right release already numbers the map's 동 9999900000 as 9999800000 (a 1:1
        # renumbering the served edition predates): PNU 6's right is read under the served code.
        renumbered = "9999800000" + pnu(6)[10:]
        rights.append({**rights[0], "pnu": renumbered, "source_record_id": "r-renumbered"})
        code_changes = [{"change_key": "c1", "kind": "pair", "old_code": "9999900000", "new_code": "9999800000",
                         "level": "eupmyeondong", "effective_date": "20990101", "source": "synthetic",
                         "derivation_run_id": "run", "recorded_at_utc": datetime(2099, 1, 1)}]
        # PNU 3 was renumbered from PNU 50: it shows PNU 50's price through the lineage.
        lineage = [{"predecessor_pnu": pnu(50), "successor_pnu": pnu(3), "relation": "code_change",
                    "grade": "code_derived", "evidence_kind": "code", "evidence_ref": "synthetic"}]
        data = {job.PARCEL_SOURCE: parcels, job.PRICE_SOURCE: prices, job.ZONING_SOURCE: zonings,
                job.ZONE_CODE_SOURCE: zone_codes, job.CHARACTERISTIC_SOURCE: characteristics,
                job.FOREST_SOURCE: forest, job.TRANSFER_SOURCE: transfers, job.LAND_RIGHT_SOURCE: rights,
                job.LINEAGE_SOURCE: lineage, job.CODE_CHANGE_SOURCE: code_changes, job.LEGAL_DONG_SOURCE: names}
        pins1 = {name: self.put(name, rows) for name, rows in data.items()}
        self.run_job(job, pins1, "parcel_panel", "2099-01-01T00:00:00Z")
        before = self.rows("parcel_panel", job)
        # Names as the sources wrote them, never their codes.
        self.assertEqual(json.loads(before[pnu(1)].legal_dong_json),
                         {"name": "synthetic-si synthetic-dong", "lowest_name": "synthetic-dong"})
        self.assertEqual(json.loads(before[pnu(2)].forest_ledger_json)["ownership_kind_name"], "synthetic-owner-kind")
        self.assertEqual(json.loads(before[pnu(2)].forest_ledger_json)["ownership_kind"], "1")
        self.assertIn('"price_per_m2":5000', before[pnu(3)].price_json, "the carried price")
        self.assertEqual(before[pnu(6)].land_right_total, 1, "the renumbered right, under the served code")
        self.assertNotIn(renumbered, before)
        self.files_before = self.files("parcel_panel")
        self.assertGreater(len(self.files_before), 1)

        # PNU 2 retired and PNU 0 is new; PNU 50 (not a parcel of the map) changed its price, which
        # reaches PNU 3 only through the lineage; PNU 4's history changed; the land-right release
        # overwrote every row under a new batch id with the same content.
        changed = {
            job.PARCEL_SOURCE: [row for row in parcels if row["pnu"] != pnu(2)] + [
                {"pnu": pnu(0), "boundary_id": "b0", "source_snapshot_id": served}],
            job.PRICE_SOURCE: [{**row, "price_per_m2": "6000"} if row["pnu"] == pnu(50) else row for row in prices],
            job.TRANSFER_SOURCE: transfers + [{**transfers[3], "transfer_history_seq": 2, "moved_at": "2026-01-01"}],
            job.LAND_RIGHT_SOURCE: [{**row, "source_snapshot_id": "lr-v2", "source_record_id": f"v2-{row['pnu']}"}
                                    for row in rights],
        }
        pins2 = {**pins1, **{name: self.put(name, rows) for name, rows in changed.items()}}
        merged = self.run_job(job, pins2, "parcel_panel", "2099-02-01T00:00:00Z", previous=pins1)
        self.assertEqual(merged["rebuild"]["mode"], "incremental", merged["rebuild"])
        reached = {pnu(0), pnu(2), pnu(3), pnu(4), pnu(50)}
        self.assertEqual(merged["rebuild"]["counts"]["affected_pnus"], len(reached), merged["rebuild"])
        self.assertEqual(merged["rebuild"]["counts"]["deleted_pnus"], 1)
        self.assertEqual(merged["rebuild"]["counts"]["inserted_pnus"], 1)
        self.run_job(job, pins2, "parcel_panel_full", "2099-02-01T00:00:00Z")
        self.assertEqual(self.content("parcel_panel", job), self.content("parcel_panel_full", job))
        after = self.rows("parcel_panel", job)
        self.assertIn('"price_per_m2":6000', after[pnu(3)].price_json)
        self.assert_untouched(job, "parcel_panel", before, after, reached - {pnu(50)})

        # Root ADR-0273: a change to the zone code table, the code change table or the lineage is
        # mapped to PNUs too. Each merges only the parcels it reaches and equals the full rebuild.
        # A zone code moves under another anchor: the parcels zoned under it (1, 4, 7, ...) change.
        moved_zone = [*zone_codes, {"ucode": "UQA110", "parent_ucode": "UQA100", "uname": "child",
                                    "source_snapshot_id": "zc-v1"}]
        zone_changes = {
            job.ZONE_CODE_SOURCE: moved_zone,
            job.ZONING_SOURCE: zonings + [{**zonings[0], "pnu": pnu(5), "zone_code": "UQA110"}],
        }
        pins3 = {**pins2, **{name: self.put(name, rows) for name, rows in zone_changes.items()}}
        self.assert_merge_equals_full(job, pins3, pins2, "2099-03-01T00:00:00Z", {pnu(5)})
        pins4 = {**pins3, job.ZONE_CODE_SOURCE: self.put(job.ZONE_CODE_SOURCE, [
            {**row, "parent_ucode": "UQB001"} if row["ucode"] == "UQA110" else row for row in moved_zone])}
        self.assert_merge_equals_full(job, pins4, pins3, "2099-04-01T00:00:00Z", {pnu(5)})

        # The code change row is gone: PNU 6 loses the renumbered right. The pair's codes are the
        # fixture's whole 시군구, so every parcel is reached.
        pins5 = {**pins4, job.CODE_CHANGE_SOURCE: self.put(job.CODE_CHANGE_SOURCE, [])}
        merged = self.assert_merge_equals_full(job, pins5, pins4, "2099-05-01T00:00:00Z",
                                               {pnu(i) for i in range(0, 41) if i != 2},
                                               fraction=1.0)
        self.assertIn(self.rows("parcel_panel", job)[pnu(6)].land_right_total, (0, None), merged["rebuild"])

        # A lineage edge 50 -> 7 is added: both its PNUs are reached, and 3, which reads PNU 50.
        lineage2 = lineage + [{**lineage[0], "successor_pnu": pnu(7)}]
        pins6 = {**pins5, job.LINEAGE_SOURCE: self.put(job.LINEAGE_SOURCE, lineage2)}
        self.assert_merge_equals_full(job, pins6, pins5, "2099-06-01T00:00:00Z", {pnu(3), pnu(7), pnu(50)})

        # A newer code list renames the 동 every parcel is under: all of them are reached, by its code.
        renamed = names + [{**names[1], "full_name": "synthetic-si new-dong", "lowest_name": None,
                            "snapshot_date": date(2099, 7, 1), "source_record_id": "synthetic-c"}]
        pins7 = {**pins6, job.LEGAL_DONG_SOURCE: self.put(job.LEGAL_DONG_SOURCE, renamed)}
        self.assert_merge_equals_full(job, pins7, pins6, "2099-07-01T00:00:00Z",
                                      {pnu(i) for i in range(0, 41) if i != 2}, fraction=1.0)
        self.assertEqual(json.loads(self.rows("parcel_panel", job)[pnu(1)].legal_dong_json),
                         {"name": "synthetic-si new-dong", "lowest_name": None})

    def assert_merge_equals_full(self, job, pins, previous, published, reached, fraction=0.9):
        """Merges `pins` over `previous` incrementally, reaching exactly `reached`, and compares every
        PNU with a full rebuild of `pins`."""
        merged = self.run_job(job, pins, "parcel_panel", published, previous=previous, fraction=fraction)
        self.assertEqual(merged["rebuild"]["mode"], "incremental", merged["rebuild"])
        self.assertEqual(merged["rebuild"]["counts"]["affected_pnus"], len(reached), merged["rebuild"])
        self.run_job(job, pins, "parcel_panel_full", published)
        self.assertEqual(self.content("parcel_panel", job), self.content("parcel_panel_full", job))
        return merged


if __name__ == "__main__":
    unittest.main()
