"""The parcel served-Gold job on a Spark runtime (root ADR-0133 §2).

Skipped where pyspark is missing, which is the CI lane. `ServedFrameSparkTest` needs pyspark only;
`ParcelServedGoldIcebergTest` runs `main` end to end against a temporary local Hadoop catalog and
opts in with RUN_SPARK_TESTS=1 like the other Iceberg proofs, because it needs the pinned Iceberg
runtime jar. Neither touches a real catalog, credentials or data.
"""

import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import served_gold_common as common  # noqa: E402
import parcel_boundary_served_gold as job  # noqa: E402



def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


HAS_PYSPARK = has_pyspark()
RUN_ICEBERG = os.environ.get("RUN_SPARK_TESTS") == "1"
SRID = job.GEOMETRY_SRID
BASE_SCHEMA = (
    "pnu string, geometry_wkb binary, geometry_srid int, geometry_checksum_sha256 string, "
    "source_snapshot_id string"
)


def pnu(n):
    return f"99999101001{n:04d}0000"


def silver_row(n, srid=SRID, snapshot="synthetic-1"):
    return (pnu(n), bytes([1, 3, n % 256]), srid, f"{n:064x}", snapshot)


def edit(seq, n, op):
    upsert = op == "upsert"
    return {
        "change_seq": seq,
        "feature_id": pnu(n),
        "op": op,
        "geometry_srid": SRID,
        "geometry_wkb_hex": f"0106{seq:04x}" if upsert else None,
        "geometry_checksum_sha256": f"{seq:064x}" if upsert else None,
    }


def session(name, iceberg_warehouse=None):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName(name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executorEnv.PYTHONPATH", common.JOBS_DIR)
    )
    if iceberg_warehouse:
        builder = (
            builder.config("spark.sql.extensions",
                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", iceberg_warehouse)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@unittest.skipUnless(HAS_PYSPARK, "requires pyspark")
class ServedFrameSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session("parcel-served-gold")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="parcel-served-")
        self.addCleanup(self.workspace.cleanup)

    def base(self, rows):
        return self.spark.createDataFrame(rows, BASE_SCHEMA)

    def test_a_pnu_served_twice_is_refused(self):
        base = self.base([silver_row(1), silver_row(2), silver_row(2)])
        with self.assertRaisesRegex(ValueError, f"more than once: {pnu(2)} x2"):
            job.check_silver(base, "synthetic-1")

    def test_a_silver_row_in_another_crs_or_an_empty_snapshot_is_refused(self):
        with self.assertRaisesRegex(ValueError, "not EPSG:4326"):
            job.check_silver(self.base([silver_row(1), silver_row(2, srid=5186)]), "synthetic-1")
        with self.assertRaisesRegex(ValueError, "holds no rows"):
            job.check_silver(self.base([]), "synthetic-1")

    def test_served_is_silver_minus_deletes_plus_new(self):
        base = self.base([silver_row(n) for n in range(1, 7)])
        rows = job.check_silver(base, "synthetic-1")
        ledger = [
            edit(1, 2, "delete"),
            edit(2, 3, "upsert"),   # replaces a Silver parcel
            edit(3, 9, "upsert"),   # a parcel Silver does not hold
            edit(4, 8, "delete"),   # absent: counted
            edit(5, 4, "delete"),
            edit(6, 4, "upsert"),   # deleted, then drawn again
        ]
        served, counts, expected = job.build_served_frame(base, rows, ledger)
        got = {row["pnu"]: row for row in served.collect()}
        self.assertEqual(sorted(got), sorted(pnu(n) for n in (1, 3, 4, 5, 6, 9)))
        self.assertEqual(expected, len(got))
        self.assertEqual(counts, {"upserts": 3, "deletes": 3, "deletes_of_absent_features": 1})
        self.assertEqual({p for p, r in got.items() if r["origin"] == "edit"}, {pnu(3), pnu(4), pnu(9)})
        self.assertEqual(got[pnu(3)]["source_snapshot_id"], "map-edit-2")
        self.assertEqual(bytes(got[pnu(4)]["geometry_wkb"]).hex(), "01060006")
        self.assertEqual(got[pnu(1)]["source_snapshot_id"], "synthetic-1")
        self.assertEqual({r["geometry_srid"] for r in got.values()}, {SRID})

    def test_no_edits_serve_the_snapshot_unchanged(self):
        base = self.base([silver_row(n) for n in range(1, 4)])
        served, counts, expected = job.build_served_frame(base, 3, [])
        self.assertEqual((served.count(), expected), (3, 3))
        self.assertEqual(counts["upserts"] + counts["deletes"], 0)

    def test_the_parts_add_up_to_the_served_rows_and_match_their_digests(self):
        rows = [silver_row(n) for n in range(1, 41)]
        frame = self.base(rows).select("pnu", "geometry_wkb", self.lit_origin())
        out = Path(self.workspace.name) / "parts"
        parts = common.write_handoff_parts(
            frame, out, id_column="pnu", property_columns=(), srid=SRID, parts=4
        )
        self.assertGreater(len(parts), 1, "the handoff is split, not one file")
        self.assertEqual(sum(part["rows"] for part in parts), len(rows))
        common.verify_handoff_parts(out, parts, served=len(rows))
        lines = []
        for part in parts:
            body = (out / part["path"]).read_bytes()
            self.assertEqual(hashlib.sha256(body).hexdigest(), part["sha256"])
            lines.extend(body.decode("utf-8").splitlines())
        expected = [
            common.bake_handoff_line(p, {}, wkb.hex(), SRID, "source") for p, wkb, *_ in sorted(rows)
        ]
        self.assertEqual(lines, expected, "parts concatenated are the v1 lines in PNU order")
        self.assertEqual(json.loads(lines[0])["properties"], {})
        with self.assertRaises(FileExistsError):
            common.write_handoff_parts(frame, out, id_column="pnu", property_columns=(), srid=SRID, parts=4)

    def test_a_session_whose_executors_cannot_import_the_jobs_is_refused(self):
        environment = self.spark.sparkContext.environment
        saved = environment.pop("PYTHONPATH", None)
        try:
            frame = self.base([silver_row(1)]).select("pnu", "geometry_wkb", self.lit_origin())
            with self.assertRaisesRegex(ValueError, "executors cannot import"):
                common.write_handoff_parts(
                    frame, Path(self.workspace.name) / "refused", id_column="pnu", property_columns=(),
                    srid=SRID, parts=1,
                )
        finally:
            if saved is not None:
                environment["PYTHONPATH"] = saved
        self.assertFalse((Path(self.workspace.name) / "refused").exists())

    @staticmethod
    def lit_origin():
        from pyspark.sql import functions as F

        return F.lit("source").alias("origin")


@unittest.skipUnless(HAS_PYSPARK and RUN_ICEBERG, "requires the pinned Spark and Iceberg runtime")
class ParcelServedGoldIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workspace = tempfile.TemporaryDirectory(prefix="parcel-served-iceberg-")
        cls.spark = session("parcel-served-gold-iceberg", cls.workspace.name)
        silver = common.ensure_table(
            cls.spark, "`proof`.`silver`.`parcel_boundaries`", "proof", "silver", job.SILVER_CONTRACT
        )
        columns = [c["name"] for c in job.SILVER_CONTRACT["columns"]]
        values = []
        for snapshot, numbers in (("synthetic-1", range(1, 6)), ("synthetic-2", range(1, 8))):
            for n in numbers:
                p = pnu(n)
                row = {
                    "boundary_id": f"{snapshot}-{p}", "pnu": p, "sido_code": "99", "sigungu_code": "99999",
                    "bjdong_code": "10100", "jibun": f"{n}", "bonbun": f"{n}", "bubun": "0",
                    "geometry_wkb": f"X'0103{n:02x}'", "geometry_srid": SRID,
                    "bbox_min_x": 0.0, "bbox_min_y": 0.0, "bbox_max_x": 1.0, "bbox_max_y": 1.0,
                    "geometry_checksum_sha256": f"{n:064x}", "source_record_id": "synthetic.zip",
                    "source_snapshot_id": snapshot, "valid_from_utc": "TIMESTAMP '2099-01-01 00:00:00'",
                    "valid_to_utc": "CAST(NULL AS TIMESTAMP)",
                    "ingested_at_utc": "TIMESTAMP '2099-01-01 00:00:00'",
                }
                values.append("(" + ", ".join(
                    v if isinstance(v, str) and (v.startswith(("X'", "TIMESTAMP", "CAST"))) else repr(v)
                    for v in (row[c] for c in columns)
                ) + ")")
        cls.spark.sql(f"INSERT INTO {silver} VALUES {', '.join(values)}")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.workspace.cleanup()

    def run_main(self, out, *extra):
        argv = [
            "--source-snapshot-id", "synthetic-2", "--output-dir", str(out / "parts"),
            "--summary-output", str(out / "summary.json"), "--iceberg-catalog-name", "proof",
            "--handoff-parts", "3", "--allow-non-smoke-write", *extra,
        ]
        with patch.object(job, "build_spark_session", return_value=self.spark), \
                patch.object(job, "assert_catalog_env"), patch.object(self.spark, "stop"):
            self.assertEqual(job.main(argv), 0)
        return json.loads((out / "summary.json").read_text(encoding="utf-8"))

    def test_the_current_snapshot_is_the_main_ref_not_the_newest_commit(self):
        from lakehouse_engine import current_snapshot

        table = "`proof`.`gold`.`rollback_probe`"
        self.spark.sql(f"CREATE TABLE {table} (n int) USING iceberg")
        with self.assertRaisesRegex(ValueError, "no main ref"):
            current_snapshot(self.spark, table)
        self.spark.sql(f"INSERT INTO {table} VALUES (1)")
        s1 = current_snapshot(self.spark, table)
        self.spark.sql(f"INSERT INTO {table} VALUES (2)")
        s2 = current_snapshot(self.spark, table)
        self.assertNotEqual(s1, s2)
        self.spark.sql(f"CALL proof.system.rollback_to_snapshot('gold.rollback_probe', {s1})")
        newest = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]["snapshot_id"]
        self.assertEqual(str(newest), s2, "the newest commit is still S2 after the rollback")
        self.assertEqual(current_snapshot(self.spark, table), s1, "the table's state is S1")

    def test_one_snapshot_is_served_and_handed_to_the_bake_in_parts(self):
        out = Path(self.workspace.name) / "run-1"
        summary = self.run_main(out)
        self.assertEqual(summary["schema_version"], "foundation-platform.polygon_served_gold.v2")
        self.assertEqual(summary["source_snapshot_id"], "synthetic-2")
        self.assertEqual((summary["unit"], summary["feature_id_property"]), ("parcels", "pnu"))
        self.assertEqual(summary["served_row_count"], 7, "only the named snapshot, not both")
        self.assertEqual(sum(part["rows"] for part in summary["handoff_parts"]), 7)
        common.verify_handoff_parts(out / "parts", summary["handoff_parts"], 7)
        gold = self.spark.sql("SELECT pnu, source_snapshot_id FROM `proof`.`gold`.`parcel_boundary_served`")
        self.assertEqual(sorted(r["pnu"] for r in gold.collect()), sorted(pnu(n) for n in range(1, 8)))
        snapshots = self.spark.sql(
            "SELECT snapshot_id FROM `proof`.`gold`.`parcel_boundary_served`.snapshots"
        ).collect()
        self.assertIn(summary["canonical_iceberg_snapshot_id"], {str(r["snapshot_id"]) for r in snapshots})
        with self.assertRaises(FileExistsError):
            self.run_main(out)


if __name__ == "__main__":
    unittest.main()
