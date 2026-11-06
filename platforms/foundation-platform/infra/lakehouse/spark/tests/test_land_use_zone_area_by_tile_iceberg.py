"""규제 구역 타일 색인의 유지와 바뀐 타일 읽기(루트 ADR-0375 §2)를 실제 Iceberg 위에서.

첫 유지는 현재 구역 전부를 읽는다(선언된 전량 예외). 다음 판은 그 판이 Silver 에 덧붙인 행만 읽고, 바뀐
구역의 줄과 삭제 표시만 덧붙인다. 같은 판을 다시 유지하면 아무것도 읽지 않는다. 서빙 중인 색인 스냅숏
이후 덧붙은 줄이 바뀐 구역과 더러운 타일을 낸다. Zones are synthetic (layers D998, D999).
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

SILVER = "`proof`.`silver`.`land_use_zone_area`"
BY_TILE = "`proof`.`gold`.`land_use_zone_area_by_tile`"
# Tile (540, 483) at z10 and its right neighbour (541, 483), near 10°E 10°N (synthetic).
LEFT = (10.05, 10.02, 10.06, 10.06)
RIGHT = (10.40, 10.02, 10.41, 10.06)


def at(day):
    return datetime(2026, 11, day, tzinfo=timezone.utc)


def zone(area, name, box, checksum, vintage, day, removed=False):
    return {"layer_code": "D998", "zone_feature_id": area, "layer_name": "SYN", "sigungu_code": None,
            "zone_code": "UQ999", "zone_name": name, "field_a7": None, "field_a8": None, "field_a9": None,
            "geometry_wkb": bytearray(b"\x01\x03"), "geometry_srid": 4326,
            "bbox_min_x": box[0], "bbox_min_y": box[1], "bbox_max_x": box[2], "bbox_max_y": box[3],
            "geometry_checksum_sha256": checksum, "source_record_id": f"obj-{vintage}",
            "source_snapshot_id": vintage, "ingested_at_utc": at(day), "row_state": "removed" if removed else None}


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ByTileIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        import land_use_zone_area_by_tile as by_tile

        cls.by_tile = by_tile
        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("land-use-zone-area-by-tile-iceberg")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.executorEnv.PYTHONPATH", str(Path(by_tile.__file__).parent))
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def append(self, rows, recorded):
        from changelog import change_record_options
        from platform_contracts import column_names, create_table_columns_sql

        contract = self.by_tile.SILVER_CONTRACT
        columns = column_names(contract)
        schema = " ".join(create_table_columns_sql(contract, indent=0).split())
        frame = self.spark.createDataFrame([tuple(row[c] for c in columns) for row in rows], schema)
        writer = frame.writeTo(SILVER.replace("`", ""))
        if recorded:
            removed = sum(row["row_state"] == "removed" for row in rows)
            for key, value in change_record_options(len(rows) - removed, removed).items():
                writer = writer.option(key, value)
        writer.append()
        return self.by_tile.current_snapshot_id(self.spark, SILVER)

    def maintain(self, snapshot, day):
        return self.by_tile.maintain(self.spark, BY_TILE, SILVER, "proof", snapshot, at(day))

    def test_a_release_is_maintained_from_what_it_appended_and_names_its_tiles(self):
        from platform_contracts import create_table_columns_sql

        by_tile = self.by_tile
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")
        self.spark.sql(f"CREATE TABLE {SILVER} ({create_table_columns_sql(by_tile.SILVER_CONTRACT)}) USING iceberg")
        first = self.append([zone("a", "stays", LEFT, "c-a", "20260609", 1),
                             zone("b", "moves", LEFT, "c-b", "20260609", 1),
                             zone("c", "goes", RIGHT, "c-c", "20260609", 1)], recorded=False)
        built = self.maintain(first, 1)
        self.assertEqual(built["scope"], "full")
        self.assertIn("did not exist", built["full_reason"])
        self.assertEqual((built["silver_rows_read"], built["rows_appended"]), (3, 3))

        second = self.append([zone("b", "moves", RIGHT, "c-b2", "20261009", 9),
                              zone("c", None, RIGHT, "c-c", "20261009", 9, removed=True)], recorded=True)
        kept = self.maintain(second, 9)
        self.assertEqual(kept["scope"], "changed-keys")
        self.assertEqual(kept["maintained_from"], first)
        # Only the two rows the release appended are read; b moves (a row and a marker), c goes.
        self.assertEqual((kept["silver_rows_read"], kept["rows_appended"]), (2, 3))
        self.assertEqual(self.maintain(second, 10)["scope"], "unchanged")

        appended = by_tile.appended_between(self.spark, BY_TILE, built["by_tile_snapshot"], kept["by_tile_snapshot"])
        rows = appended.select("tile_key", "zone_id").distinct().collect()
        left, right = by_tile.membership_tiles(*LEFT)[0], by_tile.membership_tiles(*RIGHT)[0]
        self.assertEqual({r.zone_id for r in rows}, {"D998:b", "D998:c"})
        self.assertEqual({r.tile_key for r in rows}, {left, right})
        zones = by_tile.neighbourhood(self.spark.table(BY_TILE.replace("`", "")), [left, right])
        self.assertEqual(sorted(r.zone_id for r in zones.collect()), ["D998:a", "D998:b"])
        self.assertEqual(by_tile.partition_records(self.spark, BY_TILE, [right]), 3)
        # Nothing appended since the served index snapshot: an empty change set, not a full one.
        none = by_tile.appended_between(self.spark, BY_TILE, kept["by_tile_snapshot"], kept["by_tile_snapshot"])
        self.assertEqual(none.count(), 0)

        # The partial handoff: the neighbourhood rows, its tiles and the change list.
        import json

        import land_use_zone_area_tile_handoff as job

        frame, block, changes = job.partial_handoff(self.spark, BY_TILE, appended)
        self.assertEqual(changes, [{"feature_id": "D998:b", "op": "upsert"}, {"feature_id": "D998:c", "op": "delete"}])
        self.assertEqual(block["neighbourhood_tiles"], [[540, 483], [541, 483]])
        self.assertEqual(block["by_tile_neighbourhood_rows"], 6)
        with tempfile.TemporaryDirectory() as out:
            rows, _, per_layer = job.write_frame(frame, Path(out, "served.jsonl"), 2, allow_empty=True)
            handed = [json.loads(line) for line in Path(out, "served.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual((rows, per_layer), (2, {"D998": 2}))
        self.assertEqual(sorted(row["feature_id"] for row in handed), ["D998:a", "D998:b"])
        self.assertEqual(handed[0]["properties"]["layer_code"], "D998")
        self.assertEqual(handed[0]["geometry_wkb_hex"], "0103")
        # A change set that leaves no zone in its tiles hands an empty file, not a refusal.
        empty, _, _ = job.partial_handoff(self.spark, BY_TILE, none)
        with tempfile.TemporaryDirectory() as out:
            self.assertEqual(job.write_frame(empty, Path(out, "served.jsonl"), 2, allow_empty=True)[0], 0)
            self.assertEqual(Path(out, "served.jsonl").read_bytes(), b"")


if __name__ == "__main__":
    unittest.main()
