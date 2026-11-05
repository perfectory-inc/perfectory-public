"""규제 구역 타일 넘김(루트 ADR-0373): 인자, 부분 파일 잇기, 계약과의 일치."""

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))
CONFIG = Path(__file__).resolve().parents[4] / "config"
# The map's own list of pending layer codes (products/gongzzang, root ADR-0373 §4).
WEB_PENDING = Path(__file__).resolve().parents[6] / "products/gongzzang/apps/web/lib/map/zoning-families.ts"

import land_use_zone_area_tile_handoff as job  # noqa: E402


class LandUseZoneAreaTileHandoffTest(unittest.TestCase):
    def test_the_tile_layer_is_the_one_the_bake_contract_registers(self):
        contract = json.loads((CONFIG / "tile-bake-containers.contract.json").read_text(encoding="utf-8"))
        newborn = contract["units_born_from_a_lakehouse_bake"][job.UNIT]
        self.assertEqual(newborn["feature_id_property"], job.FEATURE_ID_PROPERTY)
        self.assertEqual(newborn["feature_filter_properties"], sorted(job.TILE_PROPERTIES))
        self.assertEqual(newborn["source_record_source"], f"lakehouse:{job.SILVER_TABLE}")
        # The partition is the layer: `D032` of `D032:<area number>`.
        self.assertEqual(contract["partition_by_feature_id_prefix"][job.UNIT], len("D032"))

    def test_the_properties_and_key_are_silver_columns(self):
        columns = {column["name"] for column in job.SILVER_CONTRACT["columns"]}
        for name in (*job.KEY, *job.CONTENT, "geometry_wkb", "geometry_srid"):
            self.assertIn(name, columns)
        self.assertEqual(job.GEOMETRY_SRID, 4326)

    def test_the_raon_only_layers_are_reported_pending(self):
        contract = json.loads(job.LANE_CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual([item["layer_code"] for item in job.pending_layers(contract)], ["D054", "D125"])

    def test_the_map_names_the_same_pending_layers(self):
        if not WEB_PENDING.exists():
            self.skipTest("the map's zoning families arrive with the web change (PR #995)")
        text = WEB_PENDING.read_text(encoding="utf-8")
        listed = re.search(r"ZONING_PENDING_LAYER_CODES\s*=\s*\[([^\]]*)\]", text)
        self.assertIsNotNone(listed, "zoning-families.ts no longer exports ZONING_PENDING_LAYER_CODES")
        codes = sorted(re.findall(r'"(D[0-9]{3})"', listed.group(1)))
        contract = json.loads(job.LANE_CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(codes, [item["layer_code"] for item in job.pending_layers(contract)])

    def test_a_layer_cannot_be_loaded_and_pending(self):
        contract = {"silver_refresh": {"completeness": {"regions": ["999"]}},
                    "pending_regions": [{"region": "999", "name": "SYN", "reason": "synthetic"}]}
        with self.assertRaisesRegex(ValueError, "both loaded and pending"):
            job.pending_layers(contract)

    def test_a_metadata_location_becomes_its_bucket_relative_key(self):
        self.assertEqual(job.bucket_relative("s3://bucket/warehouse/silver/t/metadata/00001-x.metadata.json"),
                         "warehouse/silver/t/metadata/00001-x.metadata.json")
        for bad in ("warehouse/x.json", "s3://bucket/", "s3://bucket/a/../b.json"):
            with self.assertRaises(ValueError):
                job.bucket_relative(bad)

    def test_the_key_is_the_contracts_and_names_the_sigungu(self):
        # 2026-11-03: one area split at a 시군구 boundary ships as two polygons sharing A1 (308
        # pairs in D036); a key without the 시군구 gave each of them two current rows.
        self.assertEqual(job.KEY, ("layer_code", "zone_feature_id", "sigungu_code"))
        self.assertEqual(list(job.KEY), job.SILVER_CONTRACT["current_row_latest"]["key"])
        lane = json.loads(job.LANE_CONTRACT.read_text(encoding="utf-8"))
        self.assertFalse(set(job.KEY) & set(lane["silver_refresh"]["compare"]["content_columns"]),
                         "a key column is not compared as content")

    def test_a_refusal_writes_its_report_beside_the_summary_and_exits_65(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp, "served-summary.json")
            keys = [{"layer_code": "D999", "zone_feature_id": "SYN-0001", "sigungu_code": "99999", "count": 2}]
            refusal = job.Refused("divergent_current_rows", job.refusal_report("123", 1, keys))
            self.assertEqual(job.report_refusal(summary, refusal), job.REFUSED_EXIT)
            self.assertFalse(summary.exists(), "the bake gets no summary to read")
            report = json.loads(Path(tmp, "served-summary.json.refused.json").read_text(encoding="utf-8"))
            self.assertEqual((report["reason"], report["keys"], report["sample"]), ("divergent_current_rows", 1, keys))
            self.assertEqual(report["key"], list(job.KEY))

    def test_a_served_snapshot_must_be_a_snapshot_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = job.parse_args(["--output", f"{tmp}/h.jsonl", "--summary-output", f"{tmp}/s.json",
                                   "--served-snapshot", "D032"])
            with self.assertRaisesRegex(ValueError, "snapshot id"):
                job.validate_args(args)

    def test_an_existing_handoff_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "h.jsonl").write_text("{}\n", encoding="utf-8")
            args = job.parse_args(["--output", f"{tmp}/h.jsonl", "--summary-output", f"{tmp}/s.json"])
            with self.assertRaises(FileExistsError):
                job.validate_args(args)

    def test_parts_are_joined_in_name_order_into_one_create_only_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            parts_dir = Path(tmp, "parts")
            parts_dir.mkdir()
            Path(parts_dir, "part-00001.jsonl").write_bytes(b'{"feature_id":"D999:b"}\n')
            Path(parts_dir, "part-00000.jsonl").write_bytes(b'{"feature_id":"D999:a"}\n{"feature_id":"D999:a2"}\n')
            parts = [{"path": "part-00001.jsonl", "rows": 1}, {"path": "part-00000.jsonl", "rows": 2}]
            output = Path(tmp, "handoff.jsonl")
            rows, sha256 = job.concatenate_parts(parts_dir, parts, output)
            self.assertEqual(rows, 3)
            self.assertEqual(
                output.read_bytes().splitlines()[0], b'{"feature_id":"D999:a"}'
            )
            self.assertEqual(len(sha256), 64)
            with self.assertRaises(FileExistsError):
                job.concatenate_parts(parts_dir, parts, output)


if __name__ == "__main__":
    unittest.main()
