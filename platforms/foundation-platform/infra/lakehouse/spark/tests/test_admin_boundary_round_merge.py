"""`scripts/tiles/admin-boundary/merge.py` takes the official code list from the snapshot table.

The list used to be a hand-downloaded 법정동코드 전체자료 file; now it is the newest
`reference.legal_dong_code_snapshot`, read through the same reader as the administrative matching gate.
"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "infra" / "lakehouse" / "spark" / "jobs"))
_spec = importlib.util.spec_from_file_location("admin_boundary_merge", ROOT / "scripts" / "tiles" / "admin-boundary" / "merge.py")
merge = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(merge)

from legal_dong_code_snapshot_to_reference import newest_snapshot_codes  # noqa: E402

# The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py (half-open).
SQUARE = [[[127.1231, 36.1231], [127.1234, 36.1231], [127.1234, 36.1234], [127.1231, 36.1234], [127.1231, 36.1231]]]
ROUND = "209901"
CODES = {
    "9900000000": ("합성도", "존재"),
    "9999900000": ("합성도 합성구", "존재"),
    "9999910100": ("합성도 합성구 합성동", "존재"),
    "9999910200": ("합성도 합성구 옛합성동", "폐지"),
}


def dong(emd_cd: str, name: str) -> dict:
    return {
        "type": "Feature",
        "properties": {"EMD_CD": emd_cd, "EMD_NM": name, "COL_ADM_SE": "99999"},
        "geometry": {"type": "Polygon", "coordinates": SQUARE},
    }


def write_round(out_dir: Path, sido: str = "99") -> None:
    layer = {"type": "FeatureCollection", "name": f"LSMD_ADM_SECT_UMD_{sido}_{ROUND}",
             "features": [dong("99999101", "합성동"), dong("99999102", "옛합성동")]}
    (out_dir / f"30603-{sido}.geojson").write_text(json.dumps(layer, ensure_ascii=False), encoding="utf-8")


class FakeSpark:
    """Answers the two queries of `newest_snapshot_codes` from rows of two snapshot dates."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def sql(self, query: str) -> "FakeSpark":
        self.queries.append(query)
        return self

    def collect(self) -> list[dict]:
        query = self.queries[-1]
        if "max(snapshot_date)" in query:
            return [{"d": max((r["snapshot_date"] for r in self.rows), default=None)}]
        return [r for r in self.rows if f"DATE '{r['snapshot_date']}'" in query]


class MergeReadsTheSnapshotTable(unittest.TestCase):
    def test_names_and_existence_come_from_the_official_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_round(Path(tmp))
            data, report = merge.merge_round(ROUND, Path(tmp), CODES)
        features = json.loads(data)["features"]
        self.assertEqual(
            [f["properties"] for f in features],
            [{"EMD_CD": "99999101", "EMD_NM": "합성동", "SIGUNGU_CD": "99999", "SIGUNGU_NM": "합성구"}],
        )
        self.assertEqual(report["excluded"]["code not existing in the official list"]["sample"], ["99999102"])

    def test_a_sido_the_code_list_has_without_a_file_refuses_the_round(self):
        codes = dict(CODES, **{"9800000000": ("다른합성도", "존재")})
        with tempfile.TemporaryDirectory() as tmp:
            write_round(Path(tmp))
            with self.assertRaisesRegex(SystemExit, r"no file for sido \['98'\]"):
                merge.merge_round(ROUND, Path(tmp), codes)

    def test_an_empty_code_list_refuses_the_round(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_round(Path(tmp))
            with self.assertRaisesRegex(SystemExit, "code list snapshot is empty"):
                merge.merge_round(ROUND, Path(tmp), {})

    def test_there_is_no_code_file_input(self):
        with self.assertRaises(SystemExit):
            merge.parse_args(["--round", ROUND, "--code-list", "codes.zip"])
        self.assertEqual(merge.parse_args(["--round", ROUND]).code_list_table, "reference.legal_dong_code_snapshot")

    def test_main_merges_with_the_newest_snapshot(self):
        older = [{"snapshot_date": "2099-01-01", "region_cd": code, "full_name": name, "status": "폐지"}
                 for code, (name, _) in CODES.items()]
        newest = [{"snapshot_date": "2099-01-02", "region_cd": code, "full_name": name, "status": status}
                  for code, (name, status) in CODES.items()]
        spark = FakeSpark(older + newest)
        self.assertEqual(newest_snapshot_codes(spark, "t"), ("2099-01-02", CODES))
        self.assertEqual(newest_snapshot_codes(FakeSpark([]), "t"), (None, {}))

        original = merge.read_official_codes
        merge.read_official_codes = lambda args: newest_snapshot_codes(spark, args.code_list_table)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp) / "out"
                out_dir.mkdir()
                write_round(out_dir)
                self.assertEqual(merge.main(["--round", ROUND, "--out-dir", str(out_dir)]), 0)
                report = json.loads((Path(tmp) / "merge-report.json").read_text(encoding="utf-8"))
                self.assertTrue((Path(tmp) / "official-administrative-boundary.geojson").exists())
        finally:
            merge.read_official_codes = original
        self.assertEqual(report["features"], 1)
        self.assertEqual(report["code_list"]["snapshot_date"], "2099-01-02")


if __name__ == "__main__":
    unittest.main()
