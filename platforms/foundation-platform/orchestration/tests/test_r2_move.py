"""The R2 area moves of root ADR-0626 §6: each plan's old -> new key map, what it keeps, and the index rewrite."""

from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("r2_move", ROOT / "scripts" / "ops" / "r2_move.py")
r2_move = importlib.util.module_from_spec(spec)
sys.modules["r2_move"] = r2_move  # its dataclasses resolve their module by name
spec.loader.exec_module(r2_move)

H = "0" * 63 + "a"


def item(key: str, size: int = 10) -> dict:
    return {"Key": key, "Size": size, "ETag": '"abc"', "LastModified": dt.datetime(2026, 11, 28, tzinfo=dt.timezone.utc)}


class KeyMaps(unittest.TestCase):
    def test_media_goes_to_one_store_by_its_form(self):
        self.assertEqual(r2_move.media_key(f"onbid/media/sha256/{H}.jpg"), f"media/display/sha256/{H}.jpg")
        self.assertEqual(r2_move.media_key(f"onbid/media/sources/sha256/{H}.bmp"), f"media/received/sha256/{H}.bmp")
        self.assertEqual(r2_move.media_key(f"onbid/media/renditions/r1/{H}/400.avif"),
                         f"media/rendition/ladder=1/{H}/400.avif")
        self.assertEqual(r2_move.media_key(f"onbid/media/renditions/r1/{H}.json"), f"media/rendition/ladder=1/{H}.json")
        for unknown in ("onbid/media/other/x", f"onbid/media/sha256/{H}", "onbid/media/renditions/v1/x"):
            with self.assertRaises(r2_move.MoveError):
                r2_move.media_key(unknown)

    def test_court_pdfs_become_public_documents(self):
        self.assertEqual(r2_move.document_key(f"courtauction-go-kr/sale-specifications/sha256/{H}.pdf"),
                         f"media/document/sha256/{H}.pdf")
        with self.assertRaises(r2_move.MoveError):
            r2_move.document_key(f"courtauction-go-kr/sale-specifications/{H}.pdf")

    def test_complex_profiles_and_index_go_to_serving(self):
        uid = "00000000-0000-0000-0000-000000000000"
        self.assertEqual(r2_move.complex_key(f"gold/industrial-complex/profiles/{uid}.json"),
                         f"serving/industrial_complex/profile/{uid}.json")
        self.assertEqual(r2_move.complex_key("gold/industrial-complex/index-history/12.json"),
                         "serving/industrial_complex/index/12.json")
        self.assertEqual(r2_move.complex_key("gold/industrial-complex/index.json"), r2_move.COMPLEX_INDEX)

    def test_by_pnu_deletes_only_the_document_generations(self):
        plan = r2_move.PLANS["by_pnu_documents"]
        gone = ["serving/parcels/by-pnu/v1/9999900000100000000.json", "serving/parcels/zoning/by-pnu/v2/p3/x.json"]
        kept = ["serving/parcels/by-pnu/manifest.json", "serving/parcels/by-pnu/manifest-history/a.json",
                "serving/parcels/packs/documents/g2/9999900000.pack", "serving/buildings/by-pnu/v-not/x"]
        for key in gone:
            self.assertEqual(r2_move.manifest_line(plan, item(key))["action"], "delete", key)
        for key in kept:
            self.assertIsNone(r2_move.manifest_line(plan, item(key)), key)
        self.assertEqual(plan.expire_prefix(gone[1]), "serving/parcels/zoning/by-pnu/v2/")

    def test_no_plan_touches_bronze_the_catalog_or_control(self):
        for plan in r2_move.PLANS.values():
            r2_move.check_plan(plan)
            for root in r2_move.FORBIDDEN_ROOTS:
                self.assertFalse(any(prefix.startswith(root) for prefix in plan.prefixes), plan.name)
        with self.assertRaises(r2_move.MoveError):
            r2_move.check_plan(r2_move.Plan("bad", ("bronze/",), None))

    def test_a_large_object_is_refused_not_half_copied(self):
        with self.assertRaises(r2_move.MoveError):
            r2_move.manifest_line(r2_move.PLANS["media"], item(f"onbid/media/sha256/{H}.jpg", 6 * 1024**3))

    def test_the_index_rewrite_is_the_next_generation_with_new_keys(self):
        uid = "00000000-0000-0000-0000-000000000001"
        old = {"generation": 7, "source": {"table": "gold.complex_catalog"},
               "complexes": [{"complex_id": uid, "content_sha256": H,
                              "profile_object_key": f"gold/industrial-complex/profiles/{uid}.json"}]}
        new = r2_move.rewrite_complex_index(old)
        self.assertEqual(new["generation"], 8)
        self.assertEqual(new["complexes"][0]["profile_object_key"], f"serving/industrial_complex/profile/{uid}.json")
        self.assertEqual(new["complexes"][0]["content_sha256"], H)
        self.assertEqual(old["generation"], 7)

    def test_range_shards_cover_every_key_once(self):
        edges = [None, *("p/" + b for b in r2_move.BOUNDARIES), None]
        keys = ["p/0", "p/00", "p/00x", "p/1A", "p/9z~", "p/A", "p/_", "p/zz", "p/zzz", "p/~"]
        owners = []
        for key in keys:
            owners.append(sum(1 for lo, hi in zip(edges, edges[1:], strict=False)
                              if (lo is None or key > lo) and (hi is None or key <= hi)))
        self.assertEqual(owners, [1] * len(keys))


if __name__ == "__main__":
    unittest.main()
