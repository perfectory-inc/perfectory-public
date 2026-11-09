"""The Korea edge measurement (scripts/ops/korea-edge-measure.py, root ADR-0460) reads its doors from
the front door contract and names where each answer entered."""

import importlib.util
import json
import pathlib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("korea_edge_measure", PLATFORM / "scripts/ops/korea-edge-measure.py")
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


class KoreaEdgeMeasureTest(unittest.TestCase):
    def test_a_door_on_cloudflare_is_read_on_its_public_name_and_a_switched_one_on_its_origin(self):
        contract = {"doors": {
            "tiles": {"gateway": "tile", "origin_hostname": "tiles-origin.example.test", "answered_by": "cloudflare"},
            "catalog": {"gateway": "parcel", "origin_hostname": "catalog-origin.example.test",
                        "answered_by": "cloudfront"},
            "web_preview": {"public_hostname": "preview.example.test", "answered_by": "cloudfront"},
        }}
        r2 = {"tile": {"public_hostname": "tiles.example.test"}, "parcel": {"public_hostname": "catalog.example.test"}}
        self.assertEqual(measure.doors(contract, r2), {
            "tiles": {"public": "tiles.example.test", "cloudflare": "tiles.example.test", "answered_by": "cloudflare"},
            "catalog": {"public": "catalog.example.test", "cloudflare": "catalog-origin.example.test",
                        "answered_by": "cloudfront"},
        })

    def test_the_entry_point_is_cloudfronts_pop_or_cloudflares_colo(self):
        self.assertEqual(measure.entry_point({"x-amz-cf-pop": "ICN80-P1", "cf-ray": "8f-LAX"}), "ICN80-P1")
        self.assertEqual(measure.entry_point({"cf-ray": "8f00aa11bb22cc33-LAX"}), "LAX")
        self.assertEqual(measure.entry_point({}), "?")
        self.assertEqual(measure.edge_hit({"x-cache": "Hit from cloudfront"}), "Hit")
        self.assertEqual(measure.edge_hit({"cf-cache-status": "HIT"}), "HIT")

    def test_the_headers_are_the_last_responses(self):
        text = "HTTP/1.1 100 Continue\r\n\r\nHTTP/2 200\r\nCF-Ray: 1-ICN\r\nContent-Encoding: br\r\n"
        self.assertEqual(measure.parse_headers(text), {"cf-ray": "1-ICN", "content-encoding": "br"})

    def test_the_shipped_contract_has_a_door_for_every_sample_kind(self):
        contract = json.loads(measure.CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual({name for name, door in contract["doors"].items() if "gateway" in door}, {"tiles", "catalog"})


if __name__ == "__main__":
    unittest.main()
